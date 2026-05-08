# Adaptive Filter Scheduling in the Parquet Decoder

**PR:** [adriangb/datafusion#11](https://github.com/adriangb/datafusion/pull/11) (`adaptive-filters-in-decoder`)
**Branched off:** `apache/datafusion@6a09260d5`
**Companion:** [`pydantic/arrow-rs#9` — `adaptive-strategy-swap`](https://github.com/pydantic/arrow-rs/pull/9)

> One sentence summary: a runtime selectivity tracker decides per-filter
> whether each predicate runs as a Parquet row-level filter or a post-scan
> filter, and re-evaluates that placement at every row-group boundary by
> swapping the decoder's strategy in place — so the cost of
> `pushdown_filters=on` is paid only when it pays off.

## TL;DR — does the work pay off?

| Workload | main_off | main_on | PR_on | PR_on / main_off | PR_on / main_on |
|----------|---------:|--------:|------:|-----------------:|----------------:|
| **ClickBench partitioned, SSD (43 q)** | 21.02 s | 21.70 s | **17.19 s** | **0.82×** ✓ | **0.79×** ✓ |
| **TPC-DS SF1, SSD (99 q)**             | 17.00 s | **38.96 s** ⚠ | 18.06 s | 1.06× | **0.46×** ✓ |
| **TPC-H SF1, SSD (22 q)**              | **0.78 s** | 0.99 s | 1.13 s | 1.45× ✗ | 1.14× |
| TPC-H SF1, S3 (22 q)                   | 23.72 s | **52.60 s** ⚠ | 24.68 s | **1.04×** ≈ | **0.47×** ✓ |
| TPC-DS SF1, S3 (99 q)                  | 76.42 s | **141.94 s** ⚠ | 88.03 s | 1.15× | **0.62×** ✓ |
| ClickBench partitioned, S3 (43 q)      | 86.62 s | **111.31 s** ⚠ | 89.04 s | **1.03×** ≈ | **0.80×** ✓ |

> "SSD" = local NVMe. "S3" = the bench harness with `--simulate-latency`,
> which adds 20–200 ms random latency per object-store op
> ([PR #20954](https://github.com/apache/datafusion/pull/20954)). I
> didn't run against AWS — but the latency profile is what you'd see
> against any cloud object store.

What this says, framed against the original hypotheses:

1. **ClickBench:** PR beats main on *both* settings → confirmed.
2. **TPC-DS:** the user's "2× faster than main pushdown=off" claim
   actually maps to "**2× faster than main pushdown=on**". The CI
   bench at
   [PR #11 comment](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340741427)
   was triggered with explicit env flags
   `DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true` *and*
   `DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true` on **both** the
   baseline and the changed branch
   ([trigger comment](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340678050)).
   So the CI's "1.80× speedup" is exactly `PR_on / main_on`, and my
   local `0.46×` (i.e. 2.16× faster) matches it. Against
   `pushdown=off` the PR is essentially flat (+6 %). The Q64 outlier
   alone — `main_on=20 s` → `PR_on=0.65 s` — accounts for almost the
   entire delta.
3. **TPC-H:** the unaddressed case — PR is +44 % vs `main pushdown=off`,
   only +14 % vs `main pushdown=on`. Single-row-group files mean
   neither the in-decoder swap (no swap point) nor file-level
   morselization (the file is the morsel) can re-balance work. The
   actual fix is row-group-granularity morselization (see [PR
   #21766](https://github.com/apache/datafusion/pull/21766)).

So the headline "this PR neutralises the cost of `pushdown_filters=on`"
is true on the workloads where adaptive scheduling is the right tool
(ClickBench, most of TPC-DS); on TPC-H — where the bottleneck is
partition skew, not filter placement — it can't help by design.

---

## 1. The problem

`pushdown_filters=on` is a per-query lottery on `main`:

- **When it wins:** the predicate is selective enough that the
  row-level evaluator quickly produces a sparse `RowSelection`, the
  page index lets the reader skip most projection-column pages
  outright, and what remains is decoded only for surviving rows
  (late materialization). Heavy projections + selective filters get
  near-free.
- **When it loses:** the filter is mandatory but unselective, the filter
  column is also in the projection, or both. Then `pushdown_filters=on`
  buys nothing (no rows skipped, no pages skipped) but pays:
  - **Computational cost** — per-batch per-predicate predicate eval, smaller masks,
    more iterations than one big `FilterExec` post-scan apply.
  - **I/O cost** — the row-level path requests each filter's
    columns first, then a separate request for the surviving rows of the
    other filters and finally the projection. Multi-stage = more, smaller object-store reads vs. one
    coalesced read of all projected columns.

So when filter pushdown was added to DataFusion as an option, it turned
into something users had to twiddle per query.

```
                 main, pushdown=off                main, pushdown=on
                ─────────────────                ─────────────────
   ParquetSource ──► FilterExec ──► …       ParquetSource(filter) ──► …
   (stat-prunes RG)                          (stat-prunes RG +
   reads matching RGs                         row-level prune w/in RG)
   in one pass per col                       reads filter cols first,
   FilterExec applies                        then surviving cols
   one big mask                              ── more / smaller IO
                                             ── per-batch mask apply
                                             ── possible double-decode
                                                of overlapping cols
```

**Goal of this PR:** never make a query slower than `main` with the user's
preferred default (`pushdown=off`), while still capturing the wins on
queries that benefit from row-level pushdown.

---

## 2. Background: how a Parquet scan in DataFusion is shaped

### 2.1 Partitions & files

A `DataSourceExec(ParquetSource)` is replicated across `target_partitions`
streams. The planner assigns each *file* to one partition (file-level
distribution; no within-file splitting in the public path today). For
each file the partition gets, the source builds an opener that lazily
streams record batches.

```
ParquetSource (target_partitions = N)
 ├─ partition 0:  files [f0, f4, f8, …]
 ├─ partition 1:  files [f1, f5, f9, …]
 ├─ …
 └─ partition N-1: files [fN-1, …]

per file:                              per opener:
 ┌──────────┐  byte ranges    ┌──────────────┐   RecordBatch
 │ ObjStore │ ◄────────────── │ Push decoder │ ─────────────► downstream
 └──────────┘  ─────────────► │ (one per     │
              fetched bytes   │  file)       │
                              └──────────────┘
```

A *row group* is the unit at which Parquet stores statistics and at which
the decoder can pause and reconfigure. A typical ClickBench partitioned
file has ~10–30 row groups; a TPC-H lineitem SF1 file has **one**.

### 2.2 Row filter vs. post-scan filter

Row-level pushdown filtering is implemented in arrow-rs as a `RowFilter`
(`Vec<Box<dyn ArrowPredicate>>`). Each predicate's `evaluate(RecordBatch) ->
BooleanArray` runs on the actual decoded *data* of the filter columns; the
boolean mask is converted into a `RowSelection` that subsequent predicates
and the projection inherit. With page index enabled, pages whose surviving
rows are empty get skipped entirely for the projection columns. None of this
involves row-group statistics — those are a separate, plan-time mechanism.

```
            ┌────────────────────── one file, one decoder ─────────────────────┐
            │                                                                  │
 Row filter │   for each row group:                                            │
 (pushdown) │     1. fetch + decode filter-col pages                           │
            │     2. ArrowPredicate::evaluate(batch) → BooleanArray            │
            │     3. boolean → refine RowSelection                             │
            │     4. for projection cols: skip pages with no surviving rows,   │
            │        decode only surviving rows in the rest                    │
            │     5. emit batch                                                │
            │                                                                  │
            │   Pros: late materialization — projection columns barely         │
            │         decoded when filter is selective; with page index,       │
            │         whole pages skipped without I/O.                         │
            │   Cons: extra I/O for filter cols *not* in the projection;       │
            │         per-batch ArrowPredicate eval cost; double-decode of     │
            │         filter cols also in the projection (predicate cache      │
            │         helps but isn't always sufficient).                      │
            └──────────────────────────────────────────────────────────────────┘

            ┌────────────── post-scan (FilterExec or in-opener) ───────────────┐
            │                                                                  │
            │   for each row group:                                            │
            │     1. fetch + decode all projection cols (one coalesced read)   │
            │     2. evaluate predicate over the full batch                    │
            │     3. apply one mask, emit (possibly empty) batch               │
            │                                                                  │
            │   Pros: one coalesced I/O; one big mask apply; no double         │
            │         decode.                                                  │
            │   Cons: pays full decode cost on rows it will throw away.        │
            └──────────────────────────────────────────────────────────────────┘
```

> **Row-group statistics pruning is orthogonal.** It runs at plan/open
> time using per-row-group min/max/null from the file metadata; it's
> available regardless of `pushdown_filters`. So a `WHERE k >= 50 AND k
> <= 10000` predicate can prune entire row groups whether pushdown is on
> or off. What `pushdown_filters=on` *adds* is **row-level / page-level
> filtering inside surviving row groups** (driven by actual data, not
> stats) plus **late materialization of projection columns**. Some
> predicates (`URL LIKE '%google%'`) get nothing from RG stats but
> everything from row-level filtering; some (range predicates with
> bounded distributions) get the inverse; some get both.

### 2.3 Two different problems with `pushdown=on`

It's worth separating two distinct reasons `pushdown=on` can lose to
`pushdown=off`. **This PR addresses the first; the second is a
different story (morselized scans).**

**Problem A — ineffective-filter cost (this PR's scope).** When a
filter is mandatory but unselective, `pushdown=on` pays per-batch
predicate eval, smaller masks, two-stage I/O, and possible double
decode without saving any work. There's no row-group statistics win to
offset the cost. So a query that *would* be fine with `FilterExec`
post-scan (one big mask, one coalesced read) ends up slower with the
filter wedged into the decoder.

**Problem B — lopsided partitions / data skew (NOT this PR's scope).**
Even when each individual filter is correctly placed, the partition
layout can leave one stream doing 10× the work of its neighbours
because *files* are the unit of distribution. A `FilterExec` after a
`RepartitionExec` gets a free re-balance; an in-source filter doesn't.

```
    skew shape (independent of what the filter does):
    ─────────────────────────────────────────────────
    p0 ▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮ ← unfortunate file: low pruning, all rows survive
    p1 ▮                     ← lucky file: stats prune the whole RG
    p2 ▮▮▮▮                  ← partial prune
    …
       └────────── stragglers gate the whole stage ──────────┘

    pushdown=off, FilterExec after RepartitionExec(hash) re-balances:
    ─────────────────────────────────────────────────────────────────
    p0 ▮▮▮  p1 ▮▮▮  p2 ▮▮▮  …
```

Problem B's actual fix is **morselized scans**: today the morsel
granularity is per-file; the future direction is per-row-group (or
sub-chunk), so that one ill-fortuned file can't gate a stage. That
work is separate from this PR.

This PR (adaptive in-decoder filter scheduling) only changes how
*which* filter runs *where*, not how the per-partition work is
distributed. A workload where the dominant cost is partition skew —
TPC-H SF1 with one row group per file is the canonical example —
will not be fixed by this PR. It needs row-group-level morsels.

---

## 3. The solution: adaptive in-decoder filter scheduling

> **Scope:** this section describes the PR's fix for **Problem A**
> from §2.3 — the per-filter ineffective-cost lottery. It does
> **not** address Problem B (lopsided partitions / data skew); that
> requires row-group-granularity morsel scans, which is separate
> work.

### 3.1 What's new

For each `ParquetSource`, every conjunct of the source-level predicate
gets a stable `FilterId`. A `SelectivityTracker` (shared across all
files of one ParquetSource) tracks per-`FilterId` statistics and assigns
each filter to one of three states:

```
              ┌──────────┐
              │   New    │
              └─────┬────┘
                    │  initial placement based on
                    │  filter-cols-bytes / projection-bytes
       ┌────────────┴─────────────┐
       ▼                          ▼
  ┌─────────┐  promote  ┌────────────────┐
  │ PostScan │ ◄───────► │   RowFilter   │
  └────┬─────┘  demote  └───────┬────────┘
       │                        │  drop (only OptionalFilterPhysicalExpr,
       │                        │        e.g. hash-join dyn filters)
       └────► Dropped ◄─────────┘
```

Per filter the tracker keeps:
- rows seen / rows matched (selectivity),
- evaluation time (ns),
- bytes seen, and
- a Welford running mean + M2 of per-batch *bytes-saved-per-second*.

Promotion / demotion uses a one-sided **confidence interval** on
bytes-saved-per-sec rather than a point estimate, so noisy early
samples don't yo-yo a filter between states. Default `z = 2.0`
(~97.5%); the threshold is `filter_pushdown_min_bytes_per_sec = 100
MiB/s` by default. The "scatter-aware" version of the metric counts
only sub-batch *windows* where the filter killed every row — a 50%
uniform filter scores ~0 and stays post-scan; a TopK-style filter that
empties whole pages blows past the threshold.

```
             SelectivityStats   (Welford online stats)
             ─────────────────
              eff_mean   ← bytes-saved-per-sec mean over batches
              eff_m2     ← M2  → variance → stddev
              CI_lo      ← mean − z·σ/√n   (used to PROMOTE)
              CI_hi      ← mean + z·σ/√n   (used to DEMOTE)
```

### 3.2 Where the swap happens

The big mechanical piece: **the decoder is rebuilt at row-group
boundaries without re-opening the file**. Companion arrow-rs branch
adds:

```rust
ParquetPushDecoder::can_swap_strategy(&self) -> bool;          // true between RGs
ParquetPushDecoder::swap_strategy(&mut self, swap)             // replace
                                  -> Result<()>;               //   filter / projection
                                                               //   between RGs
```

`PushBuffers` survives the swap, so any column bytes already fetched
that are still in the new strategy's projection are reused.

```
                             AdaptiveParquetStream::transition()
                             ──────────────────────────────────

      ┌──────── start of file or finished previous RG ───────┐
      │                                                      │
      ▼                                                      │
  maybe_swap_strategy()                                       │
   │  ▸ tracker.partition_filters(all_conjuncts, …)           │
   │  ▸ if row-filter set unchanged → no-op                   │
   │  ▸ else build new RowFilter, decoder.swap_strategy()     │
   │                                                          │
   ▼                                                          │
  decoder.try_next_reader()                                   │
   │  ▸ NeedsData(ranges) → reader.get_byte_ranges() & push   │
   │  ▸ Data(reader)      → set as active                     │
   │  ▸ Finished          → stream ends                       │
   ▼                                                          │
  for batch in reader:                                        │
     apply_post_scan_filters_with_stats(batch, tracker, …)    │
       ↑                                                      │
       └─ feeds Welford updates for every PostScan filter ─┐  │
     project, emit                                         │  │
                                                           ▼  │
                                                    update ───┘
                                                    swap candidate
```

`OptionalFilterPhysicalExpr` is a transparent wrapper for predicates
that can be dropped without affecting correctness (today: hash-join
dynamic filters). When such a filter underperforms, the tracker can
short-circuit it via an `AtomicBool` — the filter columns still get
decoded (the decoder can't be reconfigured mid-RG) but the predicate is
not evaluated.

### 3.3 Initial placement heuristic

For a brand-new filter, the tracker checks the *extra* compressed bytes
the filter would pull in (filter-only columns not already in the
projection):

```
extra_bytes / projection_compressed_bytes  ≤  byte_ratio_threshold (=0.20)
              AND extra_bytes > 0                       → start as RowFilter
otherwise                                               → start as PostScan
```

Two cases default to PostScan:
- `extra_bytes == 0`: filter columns are entirely in the projection
  (e.g. `WHERE col <> '' GROUP BY col`). No I/O to save; defaulting to
  row-level loses on ClickBench Q10/11/13/14/26 because the predicate
  cache may evict on heavy strings, double-decoding the filter column.
- `byte_ratio > threshold`: too much extra I/O to bet on without
  evidence the filter is selective.

Promotion can still happen from PostScan once the metric crosses the
threshold; demotion is safe in both directions.

### 3.4 What it replaces (vs PR #9)

The earlier morsel-per-row-group split is gone:

- No `Vec<BoxStream>` per file, no per-chunk `AsyncFileReader::create_reader`.
- No per-chunk `RowFilter` rebuild (`RowFilter` is `!Clone`).
- No `EarlyStoppingStream`-on-chunk-0-only special case for `FilePruner`.
- No `LazyMorselShared` per-morsel `Arc` churn — the source of PR #9's
  ~10% aggregate ClickBench regression.

Result: one decoder, one `BoxStream`, one `EarlyStoppingStream` per
file, with adaptation between row groups.

---

## 4. Where this wins

(Concrete benchmark numbers in §6; this section is the qualitative
shape of the wins.)

- **Hash-join dynamic filters.** The build side publishes `k IN
  hash_set(…) AND k >= min_k AND k <= max_k` after the hash table is
  built. The tracker keeps it PostScan during the placeholder window,
  then promotes once the finalized filter is in place. Hits the
  [dynamic filters blog post benchmark](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/#hash-join-dynamic-filters)
  (`small_table JOIN large_table` with `WHERE v ≥ 50`) without giving
  up the wins. (Note that benchmark's 60×-ish speedup comes from the
  `k >= min_k AND k <= max_k` part *enabling row-group statistics
  pruning* once the build side finishes — that's plan-time stats
  pruning, not row-level filtering. This PR doesn't change stats
  pruning; it changes whether the row-level part runs at all.)
- **TopK / `URL LIKE '%google%'` style.** Highly selective string
  predicates on heavy projections (Q23 territory). RG stats can't
  help (LIKE has no min/max bound). The win is row-level evaluation
  → sparse RowSelection → page-skipping for the projection columns +
  late materialization. Promotion to RowFilter is exactly right here.
- **`pushdown_filters=on` queries that *should* lose.** Q10, Q11, Q13,
  Q14, Q26 — where `extra_bytes == 0` and the filter is uniformly
  selective — start at PostScan and stay there. Same speed as
  `pushdown=off` because the filter is effectively executing as a
  `FilterExec`-shaped post-decode mask.

```
           main pushdown=off  main pushdown=on   PR (adaptive)
           ─────────────────  ────────────────  ─────────────
 Q10/11/13   ▮▮▮▮▮              ▮▮▮▮▮▮▮▮          ▮▮▮▮▮      ← stays at post-scan
 Q23         ▮▮▮▮▮▮▮▮▮          ▮▮                ▮▮         ← promoted to row-filter
 dyn-join    ▮▮▮▮▮▮▮            ▮ (after barrier) ▮          ← promoted once finalised
```

---

## 5. Where this falls short

This PR doesn't try to fix everything `pushdown_filters` could be
slow for. Two distinct gaps remain:

### 5.1 In scope but limited: no sub-row-group adaptation

The adaptive scheduler's swap point is a *row-group boundary*. So:

- **Single-row-group files** (TPC-H tables at SF1 typical) have **no
  swap point inside the file**. The tracker still picks an initial
  placement and accumulates stats *for subsequent files*, but within
  the file the filter is whatever it started as. If the initial
  heuristic lands the wrong call, you eat the whole file with the
  wrong placement.
- **Within-row-group selectivity skew** can't be acted on either — the
  decoder can't be reconfigured mid-row-group (would need an arrow-rs
  `ParquetRecordBatchReader::pause` returning a residual `RowSelection`;
  flagged as deferred in the PR).

### 5.2 Out of scope: the lopsided-partition / data-skew problem

Even when each filter's placement is right, partition skew can
dominate. The PR doesn't change how files / row groups are
distributed to partitions. The fix for that is **morselized scans**:

- **today:** morsel = file. A partition that drew an awkward file
  carries that load alone.
- **near-term:** morsel = row group. A row-group-level work unit
  travels across partitions, so one heavy file can be split across
  multiple streams.
- **future:** morsel = sub-chunk inside a row group. Lets the runtime
  re-balance even within a single big row group (the TPC-H lineitem
  shape).

This work is independent of adaptive filter scheduling and is the
right place to address the TPC-H regression in §6.3 — there's nothing
the in-decoder scheduler can do about a file that *is* one row
group, run by *one* partition, that happens to be the slowest in the
fan-out.

### 5.3 Reading the benchmark numbers through this lens

- ClickBench partitioned (100 files, multiple row groups per file,
  reasonable file-size distribution) → primarily a Problem A
  workload. PR wins decisively.
- TPC-H SF1 (each table is one file, one row group) → primarily a
  Problem B workload. PR can't help; aggregate regresses 44 %, as
  predicted (§6.3). The right fix is morselized scans at row-group
  granularity, not anything in this PR.
- TPC-DS SF1 → mixed. Some queries look like Problem A (Q24, Q50,
  stacked dynamic filters) where the tracker should help and isn't,
  some look like Problem B (single-row-group fact tables). The
  6 %-slower aggregate (§6.2) is the contradiction worth digging
  into separately.

---

## 6. Benchmark numbers

> Runs are 5 iterations each, partitioned dataset (`hits_partitioned`,
> 100 files for ClickBench), local NVMe, M-series Mac. PR is built off
> branch tip `9a1705088`; baseline is the merge-base
> `apache/datafusion@6a09260d5` so we compare like-with-like (no
> unrelated post-divergence churn).
>
> _Tables below are filled in once the dfbench builds finish; see § 6.4
> for the methodology._

### 6.1 ClickBench (43 queries, partitioned, 100 files)

```
                       Total runtime (sum of medians, lower = better)
                       ──────────────────────────────────────────────
   main, pushdown=off    →   21 020 ms
   main, pushdown=on     →   21 699 ms     (+3.2 % vs main_off — pushdown LOSES on aggregate)
   PR  , pushdown=off    →   20 152 ms     (-4.1 % vs main_off)
   PR  , pushdown=on     →   17 190 ms     (-18.2 % vs main_off, -20.8 % vs main_on)  ✓
```

**Hypothesis #1 (user):** PR is faster than main `pushdown=off` *and*
`pushdown=on` on ClickBench → **CONFIRMED** at the aggregate level.

The 18 % aggregate win against `main pushdown=off` is the crucial
result: `pushdown=off` is the safe default users actually run, and the
PR beats it on the standard ClickBench workload while still capturing
the queries where pushdown wins big.

#### 6.1.a Top wins (PR_on vs the better of the two main configs)

| Query | main off (ms) | main on (ms) | PR on (ms) | PR_on / best(main) | what's happening |
|------:|--------------:|-------------:|-----------:|-------------------:|------------------|
| Q11   |          97.7 |         94.2 |       66.2 |          **0.70×** | filter cols overlap projection; tracker keeps post-scan, faster than both |
| Q21   |         859.4 |       1669.2 |      680.1 |          **0.79×** | pushdown regressed badly on main; tracker demotes/keeps post-scan |
| Q24   |          38.0 |         37.0 |       30.2 |              0.82× | small win across the board |
| Q10   |          70.7 |         86.5 |       59.0 |              0.83× | filter-col-in-projection case |
| Q18   |        1665.1 |       2253.4 |     1485.7 |              0.89× | pushdown regressed on main; PR even beats main_off |
| Q22   |        1158.8 |       1197.1 |     1056.0 |              0.91× | small uniform win |
| Q12   |         340.1 |        304.2 |      284.4 |              0.93× | small uniform win |
| Q26   |          47.9 |         55.1 |       45.3 |              0.95× | filter-col-in-projection case |
| Q27   |         705.0 |        965.4 |      670.1 |              0.95× | pushdown regression on main, avoided |

#### 6.1.b Pushdown regressions on main that PR neutralises

These are queries where `main pushdown=on` is materially slower than
`main pushdown=off`, and where PR's adaptive scheduler avoids the trap.
Sorted by main pushdown penalty.

| Query | main_off (ms) | main_on (ms) | main_on/main_off | PR_on (ms) | PR_on/main_off |
|------:|--------------:|-------------:|------------------:|-----------:|---------------:|
| Q29   |          36.1 |         78.6 |             2.18× |       38.9 |          1.08× |
| Q30   |         275.9 |        546.8 |             1.98× |      330.1 |          1.20× |
| Q21   |         859.4 |       1669.2 |             1.94× |      680.1 |          0.79× |
| Q31   |         300.2 |        454.8 |             1.52× |      385.0 |          1.28× |
| Q40   |           9.1 |         13.3 |             1.47× |       11.2 |          1.24× |
| Q32   |        1321.1 |       1880.2 |             1.42× |     1372.5 |          1.04× |
| Q28   |        2423.3 |       3336.1 |             1.38× |     2568.7 |          1.06× |
| Q33   |        1566.8 |       2168.8 |             1.38× |     1561.9 |          1.00× |
| Q27   |         705.0 |        965.4 |             1.37× |      670.1 |          0.95× |
| Q18   |        1665.1 |       2253.4 |             1.35× |     1485.7 |          0.89× |

So the worst pushdown regression on `main` (Q21, ~2×) is fully *erased*
on the PR; the bulk of the others are pulled back to within ~10 % of
`main_off`.

#### 6.1.c The big single-query pushdown win

The "row-level pushdown is amazing on this query" story-piece:

| Query | main_off | main_on | PR_on |
|------:|---------:|--------:|------:|
| Q23   | 3 612 ms | **121 ms** | 167 ms |

`SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY EventTime
LIMIT 10`. **No row-group statistics help here** — `LIKE '%google%'`
has no min/max bound. The win on `pushdown=on` is row-level evaluation
of the LIKE predicate on the URL column, which produces a sparse
`RowSelection`; with page index enabled, most pages of the wide
`SELECT *` projection are skipped entirely (late materialization).
~3.6 s → ~121 ms. PR captures **22× of the 30× speedup** but is not
quite as fast as `main pushdown=on`.

> **Flagging:** Q23 is the one query where the user's "PR is faster
> than both" hypothesis breaks at the per-query level — PR_on is
> 1.37× of main_on. The pattern is "tiny projection (`*`), highly
> selective string-LIKE filter, large file" — exactly the case where
> row-level placement is right and the tracker's confidence-interval
> machinery is paying for samples it doesn't need (the heuristic does
> start at row-level here, since the byte ratio is small, but
> something is leaving cost on the table). Worth digging into.

#### 6.1.d Per-query regressions worth a second look

PR_on > 1.05× best-of-main on these 18 queries (out of 43). Most are
fast queries where absolute deltas are <10ms (Q0 0.7→1.0ms, Q42
7→11ms) — i.e. tracker / per-batch overhead dominates. The non-trivial
ones:

| Q   | main_off | main_on | PR_on | PR_on/best | absolute Δ vs best |
|----:|---------:|--------:|------:|-----------:|-------------------:|
| Q23 |   3612.4 |   121.3 | 166.6 |      1.37× | +45 ms             |
| Q31 |    300.2 |   454.8 | 385.0 |      1.28× | +85 ms             |
| Q5  |    296.6 |   284.0 | 316.2 |      1.11× | +32 ms             |
| Q4  |    225.7 |   221.7 | 240.2 |      1.08× | +18 ms             |
| Q25 |    119.4 |   144.1 | 135.8 |      1.14× | +16 ms             |

These are the rows worth digging into for follow-up tuning. The
tracker's CI thresholds (`min_bytes_per_sec=100 MiB/s`, `z=2.0`) are
conservative; perhaps too conservative for sub-second queries where
the CI doesn't have time to converge before the file is consumed.

### 6.2 TPC-DS (SF1, 99 queries) — Q64-dominated win against `main pushdown=on`

```
   main, pushdown=off    →   17 003 ms
   main, pushdown=on     →   38 961 ms     (+129 % vs main_off — pushdown=on regresses 2.29×)
   PR  , pushdown=on     →   18 056 ms     (+6.2 % vs main_off, -53.7 % vs main_on)
```

The aggregate story changes a lot once `main pushdown=on` is in the
table. Comparing PR vs the *worse* main config (which is what the
[CI bench in PR #11 comment](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340741427)
implicitly does — both branches running with their default
`pushdown_filters` setting), PR is **2.16× faster**.

Almost the entire delta is **Q64**:

| Q64       | local merge-base | local PR (this run) | CI HEAD (2026-04-29) | CI branch |
|-----------|-----------------:|--------------------:|---------------------:|----------:|
| pushdown=off | 470.8 ms      | —                   | —                    | —         |
| pushdown=on  | **20 010 ms** | **646 ms**          | **29 343 ms**        | **971 ms** |

`main pushdown=on` Q64 is ~30× slower than `main pushdown=off` Q64.
The PR with adaptive scheduling identifies that the Q64 dynamic
filters belong post-scan (or at least don't belong at row-level on
the first row groups), and brings Q64 back to within ~1.4× of
`main_off`. That single query closes a ~20 s gap.

If we exclude Q64 from the totals to see what's happening on the
"normal" 98 queries:

| TPC-DS, Q64 excluded | total | vs main_off |
|---------------------|------:|------------:|
| main_off            |  16 532 ms |    — |
| main_on             |  18 950 ms | +14.6 % |
| PR_on               |  17 410 ms |  +5.3 % |

So on the rest of TPC-DS the PR is *small-positive* against `main_on`
(–8 %) and *small-negative* against `main_off` (+5 %).

Bucket counts (out of 99 TPC-DS queries):

|              | wins (PR < 0.95×) | flat (0.95–1.05×) | losses (PR > 1.05×) |
|--------------|------------------:|------------------:|--------------------:|
| TPC-DS SF1   |               20  |                36 |                 43  |

Per-query the picture is roughly even: a chunk of solid wins, a chunk
of flat, and a slightly larger chunk of regressions. Aggregate is
dragged across the line by a handful of mid-sized regressions (Q24
+393 ms, Q64 +175 ms, Q72 +123 ms) outweighing the wins.

#### 6.2.a Top wins on TPC-DS

| Q   | main_off (ms) | PR_on (ms) | speedup |
|----:|--------------:|-----------:|--------:|
|   2 |          99.2 |       69.6 |  1.43×  |
|  66 |         141.5 |      101.3 |  1.40×  |
|  98 |         109.0 |       84.6 |  1.29×  |
|  11 |         534.7 |      413.0 |  1.30×  |
|  12 |          29.5 |       23.4 |  1.26×  |
|   4 |         839.0 |      694.3 |  1.21×  |
|  74 |         324.2 |      278.7 |  1.16×  |

#### 6.2.b Top regressions on TPC-DS

| Q   | main_off (ms) | PR_on (ms) | slowdown | absolute Δ |
|----:|--------------:|-----------:|---------:|-----------:|
|  50 |         122.4 |      284.4 |   2.32×  |   +162 ms  |
|  18 |          81.8 |      183.6 |   2.24×  |   +102 ms  |
|  24 |         434.7 |      827.4 |   1.90×  |   +393 ms  |
|  26 |          49.9 |       90.4 |   1.81×  |    +41 ms  |
|  25 |         211.7 |      361.2 |   1.71×  |   +150 ms  |
|  29 |         172.3 |      294.5 |   1.71×  |   +122 ms  |
|  85 |          94.3 |      155.9 |   1.65×  |    +62 ms  |
|  17 |         137.9 |      197.7 |   1.43×  |    +60 ms  |
|  64 |         470.8 |      646.0 |   1.37×  |   +175 ms  |
|  72 |         404.2 |      527.0 |   1.30×  |   +123 ms  |

#### 6.2.c Hypotheses to check before drawing conclusions

1. **Per-file row-group count.** If TPC-DS's `store_sales` and
   `catalog_sales` files are single-row-group, the in-decoder swap has
   nowhere to swap. Easy to verify with `parquet-tools meta` on a
   sample file.
2. **Stacked dynamic filters in TPC-DS plans.** Q24, Q50, Q29 all
   have correlated subqueries producing dynamic filters with a
   slow-finalising hash-join build side. The tracker may be promoting
   these too eagerly before the build side finishes accumulating.
3. **Tracker overhead per batch on plans with many small batches.**
   Several of the regressed queries are in the 50–500 ms range — same
   regime where ClickBench Q0/Q42 also showed PR overhead.

#### 6.2.d Reconciling with the CI bench numbers

`pushdown_filters` defaults to `false` in DataFusion
([config.rs](https://github.com/apache/datafusion/blob/main/datafusion/common/src/config.rs#L902)),
so the apples-to-apples-on-defaults comparison would be `main_off`
vs `PR_off` (or `PR_on`, since the PR doesn't enable pushdown by
default either). The CI bench at the
[PR comment trigger](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340678050)
*explicitly* set `DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true`
on both branches, so its "1.80× faster" headline is the
`PR_on / main_on` comparison shown above. My local `0.46×` matches.

The strict "PR doesn't regress vs `pushdown=off` defaults" criterion
is **not satisfied on TPC-DS**: PR is +6 % aggregate, with a long
tail of correlated-subquery plans (Q24, Q50, Q29) where the tracker
promotes filters too eagerly. The strict criterion is **also not
satisfied on TPC-H** (+44 %). On ClickBench it is satisfied (–18 %).

### 6.3 TPC-H (SF1, 22 queries) — the unaddressed case

```
   main, pushdown=off    →     780 ms
   main, pushdown=on     →     989 ms      (+27 % vs main_off — pushdown=on regresses)
   PR  , pushdown=on     →   1 128 ms      (+45 % vs main_off, +14 % vs main_on)
```

**Hypothesis #3 (user):** TPC-H is slower than `main pushdown=off`
because SF1 files are single-row-group → confirmed. PR doesn't beat
either main config; it's marginally worse than `main pushdown=on`.
The fix isn't anything the in-decoder scheduler can do —
**single-row-group files have no swap point** and a single partition
owns each file, so this is the data-skew / Problem-B case that needs
[row-group-level morselization (PR #21766)](https://github.com/apache/datafusion/pull/21766).

Bucket counts (out of 22 TPC-H queries):

|             | wins (PR < 0.95×) | flat (0.95–1.05×) | losses (PR > 1.05×) |
|-------------|------------------:|------------------:|--------------------:|
| TPC-H SF1   |                 2 |                 3 |                  17 |

Aggregate is overwhelmingly losses. The two wins (Q1, Q18) are queries
where the predicate is doing the real work; elsewhere the in-decoder
placement just adds cost it can't recoup, because the file is one row
group long.

#### 6.3.a Per-query (all 22)

| Q  | main_off (ms) | PR_on (ms) | ratio | bucket |
|---:|--------------:|-----------:|------:|--------|
|  1 |          48.8 |       43.4 | 0.89× | win    |
|  2 |          14.9 |       15.5 | 1.04× | flat   |
|  3 |          35.2 |       62.9 | 1.79× | LOSS   |
|  4 |          15.7 |       18.2 | 1.16× | loss   |
|  5 |          41.8 |       97.6 | 2.34× | LOSS   |
|  6 |          16.6 |       19.2 | 1.16× | loss   |
|  7 |          50.1 |       78.7 | 1.57× | LOSS   |
|  8 |          35.1 |       79.0 | 2.25× | LOSS   |
|  9 |          44.1 |      136.5 | 3.10× | **LOSS** |
| 10 |          46.5 |       53.9 | 1.16× | loss   |
| 11 |           9.0 |       10.2 | 1.14× | loss   |
| 12 |          23.4 |       27.8 | 1.19× | loss   |
| 13 |          27.4 |       35.7 | 1.30× | loss   |
| 14 |          20.5 |       29.5 | 1.43× | loss   |
| 15 |          27.4 |       27.6 | 1.01× | flat   |
| 16 |          11.0 |       13.6 | 1.23× | loss   |
| 17 |          68.0 |      129.6 | 1.91× | LOSS   |
| 18 |         110.6 |       67.0 | 0.61× | **WIN** |
| 19 |          33.4 |       33.5 | 1.00× | flat   |
| 20 |          30.9 |       58.2 | 1.88× | LOSS   |
| 21 |          53.3 |       68.5 | 1.28× | loss   |
| 22 |          15.9 |       22.3 | 1.40× | loss   |

The pattern: filter-heavy joins on single-row-group `lineitem` (Q5,
Q7, Q8, Q9, Q17, Q20) all get worse. Q18 — the only TPC-H query with
a correlated subquery filtering lineitem first via a hash join — gets
faster, plausibly because the dynamic filter does row-group-stats
pruning when the build finishes early.

This is the data-skew story from §5 made concrete: with no
sub-row-group adaptation point, the in-decoder scheduler has zero
room to recover from a wrong initial placement on these files.

### 6.4 On S3 (`--simulate-latency`)

"S3" in this section means I ran the bench harness with
`dfbench --simulate-latency`, which adds 20–200 ms random latency
to every object-store op
([PR #20954](https://github.com/apache/datafusion/pull/20954)).
Closer to a cloud object-store deployment than local NVMe.
**The picture shifts a lot:** the PR's adaptive scheduler avoids
issuing the small/many requests that `pushdown=on` costs you when
the filter isn't earning its keep, so when RTT is the dominant cost
the gap to `main_off` narrows or closes entirely.

#### 6.4.a TPC-H SF1 on S3 (3 iterations, all 22 queries)

| TPC-H | total | vs main_off | S3 multiplier vs SSD |
|---|---:|---:|---:|
| main_off | 23 723 ms | — | 30.4× |
| main_on  | **52 597 ms** | 2.22× | **53.2×** |
| PR_on    | 24 684 ms | **1.04×** | 21.9× |
| PR_on / main_on | — | **0.47×** (i.e. 2.13× faster) | — |

So with cloud-storage RTT, **PR_on is essentially flat against
`main_off` on TPC-H** — closing the +44 % gap from the SSD run.
The story is no longer "PR can't help on TPC-H"; it's "the SSD
benchmark understates the I/O cost of `pushdown=on`, and PR
neutralises most of that cost". `main_on` gets crushed (53×
multiplier) because each query issues many small filter-cols-then-
survivors decode requests; PR's adaptive scheduler keeps
mandatory-but-unselective filters at PostScan, so the scan issues
one coalesced request per row group instead.

Per-query, PR_on beats main_off on 7 / 22 queries and ties (within
±10 %) on another 8. The remaining 7 are within 1.3× — modest
losses that look like the same "single-row-group, no swap point"
pattern from the SSD run, but the absolute deltas are dwarfed by
the S3 RTT.

#### 6.4.b TPC-DS SF1 on S3 (3 iterations, 99 queries)

| TPC-DS | total | vs main_off | S3 multiplier vs SSD |
|---|---:|---:|---:|
| main_off | 76 418 ms | — | 4.5× |
| main_on  | **141 940 ms** | 1.86× | 3.6× |
| PR_on    | 88 026 ms | 1.15× | 4.9× |
| PR_on / main_on | — | **0.62×** (i.e. 1.61× faster) | — |

Q64 is still the dominant single-query effect: `main_on=22 030 ms`
→ `PR_on=2 267 ms`. Excluding Q64 the totals are:

| TPC-DS, Q64 excluded | total | vs main_off |
|---|---:|---:|
| main_off | 74 286 ms | — |
| main_on  | 119 910 ms | 1.61× |
| PR_on    | 85 759 ms | 1.15× |
| PR_on / main_on | — | **0.72×** |

So the S3 story for TPC-DS is more mixed than TPC-H:

- vs **main_on**: PR keeps the win it had on SSD (0.46× → 0.62×;
  smaller margin but still a clear win).
- vs **main_off**: PR slightly *widens* the gap (1.06× → 1.15×). 14
  of 99 queries are PR-faster than main_off, 16 are flat, 69 are
  modestly slower (1.05–1.85×). Top losses: Q25 (1.83×), Q34
  (1.83×), Q8 (1.76×), Q96 (1.61×), Q26 (1.57×), Q18 (1.56×).

The intuition: many TPC-DS queries have mostly-unselective filters
and small row groups; the PR's adaptive scheduler is still issuing
the row-level filter-column read on the first row group (at
minimum) before the tracker has enough samples to demote. With
200 ms RTT tacked onto each request, that initial
"pay-for-evidence" cost shows up as a 5–80 % per-query regression
on plans that were close to flat on SSD. Worth pointing out: this
is the **main config-tuning lever** — `confidence_z` and
`min_bytes_per_sec` could probably be tightened.

#### 6.4.c ClickBench partitioned on S3 (3 iterations, 43 queries)

| ClickBench partitioned | total | vs main_off | S3 multiplier vs SSD |
|---|---:|---:|---:|
| main_off | 86 624 ms | — | 4.1× |
| main_on  | **111 312 ms** | 1.29× | 5.1× |
| PR_on    | 89 037 ms | **1.03×** | 5.2× |
| PR_on / main_on | — | **0.80×** (i.e. 1.25× faster) | — |

The aggregate wins compress on S3: PR vs main_off goes from `0.82×`
(SSD, 18 % faster) to `1.03×` (essentially tied). The `vs main_on`
advantage (0.80×) is preserved.

Why does the SSD 18 % win shrink to a tie? Two competing effects:

- **Where PR was ahead because main_on regressed:** Q23
  (`URL LIKE '%google%'`), Q27, Q13 — wins survive: Q23 is now
  `4 720 → 1 110 ms` (0.24× of main_off), the biggest single win in
  the suite.
- **New per-query losses on fast queries:** Q37 (2.88×), Q40
  (2.24×), Q38 (2.02×), Q42 (1.96×), Q6 (1.83×), Q41 (1.58×). These
  are ~200 ms queries on main_off that become 700–1 000 ms on
  main_on when each row-level filter-column read pays a 20–200 ms
  RTT. PR captures *about half* of that regression (e.g. Q38:
  main_off 230 → main_on 991 → PR_on 464) — better than main_on
  but not as good as main_off, because the tracker still pays the
  initial-placement cost on the first row group before it has
  evidence to demote.

Per-query bucketing (PR_on vs better-of-main):

|                    | wins | flat | losses |
|--------------------|----:|-----:|-------:|
| ClickBench (SSD)   |   8 |   17 |     18 |
| ClickBench (S3)    |   1 |   28 |     14 |

On S3 the **distribution shifts toward "flat"** — most queries land
within ±5 % of the better-of-main config, the few big wins get
bigger (Q23), the worst losses are sharper but contained (top loss
2.88×, vs 1.55× on SSD). The interpretation: at S3 RTT the tracker's
first-row-group evidence cost is more visible; the
`confidence_z=2.0` default means the tracker waits longer before
demoting, and the cost of that wait scales with per-request RTT.

This is **the most actionable single tuning lever** for follow-up
work: a per-batch-RTT-aware confidence-z, or an "infer initial
placement from the file-pruning hit-rate" heuristic for queries
where row-group stats already tell you the filter is selective.

> Methodology note: latency runs use 3 iterations (vs 5 for the
> non-latency runs) because the per-query wall time is ~25× higher.
> Median across iterations is still used.

### 6.5 Methodology

```bash
# both binaries built from clean target dirs
PR_BIN=/Users/adrian/GitHub/datafusion/target/release/dfbench
MAIN_BIN=/tmp/datafusion-bench-base/target/release/dfbench

# ClickBench partitioned + pushdown
$BIN clickbench --pushdown --iterations 5 \
  --path benchmarks/data/hits_partitioned \
  --queries-path benchmarks/queries/clickbench/queries \
  -o results/clickbench_pushdown_<branch>.json

# ClickBench partitioned + no pushdown
$BIN clickbench --iterations 5 \
  --path benchmarks/data/hits_partitioned \
  --queries-path benchmarks/queries/clickbench/queries \
  -o results/clickbench_nopushdown_<branch>.json

# TPC-DS SF1 (parquet)
$BIN tpcds --iterations 5 --path benchmarks/data/tpcds_sf1 \
  -o results/tpcds_<branch>.json

# TPC-H SF1 (parquet)
$BIN tpch --iterations 5 --path benchmarks/data/tpch_sf1 \
  -o results/tpch_<branch>.json
```

Reported numbers below use the median across the 5 iterations to keep
warmup noise out.

---

## 7. What's missing / open questions

This section deepens two of the most-asked-about follow-up items:
**sub-row-group adaptation** and **smarter initial-placement /
latency-aware policy**.

### 7.1 Sub-row-group adaptation

**Today (this PR + companion arrow-rs branch).** The decoder exposes:
```rust
// arrow-rs `adaptive-strategy-swap` branch
ParquetPushDecoder::can_swap_strategy(&self) -> bool;
ParquetPushDecoder::swap_strategy(&mut self, swap: StrategySwap) -> Result<()>;
```
`can_swap_strategy()` returns `true` only between row groups (state
`ReadingRowGroup` + inner `Finished`). Calling `swap_strategy()`
mid-row-group errors with `ParquetError::General`. So the smallest
unit at which the tracker can change placement is **one row group**.

For ClickBench partitioned (~10–30 row groups per file) that's plenty.
For TPC-H SF1 (one row group per file) it's nothing — the first
placement decision is the only decision.

**Why mid-row-group is hard in arrow-rs.** A `ParquetRecordBatchReader`
holds three pieces of mutable state inside one row group:

1. `Box<dyn ArrayReader>` — column readers, each positioned at some
   page + within-page offset, possibly with the dictionary already
   loaded.
2. `ReadPlan { row_selection_cursor: RowSelectionCursor }` — tracks
   which rows of the row group are still unread. Already pauseable
   (cursor exposes `row_selection_cursor_mut()`).
3. The `RowFilter` itself — wired into the pre-decode column subset
   that `ParquetRecordBatchReader::next_inner` runs on each batch
   (see `mod.rs:1426`). Changing the filter changes which columns
   need to be pre-decoded, which changes the array reader topology.

Item (3) is what blocks an in-place swap — the array readers built
for "pre-decode only filter cols, then late-materialise survivors"
are structurally different from "decode the full projection in one
shot". Swapping the filter requires re-building the array readers
for the row group.

**The realistic API shape.**
```rust
impl ParquetPushDecoder {
    /// True at any record-batch boundary inside a row group.
    pub fn can_pause(&self) -> bool;

    /// Stop emitting from the current row group, return the residual
    /// state. Bytes already fetched stay in `PushBuffers`.
    pub fn pause(&mut self) -> Result<PausedRowGroup>;

    /// Resume the same row group with a different filter / projection.
    /// Re-builds array readers for the row group; `PushBuffers` is
    /// reused so no extra IO is needed for column data still in scope.
    pub fn resume_with(
        &mut self,
        paused: PausedRowGroup,
        new_strategy: StrategySwap,
    ) -> Result<()>;
}

pub struct PausedRowGroup {
    row_group_idx: usize,
    cursor: RowSelectionCursor,   // residual rows
    // PushBuffers stays inside the decoder
}
```

**Cost model.** Resume cost is dominated by re-decoding compressed
pages from the start of whichever pages contain the resume row.
Compressed pages are typically 1 MiB; on M-series hardware that's
~1–5 ms per page per column. Resuming with a strict superset of the
original projection is essentially free for already-decoded columns
(if we keep them) and ~1 ms-per-page for new columns. Decompression
work is duplicated for columns whose state we threw away.

**What this would unlock.**

| Workload                                     | benefit |
|----------------------------------------------|--------|
| TPC-H SF1 (one big row group per file)       | The big one. Today the tracker eats the whole row group with the initial placement; sub-RG would let it demote partway through. The latency case (§6.4.a) is already a tie; sub-RG would close the no-latency gap (1.45× → ?). |
| ClickBench, intra-RG skew                    | Smaller. Most ClickBench wins / losses already track row-group boundaries; per-RG decisions cover most of the value. |
| Hash-join dynamic filters that finalise mid-RG | The dynamic filter's `snapshot_generation` already triggers a re-evaluation in the tracker, but only at the next RG boundary. Sub-RG would let the new filter take effect immediately. |

**Important caveat.** Sub-RG adaptation does **not** address Problem B
(lopsided partitions, §2.3). A single big row group still belongs to
one partition; sub-RG only changes how that partition processes the
row group, not how the work distributes across partitions. The
real fix for TPC-H lineitem is morselization at row-group or smaller
granularity ([#21766](https://github.com/apache/datafusion/pull/21766)),
which is independent of sub-RG adaptation.

### 7.2 Smarter initial placement: latency-aware `confidence_z` + page-pruning prior

The tracker's two policy knobs (`min_bytes_per_sec`, `confidence_z`)
define a "wait for evidence before changing placement" gate. Default
`z = 2.0` ≈ 97.5 % one-sided. Under simulated latency (§6.4) we saw
that the cost of waiting is what drives the residual `vs main_off`
regressions on TPC-DS and fast ClickBench queries (Q37 2.88×, Q40
2.24×, Q38 2.02×, Q42 1.96×). Two complementary directions:

#### 7.2.a Latency-aware `confidence_z`

The current promotion / demotion rule is:
```text
demote if  upper_bound(eff, z) < min_bytes_per_sec
promote if lower_bound(eff, z) ≥ min_bytes_per_sec
```
where `eff = bytes_saved_per_sec`. Higher `z` ⇒ wider CI ⇒ more
samples needed before either side fires.

The **wrong-placement cost is amplified by per-request latency**: a
filter stuck at row-level under high latency pays an RTT for every
row group's filter-column fetch. If we estimate effective
per-fetch wall time, we should be willing to demote on weaker
evidence.

Concrete sketch:
```rust
// In SelectivityTracker::partition_filters, for the row-level demote check:
let latency_factor = (per_fetch_ms / FAST_PATH_MS).clamp(1.0, 8.0);
let z_effective   = self.config.confidence_z / latency_factor;
if let Some(ub) = stats.confidence_upper_bound(z_effective) { ... }
```
Where `per_fetch_ms` is the running average of `time_elapsed_scanning_total /
num_byte_ranges_fetched` (both already in `ParquetFileMetrics`).
With `FAST_PATH_MS = 5 ms` (rough SSD baseline) and 100 ms
per-fetch RTT under latency, `z_effective` shrinks from 2.0 to ~0.1
— effectively demoting on point estimates.

This wouldn't change the no-latency results (where `latency_factor`
clamps to 1.0) but would close the residual TPC-DS-lat (+15 %) and
fast-query-lat (Q37/Q40/Q38) gaps that are *all* on the slow side
of "wait too long to demote".

Implementation cost: ~30 LOC in `selectivity.rs` plus a hook on the
opener to feed back per-fetch latency into the tracker.

#### 7.2.b Page-pruning-driven initial placement

The current initial-placement heuristic
(`SelectivityTrackerInner::partition_filters`, `selectivity.rs:778`)
is purely byte-ratio:
```rust
if extra_bytes > 0 && extra_bytes / projection_bytes <= 0.20 { RowFilter }
else                                                         { PostScan }
```
This is "row-level if the filter is cheap to read", with no notion
of *selectivity*. But by the time this runs, DataFusion has already
done **row-group statistics pruning** at
`opener.rs:930-995`; the `pruning_predicate` has been evaluated
against per-row-group min/max, and the surviving row group set is
known. Per-conjunct pruning effectiveness is the single best
selectivity prior we'll ever get for free.

Concrete sketch of a smarter prior:
```rust
fn selectivity_prior(
    expr: &Arc<dyn PhysicalExpr>,
    file_metadata: &ParquetMetaData,
    file_schema: &Schema,
) -> Option<f64> {
    // Build a per-conjunct PruningPredicate (already plumbed via
    // datafusion_pruning::build_pruning_predicate).
    let pp = build_pruning_predicate(expr.clone(), file_schema)?;
    let total = file_metadata.row_groups().len();
    let kept  = pp.prune(file_metadata)?.iter().filter(|b| **b).count();
    let pruned_rate = (total - kept) as f64 / total as f64;
    Some(pruned_rate)
}
```
Then in `partition_filters` for new filters:
```rust
match selectivity_prior(&expr, metadata, schema) {
    Some(p) if p >= 0.5 => RowFilter,    // statistically selective
    Some(p) if p <= 0.05 => PostScan,    // not selective per stats
    _                    => byte_ratio_heuristic(...)  // fallback
}
```
**What this would buy:**

- **Q23** (`URL LIKE '%google%'`) already lands at row-level by the
  byte-ratio heuristic; this wouldn't change it. But for
  similarly-shaped LIKE-on-URL queries with bigger projections, the
  prior would reinforce row-level placement against what the byte
  ratio alone might suggest.
- **Fast-query latency regressions** (Q37/Q40/Q38/Q42): these are
  filters where `extra_bytes > 0` and `byte_ratio ≤ 0.20`, so they
  start at row-level today. But row-group stats pruning is not
  effective for them (they're aggregations on `SearchEngineID` etc.,
  not selective range predicates). With the prior, `pruned_rate ≈
  0` would override the byte-ratio default and start them at
  PostScan — neutralising the latency regression *before* the
  tracker has to learn it via samples.
- **TPC-DS correlated-subquery regressions** (Q24/Q25/Q26/Q34/Q50):
  the dynamic filter's `snapshot_generation` already triggers a
  re-evaluation; combining that with a stats-based prior would let
  the tracker make a sound decision on the first row group instead
  of needing to gather samples.

Implementation cost: builds on the `PruningPredicate` machinery
already in `datafusion_pruning::build_pruning_predicate`. Would
need to construct per-conjunct predicates (currently only the
combined predicate is built). ~50–100 LOC.

#### 7.2.c Bonus: page-index prior

A step further: even when row-group stats pruning rate is 0%, the
**column index / offset index** can give per-page stats. If we run
page-index pruning on the *first* row group only, we can compute a
"would-be pages-skipped" rate as a finer-grained prior. DataFusion
already has the machinery (`PagePruningAccessPlanFilter` in
`opener.rs:325`), but it's currently consumed during row-group
processing, not as input to the tracker's initial placement
decision.

This is strictly better than the row-group stats prior for
predicates that are page-selective but not row-group-selective,
which is most LIKE / range-query workloads.

### 7.3 Other follow-ups (unchanged)

3. **Cross-partition row-group shuffling.** Today morselization moves
   *files* across partitions but not row groups
   ([#21351](https://github.com/apache/datafusion/pull/21351) merged,
   [#21766](https://github.com/apache/datafusion/pull/21766) draft).
   This is the actual fix for the TPC-H regression in §6.3 — sub-RG
   adaptation (§7.1) helps but doesn't redistribute work across
   partitions.
4. **Three new config knobs (`filter_pushdown_min_bytes_per_sec`,
   `filter_collecting_byte_ratio_threshold`, `filter_confidence_z`)
   aren't in the proto schema yet.** `from_proto` fills with defaults
   so a roundtrip preserves behavior; needs a follow-up to plumb
   through.
5. **Confidence interval defaults** were tuned by hand against
   ClickBench. After §7.2 lands they would mostly become a fallback
   for the page-index-pruning-doesn't-help cases.

### 7.4 Suggested ordering

If I were prioritising the four directions above, I'd order them by
expected cost / impact ratio:

| # | Item | Cost | Impact | Why |
|--|------|------|--------|-----|
| 1 | §7.2.b page-pruning prior | low (~50–100 LOC) | high | closes most fast-query latency regressions; prevents bad first placements; reuses existing infrastructure |
| 2 | §7.2.a latency-aware z   | low (~30 LOC)     | medium-high | closes residual TPC-DS-lat gap; cheap to ship next to §7.2.b |
| 3 | §7.3 #3 row-group morselization (separate PR) | high | high on TPC-H | only thing that fixes the partition-skew problem; tracked elsewhere |
| 4 | §7.1 sub-RG adaptation in arrow-rs | high (state mgmt across array readers) | medium-high but workload-specific | bigger arrow-rs surface; benefits TPC-H even after morselization for "dynamic filter that finalises mid-RG" cases |

---

## 8. Speaker-notes summary (10-min talk shape)

1. **Hook (1m)** — `pushdown_filters` is a per-query lottery on `main`. Show
   one query that wins big, one that loses big.
2. **Why (2m)** — IO patterns + per-batch eval cost diagrams (§2.2).
3. **The lopsided-partition slide (1m)** — the one in §2.3.
4. **Solution (2m)** — state machine diagram + the swap-at-RG-boundary
   diagram (§3.1, §3.2).
5. **Wins (2m)** — ClickBench aggregate + 3 representative queries
   (TopK / hash-join dyn / `WHERE col<>'' GROUP BY col`).
6. **Where it falls short (1m)** — TPC-H + the deferred sub-row-group
   pause story (§5, §7).
7. **Close (1m)** — "never slower than `pushdown=off`" success
   criterion, status, what's next.

---

## 9. References

### This PR and its CI bench

- [adriangb/datafusion#11 — Adaptive filter scheduling in the Parquet decoder](https://github.com/adriangb/datafusion/pull/11) (this PR)
- [pydantic/arrow-rs#9 — `adaptive-strategy-swap`](https://github.com/pydantic/arrow-rs/pull/9) (companion arrow-rs change adding `swap_strategy` / `can_swap_strategy` / `PushBuffers` reuse)
- [PR #11 CI bench result](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340741427) — TPC-DS table cited in §6.2.
- [PR #11 CI bench trigger](https://github.com/adriangb/datafusion/pull/11#issuecomment-4340678050) — confirms both branches ran with `DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true` and `..._REORDER_FILTERS=true`.

### Filter-pushdown-on-by-default work (apache/datafusion)

- [#3463 — Enable parquet filter pushdown by default](https://github.com/apache/datafusion/issues/3463) — the long-running tracking issue this PR is ultimately in service of.
- [#20324 — \[EPIC\] Fix performance regressions when enabling parquet filter pushdown (late materialization)](https://github.com/apache/datafusion/issues/20324) — epic enumerating the regressions adaptive scheduling is meant to neutralize.
- [#20325 — ClickBench Q10 slows down when filter pushdown is enabled](https://github.com/apache/datafusion/issues/20325) — concrete example of the "Problem A" lottery.

### Morselized scans (the "Problem B" / data-skew fix path)

- [#20529 — Introduce morsel-driven Parquet scan](https://github.com/apache/datafusion/issues/20529) — the original tracking issue for morselization.
- [#21351 — Dynamic work scheduling in `FileStream`](https://github.com/apache/datafusion/pull/21351) — **merged**: file-level morselization (siblings steal whole files from a shared queue). Closes the "files are units of work" half of #20529 but explicitly defers row-group splitting.
- [#21766 — feat(parquet): row-group morselization for sibling FileStream stealing](https://github.com/apache/datafusion/pull/21766) — **draft**: row-group-level morselization, the actual fix for the TPC-H regression in §6.3.

### Other related apache/datafusion threads

- [#19654 — Poor performance of parquet pushdown with offset](https://github.com/apache/datafusion/issues/19654)
- [#21317 — Sort pushdown: reorder row groups by statistics within each file](https://github.com/apache/datafusion/issues/21317)
- [#21915 — feat: OFFSET pushdown for multi-file parquet scans](https://github.com/apache/datafusion/issues/21915)
- [#21916 — feat: OFFSET pushdown with WHERE filters using fully-matched RG statistics](https://github.com/apache/datafusion/issues/21916)
- [#21440 — Dynamic BufferExec sizing: row limit + memory cap for sort pushdown](https://github.com/apache/datafusion/issues/21440)
- [#20443 — Support filter pushdown through `SortMergeJoinExec`](https://github.com/apache/datafusion/issues/20443)
- [#21145 — Add example implementing filter pushdown](https://github.com/apache/datafusion/issues/21145)
