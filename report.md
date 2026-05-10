# Adaptive filter scheduling in the Parquet decoder

> This is a discursive write-up of a proposed change to DataFusion's
> Parquet scan. The companion `design.md` is the spec; this report is
> the analysis — what the change does, why, where it wins, where it
> doesn't, and the benchmark drill-downs that support the headline
> claims. Numbers are sums-of-medians (lower is better) across queries
> for the workload. The change is built on top of
> `apache/datafusion@main`; a companion arrow-rs change adds the
> in-decoder strategy-swap APIs (`can_swap_strategy`, `swap_strategy`,
> reusable `PushBuffers`).

## TL;DR — does the work pay off?

**Local NVMe** — sum-of-medians, ms

| Workload | main | main + pushdown | **change** | change / main | change / main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench partitioned (43 q) | 21 020 | 21 699 | **17 919** | **0.85×** ✓ | **0.83×** ✓ |
| TPC-DS SF1 (99 q) | 17 003 | 38 961 | **16 852** | **0.99×** ✓ | **0.43×** ✓ |
| TPC-H SF1 (22 q) | 780 | 989 | **691** | **0.89×** ✓ | **0.70×** ✓ |

**`--simulate-latency`** (`dfbench --simulate-latency`, 20–200 ms per object-store op)

| Workload | main | main + pushdown | **change** | change / main | change / main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench partitioned (43 q) | 86 562 | 111 321 | **88 947** | 1.03× ≈ | **0.80×** ✓ |
| TPC-DS SF1 (99 q) | 76 418 | 141 940 | **77 546** | 1.01× ≈ | **0.55×** ✓ |
| TPC-H SF1 (22 q) | 23 723 | 52 597 | **24 157** | 1.02× ≈ | **0.46×** ✓ |

Two headlines:

1. The success criterion — `pushdown_filters = on` with the change is
   never slower than today's `pushdown_filters = off` on `main` — holds
   on every workload-platform pair. On local SSD the change beats
   `main` outright by 15–30 %; on simulated cloud storage it lands
   within 1–3 % of `main`, inside run-to-run noise.
2. Against `pushdown_filters = on` on `main` — the configuration the
   change is meant to neutralise — the change is 17–57 % faster in
   every cell.

The S3-latency profile (`--simulate-latency`,
[#20954](https://github.com/apache/datafusion/pull/20954)) adds 20–200 ms
of random latency per object-store op. It's not a real AWS run, but the
latency profile matches what you'd see against any cloud object store.

---

## 1. The problem

`pushdown_filters = on` is a per-query lottery on `main` today.

- **When it wins** — the predicate is selective enough that the
  row-level evaluator quickly produces a sparse `RowSelection`; the
  page index lets the reader skip whole pages of the projection
  columns outright; what survives is decoded only for the surviving
  rows (late materialisation). Heavy projections + selective filters
  get close to free.
- **When it loses** — the filter is mandatory but unselective, the
  filter column also overlaps the projection, or both. Then
  `pushdown=on` buys nothing — no rows skipped, no pages skipped — but
  pays per-batch ArrowPredicate eval cost, smaller masks, more
  iterations than one big post-scan apply, and extra I/O for filter
  columns not in the projection. Under cloud-storage latency the
  many-small-GETs profile of `pushdown=on` compounds the cost.

Both shapes coexist on real workloads, so the config is gated off by
default. [#3463](https://github.com/apache/datafusion/issues/3463) —
the long-running ask to flip `pushdown_filters` on by default — has
stayed open for years for exactly this reason.

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

**Goal of the change:** never make a query slower than today's
`pushdown_filters = off` default, while still capturing the wins on
queries that benefit from row-level pushdown.

---

## 2. Background: how a Parquet scan in DataFusion is shaped

### 2.1 Partitions and files

A `DataSourceExec(ParquetSource)` is replicated across
`target_partitions` streams. The planner assigns each *file* to one
partition (file-level distribution; no within-file splitting in the
public path today). For each file the partition gets, the source
builds an opener that lazily streams record batches.

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

A *row group* is the unit at which Parquet stores statistics and at
which the decoder can pause and reconfigure. A typical ClickBench
partitioned file has ~10–30 row groups; a TPC-H lineitem SF1 file has
**one**.

### 2.2 Row filter vs. post-scan filter

Row-level pushdown filtering is implemented in arrow-rs as a
`RowFilter` (`Vec<Box<dyn ArrowPredicate>>`). Each predicate's
`evaluate(RecordBatch) -> BooleanArray` runs on the actual decoded
*data* of the filter columns; the boolean mask is converted into a
`RowSelection` that subsequent predicates and the projection inherit.
With the page index enabled, pages whose surviving rows are empty are
skipped entirely for the projection columns. None of this involves
row-group statistics — those are a separate, plan-time mechanism.

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
            │   Pros: late materialisation — projection columns barely         │
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
> available regardless of `pushdown_filters`. A `WHERE k >= 50 AND k
> <= 10000` predicate can prune entire row groups whether pushdown is
> on or off. What `pushdown_filters=on` *adds* is **row-level / page-
> level filtering inside surviving row groups** (driven by actual
> data, not stats) plus **late materialisation of projection
> columns**. Some predicates (`URL LIKE '%google%'`) get nothing from
> RG stats but everything from row-level filtering; some (range
> predicates with bounded distributions) get the inverse; some get
> both.

### 2.3 Two different problems with `pushdown=on`

It's worth separating two distinct reasons `pushdown=on` can lose to
`pushdown=off`. **The change in this proposal addresses the first;
the second is a different story (morselized scans).**

**Problem A — ineffective-filter cost (the scope of this change).**
When a filter is mandatory but unselective, `pushdown=on` pays
per-batch predicate eval, smaller masks, two-stage I/O, and possible
double decode without saving any work. So a query that *would* be
fine with `FilterExec` post-scan ends up slower with the filter
wedged into the decoder.

**Problem B — lopsided partitions / data skew (out of scope here).**
Even when each individual filter is correctly placed, the partition
layout can leave one stream doing 10× the work of its neighbours
because *files* are the unit of distribution.

```
    skew shape (independent of what the filter does):
    ─────────────────────────────────────────────────
    p0 ▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮▮ ← unfortunate file: low pruning, all rows survive
    p1 ▮                     ← lucky file: stats prune the whole RG
    p2 ▮▮▮▮                  ← partial prune
    …
       └────────── stragglers gate the whole stage ──────────┘
```

Problem B's fix is **morselized scans**: today the morsel
granularity is per-file; the future direction is per-row-group (or
sub-chunk), so that one ill-fortuned file can't gate a stage. That
work is tracked at
[#21766](https://github.com/apache/datafusion/pull/21766) and is
independent of this proposal. Filter placement (the scope of the
change in this proposal) only governs *which* filter runs *where*
inside a single scan's per-partition stream; it does not
redistribute work across partitions.

---

## 3. The change: adaptive in-decoder filter scheduling

> **Scope reminder:** this section describes the fix for **Problem A**
> from §2.3 — the per-filter ineffective-cost lottery. It does **not**
> address Problem B (lopsided partitions / data skew); that requires
> row-group-granularity morsel scans, which is separate work.

### 3.1 What's new

For each `ParquetSource`, every conjunct of the source-level predicate
gets a stable `FilterId`. A `SelectivityTracker` (shared across all
files of one `ParquetSource`) tracks per-`FilterId` statistics and
assigns each filter to one of three states:

```
              ┌──────────┐
              │   New    │
              └─────┬────┘
                    │  initial placement
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
(~97.5 %); the threshold is `filter_pushdown_min_bytes_per_sec =
100 MiB/s` by default. The "scatter-aware" version of the metric
counts only sub-batch *windows* where the filter killed every row —
a uniformly 50 %-selective filter scores ~0 and stays post-scan; a
TopK-style filter that empties whole pages blows past the threshold.

```
             SelectivityStats   (Welford online stats)
             ─────────────────
              eff_mean   ← bytes-saved-per-sec mean over batches
              eff_m2     ← M2  → variance → stddev
              CI_lo      ← mean − z·σ/√n   (used to PROMOTE)
              CI_hi      ← mean + z·σ/√n   (used to DEMOTE)
```

### 3.2 Initial placement from per-conjunct pruning rates

The cheapest, highest-quality selectivity signal available comes
from the three pruning passes DataFusion already runs at file open:
page-index pruning, row-group min/max pruning, and bloom-filter
pruning. Each evaluates the file's predicates against a different
container granularity. Their collective output today is a single
`Vec<bool>` — "keep this row group" — which loses the per-conjunct
information.

The change introduces a per-conjunct API on `PruningPredicate`
(`try_new_tagged_conjuncts`, `prune_per_conjunct`) and instruments
the three pruning passes with `_with_per_conjunct_stats` variants
that emit a `Vec<PerConjunctPruneStats>` alongside the existing
output. `ParquetOpener` accumulates these into a single
`HashMap<FilterId, f64>` and threads it to the tracker. Initial
placement is then:

```
prior = page_pruning_rates.get(&id)
match prior {
    Some(p) if p >= prior_promote_threshold (default 0.5) => RowFilter,
    Some(p) if p <= prior_demote_threshold  (default 0.05) => PostScan,
    _ => byte_ratio_heuristic(...)   // fallback for files with no page index
}
```

The byte-ratio fallback — extra-bytes-for-filter-cols /
projection-bytes — runs only when the pruning passes had nothing to
say about the filter (e.g. a ClickBench file with no page index, on
a predicate that isn't single-column for row-group stats). The
common case is that a real prior is available before the scan even
starts.

### 3.3 Refresh prior for populated dynamic filters

Hash-join build-side filters arrive at plan-time as
`OptionalFilterPhysicalExpr`-wrapped `DynamicFilterPhysicalExpr`s
whose `snapshot_generation()` is zero. The build side publishes a
populated predicate later; the file-open rate captured for the
placeholder is then stale.

`fresh_rate_for_dynamic_conjunct` re-evaluates a per-conjunct
`PruningPredicate` against the file's row-group statistics
*only* for filters where `snapshot_generation > 0`. It tries the
whole conjunct first; if the predicate rewriter can't fold the
`col >= lo AND col <= hi AND hash_lookup(...)` shape into a
`PruningPredicate`, it splits the snapshotted inner expression and
takes the max rate across prunable sub-parts, returning that as a
**promote-only** signal (rate ≥ 0.5). Below 0.5 it returns `None`
and the standard prior fallback decides — a partial AND
undercounts and would mislead a demote.

### 3.4 Latency-aware confidence shrink

Under cloud-storage RTT the cost of holding a wrong placement
scales with per-fetch wall time. The effective confidence z shrinks
when per-fetch wall time exceeds a baseline:

```rust
fn effective_z(&self) -> f64 {
    let avg_fetch_ms = total_fetch_ns / total_fetches / 1e6;
    if avg_fetch_ms <= latency_z_baseline_ms { return confidence_z; }
    let factor = (avg_fetch_ms / latency_z_baseline_ms)
        .clamp(1.0, latency_z_max_scale);
    confidence_z / factor
}
```

`ParquetOpener` feeds `record_fetch(n_ranges, elapsed_ns)` from
each `get_byte_ranges` call. With defaults
`latency_z_baseline_ms = 5.0` and `latency_z_max_scale = 8.0`,
local NVMe (< 5 ms per fetch) is unaffected (`factor = 1`); under
100 ms RTT the effective z shrinks to ~0.4, which lets the
scheduler demote on a point estimate instead of waiting for a tight
confidence interval.

### 3.5 Where the swap happens

The big mechanical piece: **the decoder is rebuilt at row-group
boundaries without re-opening the file**. The companion arrow-rs
change adds:

```rust
ParquetPushDecoder::can_swap_strategy(&self) -> bool;          // true between RGs
ParquetPushDecoder::swap_strategy(&mut self, swap)             // replace
                                  -> Result<()>;               //   filter / projection
                                                               //   between RGs
```

`PushBuffers` survives the swap, so any column bytes already
fetched that are still in the new strategy's projection are reused.

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
decoded (the decoder can't be reconfigured mid-RG) but the predicate
is not evaluated.

---

## 4. Where this wins

### 4.1 ClickBench

```
                 Total runtime (sum of medians, lower = better)
                 ──────────────────────────────────────────────
   main, pushdown=off    →   21 020 ms
   main, pushdown=on     →   21 699 ms     (+3 % vs main — pushdown LOSES at aggregate)
   change, pushdown=on   →   17 919 ms     (−15 % vs main, −17 % vs main+pushdown)  ✓
```

ClickBench partitioned (43 queries, 100 files of `hits_partitioned`) is
the workload `pushdown=on` should win on the most — selective
predicates with heavy projections. The change beats `main + pushdown`
across the board *and* beats `main` (the safe default) by 15 %.

**Top wins vs `main`** (change/main < 0.95×):

| Query | main (ms) | main + pd (ms) | change (ms) | change/main | what's happening |
|------:|----------:|---------------:|------------:|------------:|-----------------|
| Q23 | 3 612 | **121** | **168** | **0.05×** | `URL LIKE '%google%'`: tiny projection, RG stats can't help, row-level eval is the win. Change starts at row-level via the byte-ratio fallback. |
| Q11 | 98 | 94 | 73 | 0.74× | filter cols overlap projection — change keeps post-scan, faster than both |
| Q24 | 38 | 37 | 30 | 0.79× | small uniform win |
| Q21 | 859 | **1 669** | 685 | **0.80×** | pushdown regressed badly on `main` (1.94×); change demotes / keeps post-scan |
| Q12 | 340 | 304 | 277 | 0.82× | small uniform win |
| Q10 | 71 | 87 | 62 | 0.88× | filter-col-in-projection; change avoids the row-level cost |

**Pushdown regressions on `main` that the change neutralises** —
queries where `main + pushdown` is materially worse than `main`,
sorted by `main+pushdown / main`:

| Query | main (ms) | main + pd (ms) | +pd / main | change (ms) | change / main |
|------:|----------:|---------------:|-----------:|------------:|--------------:|
| Q29 | 36 | 79 | **2.18×** | 37 | 1.03× |
| Q30 | 276 | 547 | **1.98×** | 297 | 1.08× |
| Q21 | 859 | 1 669 | **1.94×** | 685 | **0.80×** |
| Q31 | 300 | 455 | 1.52× | 416 | 1.39× |
| Q32 | 1 321 | 1 880 | 1.42× | 1 645 | 1.25× |
| Q28 | 2 423 | 3 336 | 1.38× | 2 615 | 1.08× |
| Q33 | 1 567 | 2 169 | 1.38× | 1 638 | 1.05× |
| Q27 | 705 | 965 | 1.37× | 744 | 1.05× |
| Q18 | 1 665 | 2 253 | 1.35× | 1 551 | 0.93× |

The worst pushdown regression on `main` (Q21, ~2×) is fully
*erased* on the change; the rest are pulled back to within ~10 % of
`main`.

The one residual loss vs `main` worth flagging is **Q31** at 1.39×.
The pattern there is "fast, mid-selectivity filter on a wide
projection" — the byte-ratio fallback chooses row-level, the
runtime samples slowly drift toward "this isn't pulling its weight",
and the tracker takes a few row groups to demote. The
`main + pushdown` cell is worse (1.52×), so it's still a net win
against the configuration the change is meant to neutralise; but
it's the marker for "warm-up cost on the first row group" as a
known residual (§8.1 of the design doc).

### 4.2 TPC-DS — the Q64 story

```
   main, pushdown=off    →   17 003 ms
   main, pushdown=on     →   38 961 ms     (+129 % vs main — pushdown=on regresses 2.29×)
   change, pushdown=on   →   16 852 ms     (−1 % vs main, −57 % vs main+pushdown)
```

Almost the entire delta vs `main + pushdown` is **Q64**:

| Q64 | main | main + pd | **change** |
|----|---:|---:|---:|
| local SSD | 471 ms | **20 010 ms** ⚠ | **474 ms** ✓ |

A 42× regression on `main + pushdown` is neutralised to flat. Q64 is
a dynamic-filter chain (correlated subqueries producing hash-join
build-side filters); `main + pushdown` evaluates the chain at
row-level *before the build side has finished*, so each row group
pays for re-evaluation of a placeholder predicate. The change keeps
those filters post-scan until `snapshot_generation > 0`, then the
re-evaluated rate (§3.3) makes the right call.

That's the single biggest neutralisation in the suite. Other TPC-DS
pushdown regressions on `main` are similar in shape, all
neutralised:

| Query | main (ms) | main + pd (ms) | +pd / main | change (ms) | change / main |
|------:|----------:|---------------:|-----------:|------------:|--------------:|
| Q64 | 471 | **20 010** | **42.5×** | 474 | **1.01×** |
| Q18 | 82 | 214 | 2.62× | 78 | 0.96× |
| Q50 | 122 | 318 | 2.60× | 123 | 1.01× |
| Q72 | 404 | 787 | 1.95× | 416 | 1.03× |
| Q9 | 102 | 188 | 1.85× | 83 | **0.81×** |
| Q24 | 435 | 747 | 1.72× | 427 | 0.98× |

Per-query the picture is essentially flat against `main`: a handful
of solid wins, a long tail within ±10 %, no remaining big losers.

### 4.3 TPC-H — the once-regressed workload

```
   main, pushdown=off    →     780 ms
   main, pushdown=on     →     989 ms      (+27 % vs main — pushdown=on regresses)
   change, pushdown=on   →     691 ms      (−11 % vs main, −30 % vs main+pushdown)
```

TPC-H SF1 is the canonical single-row-group case: `lineitem` is one
file of one row group. Earlier iterations of this work regressed
TPC-H by 45 % vs `main + pushdown=off` because the in-decoder
scheduler had nowhere to swap *within* the file — once initial
placement was wrong, it stayed wrong for the whole file.

The change inverts that result and lands at **0.89× of `main`** —
better than either `main` configuration. The story is entirely
about correct row-level placement on the filters that benefit:

- **Q18 is the headline.** Its `l_quantity IN (subquery)` is the
  canonical hash-join dynamic filter. Once the build side
  publishes a populated predicate, the dynamic-filter refresh
  (§3.3) re-evaluates the prior and promotes it to row-level. Q18
  lands at **0.59× of `main`** — 46 ms saved on a 110 ms baseline,
  more than half the aggregate 89 ms delta.
- **Q1, Q3, Q19 contribute smaller wins** (0.81–0.86×) from
  row-level placement on selective date and range predicates that
  generate sparse `RowSelection`s and let the projection skip
  pages.
- **The post-scan-placed filters wash.** When the scheduler picks
  post-scan placement, the filter runs inside the parquet opener
  (`apply_post_scan_filters_with_stats`) rather than via a
  `FilterExec` operator above the scan as `main + pushdown=off`
  would. The two paths are equivalent in cost — the in-scan
  application doesn't buy anything by itself. The win is the
  correct row-level placement on the queries above, not the
  in-scan-vs-above-scan choice for the rest.

Per-query, every TPC-H query is at parity-or-better vs `main`; the
worst pushdown regressions on `main` (Q17 1.90×, Q12 1.89×, Q22
1.74×, Q9 1.66×, Q15 1.55×) all collapse to within 5 % of `main`
on the change.

The post-scan path is per-partition — there is no shuffle
re-balance involved. Cross-partition skew on single-row-group
files (one file's worth of work concentrated on one partition) is
out of scope for this change; the fix is row-group morselization
([#21766](https://github.com/apache/datafusion/pull/21766)).

### 4.4 The `--simulate-latency` profile

Under simulated S3 latency the gaps to `main + pushdown=on` widen,
because the regression cost on `main + pushdown` is dominated by
per-request RTT and the change avoids most of those requests:

| Workload | main | main + pushdown | change | change / main+pushdown |
|---|--:|--:|--:|--:|
| ClickBench S3 | 86 562 | **111 321** | 88 947 | **0.80×** |
| TPC-DS S3 | 76 418 | **141 940** | 77 546 | **0.55×** |
| TPC-H S3 | 23 723 | **52 597** | 24 157 | **0.46×** |

On TPC-H S3 the change closes the 2.22× regression from
`main + pushdown` to flat against `main` (1.02×, inside run-to-run
noise). Same shape for TPC-DS and ClickBench.

The residual S3 losses are mostly on fast ClickBench queries that
have no page index for the filter column, so the byte-ratio
fallback fires and the tracker pays a row group or two of evidence
before demoting. Q6, Q42, Q37, Q38 are the canonical examples:
~200 ms on `main`, ~600–800 ms on `main + pushdown`, ~400–500 ms on
the change. The change captures *about half* of the regression —
better than `main + pushdown`, but the warm-up cost is still
visible.

---

## 5. Where this falls short

The change doesn't try to fix everything `pushdown_filters` could be
slow for. Two distinct gaps remain.

### 5.1 No sub-row-group adaptation

The scheduler's swap point is a row-group boundary. So:

- **Single-row-group files** (TPC-H tables at SF1 typical) have no
  swap point inside the file. The tracker still picks an initial
  placement and accumulates stats *for subsequent files*, but
  within a single file the filter is whatever it started as. If the
  initial heuristic lands the wrong call, you eat the whole file
  with the wrong placement.
- **Within-row-group selectivity skew** can't be acted on either —
  the decoder can't be reconfigured mid-row-group. This would
  require an arrow-rs `ParquetRecordBatchReader::pause` returning a
  residual `RowSelection`. Out of scope here.

TPC-H SF1 happens to land well anyway (§4.3) because the tracker's
runtime samples quickly say "no payoff", and the demote-to-post-scan
path is itself a valid plan. But the residual S3 fast-query losses
in ClickBench (§4.4) are exactly the shape that sub-row-group
adaptation would close.

### 5.2 Cross-partition row-group balance is a different problem

Even when each filter's placement is right, partition skew can
dominate. The change doesn't redistribute files/row groups across
partitions. The fix for that is morselized scans
([#21766](https://github.com/apache/datafusion/pull/21766)):

- today: morsel = file. A partition that drew an awkward file
  carries that load alone.
- near-term: morsel = row group. A row-group-level work unit
  travels across partitions, so one heavy file can be split across
  multiple streams.
- future: morsel = sub-chunk inside a row group.

That work is independent of adaptive filter scheduling.

---

## 6. Benchmark methodology

Three workloads (TPC-DS SF1, TPC-H SF1, ClickBench partitioned at
100 files of `hits_partitioned`); two latency modes (local NVMe at
5 iterations; `--simulate-latency` at 3 iterations because per-query
wall time is ~25× higher). Three configurations: `main +
pushdown=off`, `main + pushdown=on`, change with `pushdown=on`.
Same machine state across configurations, sequential runs.

```bash
# both binaries built from clean target dirs
BIN_MAIN=/path/to/main/target/release/dfbench
BIN_CHANGE=/path/to/change/target/release/dfbench

# ClickBench partitioned
$BIN clickbench [--pushdown] --iterations 5 \
  --path benchmarks/data/hits_partitioned \
  --queries-path benchmarks/queries/clickbench/queries \
  -o results/<config>/clickbench_partitioned.json

# TPC-DS SF1 (parquet)
$BIN tpcds [--pushdown] --iterations 5 \
  --path benchmarks/data/tpcds_sf1 \
  -o results/<config>/tpcds_sf1.json

# TPC-H SF1 (parquet)
$BIN tpch [--pushdown] --iterations 5 \
  --path benchmarks/data/tpch_sf1 \
  -o results/<config>/tpch_sf1.json

# add --simulate-latency for the S3-profile runs
```

Reported numbers use the median across iterations.

---

## 7. What's missing / open questions

### 7.1 Sub-row-group adaptation (arrow-rs surface)

The smallest unit at which the tracker can change placement is one
row group. For ClickBench partitioned (~10–30 row groups per file)
that's plenty; for TPC-H SF1 (one row group per file) it's nothing —
the first placement decision is the only decision. The current
result is fine on TPC-H because the runtime samples cause a quick
demote-to-post-scan that itself yields a good plan; but the residual
S3 fast-query losses in ClickBench (Q6, Q37, Q38, Q42) are the
shape where mid-row-group adaptation would help.

**Why mid-row-group is hard in arrow-rs.** A
`ParquetRecordBatchReader` holds three pieces of mutable state
inside one row group: the `Box<dyn ArrayReader>` column readers
(each positioned at some page + offset, possibly with a dictionary
loaded), the `ReadPlan { row_selection_cursor }` (pauseable
already), and the `RowFilter` wired into the pre-decode column
subset. Swapping the filter changes which columns need to be
pre-decoded, which changes the array reader topology — the array
readers built for "pre-decode only filter cols, then late-
materialise survivors" are structurally different from "decode the
full projection in one shot".

**Realistic API shape.**

```rust
impl ParquetPushDecoder {
    /// True at any record-batch boundary inside a row group.
    pub fn can_pause(&self) -> bool;

    /// Stop emitting from the current row group, return residual state.
    pub fn pause(&mut self) -> Result<PausedRowGroup>;

    /// Resume the same row group with a different filter / projection.
    /// Re-builds array readers; PushBuffers reused.
    pub fn resume_with(
        &mut self,
        paused: PausedRowGroup,
        new_strategy: StrategySwap,
    ) -> Result<()>;
}
```

Resume cost is dominated by re-decoding compressed pages from the
start of whichever pages contain the resume row. Compressed pages
are typically 1 MiB; on M-series hardware that's ~1–5 ms per page
per column.

### 7.2 Row-group morselization

Independent of this proposal. Today morselization moves *files*
across partitions but not row groups
([#21351](https://github.com/apache/datafusion/pull/21351) merged,
[#21766](https://github.com/apache/datafusion/pull/21766) draft).
Combined with the change in this proposal, row-group morselization
closes the partition-skew side of the lottery for cases the
in-decoder scheduler can't address.

### 7.3 Three new config knobs not yet plumbed through proto-common

The new fields on `ParquetOptions` — `prior_promote_threshold`
(0.5), `prior_demote_threshold` (0.05), `latency_z_baseline_ms`
(5.0), `latency_z_max_scale` (8.0) — aren't yet in the proto
schema. `from_proto` defaults to safe values so round-trip
preserves behaviour, but a follow-up should plumb them through.

### 7.4 Bayesian prior across files in the same scan

The byte-ratio fallback runs when no per-conjunct rate is
available. Some ClickBench files have no page index for the filter
column, so byte-ratio is the prior in practice for some queries.
Open question: feed the tracker's accumulated runtime stats back
as a Bayesian prior on `eff_mean` to short-circuit warm-up on
subsequent files within the same scan. Estimated impact: small;
not in scope.

---

## 8. Speaker-notes summary (10-min talk shape)

1. **Hook (1 m)** — `pushdown_filters` is a per-query lottery on `main`. Show one query that wins big, one that loses big.
2. **Why (2 m)** — I/O patterns + per-batch eval cost diagrams (§2.2).
3. **The lopsided-partition slide (1 m)** — the one in §2.3.
4. **Solution (2 m)** — state machine diagram + the swap-at-RG-boundary diagram (§3.1, §3.5).
5. **Per-conjunct pruning rates (1 m)** — the one idea that closed the latency gap (§3.2).
6. **Wins (2 m)** — ClickBench aggregate + Q23 (LIKE) + Q64 (dynamic filters).
7. **Where it falls short (1 m)** — sub-row-group, morselization (§5, §7).
8. **Close (1 m)** — "never slower than `pushdown=off`" success criterion, status, what's next.

---

## 9. References

### Filter-pushdown-on-by-default work (apache/datafusion)

- [#3463 — Enable parquet filter pushdown by default](https://github.com/apache/datafusion/issues/3463) — the long-running tracking issue this proposal is ultimately in service of.
- [#20324 — \[EPIC\] Fix performance regressions when enabling parquet filter pushdown (late materialization)](https://github.com/apache/datafusion/issues/20324) — epic enumerating the regressions adaptive scheduling neutralises.
- [#20325 — ClickBench Q10 slows down when filter pushdown is enabled](https://github.com/apache/datafusion/issues/20325) — concrete example of the "Problem A" lottery.

### Morselized scans (the "Problem B" / data-skew fix path)

- [#20529 — Introduce morsel-driven Parquet scan](https://github.com/apache/datafusion/issues/20529) — the original tracking issue for morselization.
- [#21351 — Dynamic work scheduling in `FileStream`](https://github.com/apache/datafusion/pull/21351) — **merged**: file-level morselization (siblings steal whole files from a shared queue). Closes the "files are units of work" half of #20529 but explicitly defers row-group splitting.
- [#21766 — feat(parquet): row-group morselization for sibling FileStream stealing](https://github.com/apache/datafusion/pull/21766) — **draft**: row-group-level morselization, the complementary fix for the partition-skew case.

### Object-store latency simulation

- [#20954 — `--simulate-latency` flag for `dfbench`](https://github.com/apache/datafusion/pull/20954) — what the "S3" rows in §4.4 are using.

### Other related apache/datafusion threads

- [#19654 — Poor performance of parquet pushdown with offset](https://github.com/apache/datafusion/issues/19654)
- [#21317 — Sort pushdown: reorder row groups by statistics within each file](https://github.com/apache/datafusion/issues/21317)
- [#20443 — Support filter pushdown through `SortMergeJoinExec`](https://github.com/apache/datafusion/issues/20443)
- [#21145 — Add example implementing filter pushdown](https://github.com/apache/datafusion/issues/21145)
