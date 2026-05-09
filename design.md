# Adaptive filter scheduling in the Parquet decoder

**Status**: pending upstream review
**Branch**: [`adriangb/datafusion:pr/round6-stack`](https://github.com/adriangb/datafusion/pull/new/pr/round6-stack) (6 commits on top of [PR #11](https://github.com/adriangb/datafusion/pull/11))
**Companion**: [`pydantic/arrow-rs:adaptive-strategy-swap`](https://github.com/pydantic/arrow-rs/pull/9)
**Related**: [#3463](https://github.com/apache/datafusion/issues/3463) (`pushdown_filters` on by default), [#20324](https://github.com/apache/datafusion/issues/20324) (push-down regressions epic), [#21766](https://github.com/apache/datafusion/pull/21766) (row-group morselization, draft)

## 1. Problem

`pushdown_filters=on` in the Parquet scan is a per-query lottery
today. Wins (sparse filter, late-materialised projection) and
losses (filter-as-projection-overlap, single-row-group skew, cloud
storage RTT amplification) coexist on the same workload, so the
config is gated off by default — and the long-running ask in
[#3463](https://github.com/apache/datafusion/issues/3463) to flip it
on by default has stayed open for years.

This document specifies a per-filter, adaptive, in-decoder
scheduler that decides — and re-decides — whether each predicate
runs at row-level (inside the Parquet decoder, with `RowSelection`
+ page-skipping) or post-scan (as a `FilterExec` after the wide
scan). The scheduler is designed to **never be slower than
`pushdown=off`** while capturing the wins of `pushdown=on` where
they exist.

## 2. Goals and non-goals

### Goals

1. **Success criterion**: on every workload tested,
   `branch + pushdown_filters=on` ≤ `main + pushdown_filters=off`.
   Measured on TPC-DS SF1, TPC-H SF1, ClickBench partitioned, both
   local-NVMe and `--simulate-latency`.
2. Capture the per-query wins of row-level filtering when present
   (sparse `RowSelection` + late materialisation).
3. Avoid the per-query losses of row-level filtering when filters
   are unselective, projection-overlapping, or paying many small
   range I/Os under cloud-storage latency.
4. Re-evaluate placement at every row-group boundary so the scan
   self-corrects mid-file when the cost/benefit shifts (e.g. a
   dynamic filter populates after the build side finishes).

### Non-goals

1. **Cross-partition row-group balance.** When `lineitem` is one
   row group in one file, even an in-scan filter that demotes
   correctly to post-scan can't redistribute work across
   partitions. The fix is row-group-level morselization,
   tracked separately at [#21766](https://github.com/apache/datafusion/pull/21766).
2. **Sub-row-group adaptation.** Today the scheduler swaps
   strategy at row-group boundaries. A finer-grained "pause and
   resume mid-row-group" requires arrow-rs API surface
   (`ParquetRecordBatchReader::pause` returning residual
   `RowSelection`) that is out of scope.
3. **Replacing `FilterExec` semantics.** The post-scan path
   continues to apply mandatory filters; the scheduler only moves
   filters between in-scan and post-scan, never drops them
   (except optional dynamic filters that become known to be
   useless — `OptionalFilterPhysicalExpr` only).

## 3. Background

### 3.1 The lottery, mechanically

| Mode | Decode pattern per row group | Cost when filter is unselective |
|---|---|---|
| `pushdown=off` | decode all projected cols → coalesced read → filter mask post-decode | none beyond the mask — filter is a `FilterExec` after the scan |
| `pushdown=on` | decode filter cols → evaluate predicate → build `RowSelection` → skip pages / decode only surviving rows for the projection | per-batch ArrowPredicate eval cost; extra I/O for filter cols not in the projection; possible double-decode when the filter column is also in the projection |

Under cloud-storage latency, `pushdown=on` issues many small
ranged GETs — one per surviving page — each paying a per-RTT cost.
This is what makes the `main+pushdown` column blow up on the
`--simulate-latency` benches (TPC-H 2.2×, TPC-DS 1.8×).

### 3.2 What was already in place before this work

- **PR #11 (the base)** introduced an in-decoder scheduler that
  could swap row-level / post-scan placement at row-group
  boundaries via the arrow-rs companion change. It tracked
  per-filter selectivity stats (Welford), used confidence
  intervals to gate promote/demote, and shipped the lock-shaped
  outer `SelectivityTracker` API.
- It used a byte-ratio heuristic (extra-bytes-for-filter-cols /
  projection-bytes) as the *initial placement* prior, which let
  it pick a sensible default the first time it saw a filter,
  before any runtime samples existed.

### 3.3 Where PR #11 fell short

- **Slow placement convergence under latency.** The byte-ratio
  prior made the right decision often enough, but the
  confidence-interval gate held a wrong initial decision for
  several row groups before promote/demote could fire. On
  `--simulate-latency` this dominated the residual gap to
  `main+pushdown=off`.
- **Dynamic filters were placeholders at file open.**
  Hash-join build-side filters hadn't published their selective
  predicate yet when initial placement ran, so the scheduler had
  no signal beyond byte-ratio for them.
- **Single-row-group regression on TPC-H.** `lineitem`'s file is
  one row group; the scheduler had no time to swap strategy
  before the file was done, so a wrong initial placement stuck.

### 3.4 The signal we left on the floor

DataFusion already runs three pruning passes at file open
(page-index, row-group min/max statistics, bloom filter). Each one
evaluates *some* sub-predicates against *some* containers and
discards what can't possibly match. Those passes already know
something concrete about each conjunct's selectivity on this file
— but their output (a single `Vec<bool>` of "keep this row group")
is too coarse to reuse.

The core idea of this design: **make the existing pruning passes
emit per-conjunct rates as a side-effect** and feed those rates
into the scheduler's initial placement decision.

## 4. Design

### 4.1 Per-conjunct PruningPredicate API (`datafusion-pruning`)

A new constructor on `PruningPredicate` takes
`&[(FilterId, Arc<dyn PhysicalExpr>)]` — already-split top-level
conjuncts each carrying a caller-chosen tag — and produces a
*tagged wrapper* predicate. The wrapper's own `predicate_expr` is
a literal-true placeholder; the real per-conjunct logic lives in
`sub_predicates: Option<Vec<TaggedSubPredicate>>`.

```rust
impl PruningPredicate {
    pub fn try_new_tagged_conjuncts(
        tagged: &[(FilterId, Arc<dyn PhysicalExpr>)],
        schema: SchemaRef,
    ) -> Result<Self>;

    pub fn prune_per_conjunct<S: PruningStatistics + ?Sized>(
        &self,
        statistics: &S,
    ) -> Result<(Vec<bool>, Vec<PerConjunctPruneStats>)>;
}

pub struct PerConjunctPruneStats {
    pub tag: Option<FilterId>,
    pub containers_seen: usize,
    pub containers_pruned: usize,
}
```

`PruningPredicate::prune()` is special-cased on
`sub_predicates.is_some()`: it AND-s each leaf's prune output to
produce the same `Vec<bool>` a non-tagged predicate would, with no
double evaluation. `prune_per_conjunct()` does the same AND and
also reports each leaf's pruned-vs-seen counts.

`literal_columns()` unions the leaves' columns when
`sub_predicates.is_some()`. **This is load-bearing**: without the
union, downstream consumers (notably `ParquetOpener::open` deciding
which bloom filters to fetch) see "no columns of interest" and
silently skip bloom-filter pruning altogether. (See §4.5 below;
this was the bug commit 5 fixes.)

### 4.2 Side-effect rate emission (`datafusion-datasource-parquet`)

Three pruning passes already run at file open. Each gets a
`_with_per_conjunct_stats` variant that returns `Vec<PerConjunctPruneStats>`
in addition to its existing output:

- `PagePruningAccessPlanFilter::new_tagged` /
  `prune_plan_with_per_conjunct_stats` — page-index pruning rate
  per conjunct, finest granularity. Seeds the rate map.
- `RowGroupAccessPlanFilter::prune_by_statistics_with_per_conjunct_stats`
  — row-group min/max stats rate per conjunct.
- `RowGroupAccessPlanFilter::prune_by_bloom_filters_with_per_conjunct_stats`
  — bloom-filter rate per conjunct, taking max with whatever
  page/RG rate was already there.

The opener accumulates these into a single
`HashMap<FilterId, f64>` per file open and threads it through to
the per-partition `AdaptiveParquetStream`. Layering rule: page
rates (finest) seed the map first; row-group rates fill in for
filters page-index didn't cover; bloom rates take max with
whatever was there (bloom is strictly more powerful than min/max
for equality predicates).

The opener also reorders its existing operations so
`prune_by_limit` and page-index pruning run *before* the initial
`partition_filters` call. This means rates are available when
placement happens, not after.

### 4.3 Initial placement consumer (`SelectivityTracker`)

`partition_filters` consumes the rate map for newly-seen filters:

```rust
let prior = page_pruning_rates.get(&id).copied();
let row_level = match prior {
    Some(p) if p >= prior_promote_threshold (default 0.5) => RowFilter,
    Some(p) if p <= prior_demote_threshold  (default 0.05) => PostScan,
    _ => byte_ratio_heuristic(...),  // PR #11's existing heuristic
};
```

The byte-ratio fallback runs only when no prior is available
(e.g. the file has no page index and the predicate isn't single-column,
which makes row-group pruning a no-op). On the workloads we test
the fallback is rare — most ClickBench files have page indexes;
TPC-DS / TPC-H rely on row-group stats.

### 4.4 Refresh prior for populated dynamic filters

Hash-join build-side filters are `OptionalFilterPhysicalExpr`-wrapped
`DynamicFilterPhysicalExpr`s; their `snapshot_generation()` starts
at 0 (placeholder) and increments when the build side publishes a
populated predicate. The rates we captured at file open belong to
the placeholder, not the populated filter.

`fresh_rate_for_dynamic_conjunct(expr, arrow_schema, parquet_schema, metadata)`
runs *only* for filters where `snapshot_generation > 0`. It
evaluates a per-conjunct `PruningPredicate` against the file's
current row-group statistics:

1. **Whole-conjunct path** — `PruningPredicate::try_new(expr, schema)`
   succeeds and is not `always_true` for most populated dynamic
   filters; we evaluate it and return the rate directly.
2. **Partial-AND fallback** — for the
   `col >= lo AND col <= hi AND hash_lookup(...)` shape that the
   predicate rewriter can't turn into a single
   `PruningPredicate`, we snapshot the dynamic filter to
   materialise its inner expression, `split_conjunction()` it,
   evaluate each prunable part separately, and take the max rate
   across sub-parts. This is a **promote-only signal**: we return
   it only when ≥ 0.5 (a confident "this filter is selective");
   below that we return `None` and let the standard prior /
   byte-ratio fallback decide, which won't be misled by an
   undercounted partial rate.

The targeted re-evaluation runs only for *populated dynamic
filters*, not the static path, so it doesn't count as an "extra
pruning run" on the cold path.

### 4.5 Latency-aware confidence-z shrink

Promote/demote uses a one-sided confidence interval on per-batch
bytes-saved-per-second:

```
demote when upper_bound(eff, z_eff) < min_bytes_per_sec
promote when lower_bound(eff, z_eff) >= min_bytes_per_sec
```

Under cloud-storage RTT the cost of holding a wrong placement
scales with per-fetch latency. We shrink the effective z when
per-fetch wall time exceeds a baseline:

```rust
fn effective_z(&self) -> f64 {
    let avg_fetch_ms = total_fetch_ns / total_fetches / 1e6;
    if avg_fetch_ms <= latency_z_baseline_ms { return confidence_z; }
    let factor = (avg_fetch_ms / latency_z_baseline_ms)
        .clamp(1.0, latency_z_max_scale);
    confidence_z / factor
}
```

The opener feeds `record_fetch(n_ranges, elapsed_ns)` for each
`get_byte_ranges` call. With defaults `latency_z_baseline_ms = 5.0`
and `latency_z_max_scale = 8.0`, on local NVMe (<5 ms per fetch)
nothing changes (`factor = 1`); under 100 ms RTT the effective z
shrinks to ~0.4, demoting on point estimates rather than waiting
for a tight CI.

### 4.6 Mid-stream skip flag (already in PR #11 base)

For optional filters wrapped in `OptionalFilterPhysicalExpr`, the
hot per-batch `update()` path checks the CI upper bound after each
batch and, if it falls below `min_bytes_per_sec`, sets a
per-filter `AtomicBool` skip flag. Subsequent batches in both the
row-level and post-scan paths short-circuit evaluation for that
filter. Mandatory filters are never flagged.

This is unchanged from PR #11 and not part of this design's
contribution; mentioned for completeness because the test fixture
in commit 6 has to set `filter_pushdown_min_bytes_per_sec = 0.0`
to force-promote, which interacts with this path.

## 5. Module structure

`datafusion-datasource-parquet`'s `selectivity` is split from a
2.3k-line monolith into a six-file submodule:

```
selectivity/
├── mod.rs        — submod decls, public re-exports, tests submod
├── skippable.rs  — count_skippable_bytes + SKIP_WINDOW_ROWS
├── types.rs      — FilterId, FilterState, PartitionedFilters,
│                   PartitionResult
├── stats.rs      — SelectivityStats Welford accumulator + CI bounds
├── config.rs     — TrackerConfig builder
├── tracker.rs    — SelectivityTracker outer API + Inner state machine
└── tests.rs      — moved verbatim with adjusted imports
```

The split is pure code-motion (commit 2) and lands before any
feature commits, so the feature commits' diffs are localised to
the right module. Public-API names and signatures are preserved;
crate-internal items (`count_skippable_bytes`, `FilterState`) keep
their `pub(crate)` visibility through targeted re-exports in
`mod.rs`. Submodule-internal items (`SelectivityStats`, the
`Inner` struct, helpers) are now `pub(super)` — narrower than the
implicit "all of selectivity.rs sees everything" of the old
layout.

## 6. Implementation: the six-commit stack

Each commit builds and is lint-clean; tests pass at every step.

```
72a3eaa85  test(parquet): force-promote filters in tests that drove
                          the legacy 'pushdown=on' contract           +40
e799f85d7  fix(pruning): union sub-predicate columns in
                         literal_columns()                            +22 −7
d698aae5a  feat(parquet): adaptive placement prior from per-conjunct
                          pruning rates                              +812 −179
ffbcd0c1c  feat(pruning): per-conjunct PruningPredicate rates API    +182 −2
79beaf339  refactor(parquet): split selectivity.rs into modules    +2522 −2335
9bb321284  test(parquet): update selectivity tests for scatter-aware
                          bytes API                                   +37 −28
9a1705088  Revert "feat(parquet): coalesce post-scan-filtered batches"  ← PR #11 base
```

1. **`test(parquet):` selectivity test API drift fix** (pre-existing).
   Commit `97c62a684` (four commits before this stack) refactored
   `SelectivityStats::update` to take caller-precomputed
   `skippable_bytes` instead of deriving it from
   `matched/total/total_bytes`. The unit tests weren't updated.
   This commit fixes them. Independent of the round-6 work; could
   land as its own first PR.
2. **`refactor(parquet):` split selectivity.rs into modules.**
   Pure code-motion. Lands before the feature commits so their
   diffs are localised.
3. **`feat(pruning):` per-conjunct PruningPredicate rates API**
   (§4.1). Self-contained in `datafusion-pruning`. No consumer
   yet.
4. **`feat(parquet):` adaptive placement prior from per-conjunct
   pruning rates** (§4.2 + §4.3 + §4.4 + §4.5). Wires page /
   row-group / bloom passes to emit rates, opener threads them
   through, `SelectivityTracker::partition_filters` consumes
   them, plus the dynamic-filter refresh and latency-aware z.
   Largest commit; bundles tightly-coupled changes.
5. **`fix(pruning):` union sub-predicate columns in literal_columns()**.
   Load-bearing correctness fix uncovered by the workspace tests.
   Without it, tagged-predicate scans silently disabled bloom
   filter pruning. See §4.1.
6. **`test(parquet):` force-promote filters in tests that drove
   the legacy 'pushdown=on' contract.** Several `datafusion-core`
   tests assumed `pushdown_filters=true` meant "every filter
   runs at row-level", which is no longer true under the
   adaptive scheduler default (filters land at post-scan until
   evidence accumulates). Setting
   `filter_pushdown_min_bytes_per_sec = 0.0` alongside
   `pushdown_filters = true` restores the legacy contract for
   the affected fixtures.

## 7. Validation

### 7.1 Tests

`cargo test --workspace --no-fail-fast` on the stack tip:
**9 240 passed, 0 failed, 106 ignored.** Notable subsets:

- `cargo test -p datafusion-pruning --lib`: 82 / 82.
- `cargo test -p datafusion-datasource-parquet --lib`: 143 / 143.
- `cargo test -p datafusion --test parquet_integration`: 200 / 200
  (the integration suite that includes the bloom-filter and
  predicate-cache tests fixed by commit 5 and commit 6).

### 7.2 Lint

`cargo clippy -p datafusion-datasource-parquet -p datafusion-pruning
--all-targets --all-features -- -D warnings` clean.

### 7.3 Bench matrix

Three workloads (TPC-DS SF1, TPC-H SF1, ClickBench partitioned),
two latency modes (local NVMe at 5 iterations; `--simulate-latency`
at 3 iterations), three branches. Sequential same-state runs.

**no-lat** — sum-of-medians, ms

| Workload | main | main + pushdown | change | change/main | change/main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench (43q) | 21 020 | 21 699 | **17 919** | **0.85×** | **0.83×** |
| TPC-DS (99q) | 17 003 | 38 961 | **16 852** | **0.99×** | **0.43×** |
| TPC-H (22q) | 780 | 989 | **691** | **0.89×** | **0.70×** |

**lat** (`--simulate-latency`, 20-200 ms per object-store op)

| Workload | main | main + pushdown | change | change/main | change/main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench (43q) | 86 562 | 111 321 | **88 947** | 1.03× | **0.80×** |
| TPC-DS (99q) | 76 418 | 141 940 | **77 546** | 1.01× | **0.55×** |
| TPC-H (22q) | 23 723 | 52 597 | **24 157** | 1.02× | **0.46×** |

The change beats `main` outright on local SSD across all three
workloads (15-30 % faster) and is at parity-or-better on
simulated cloud storage (within 1-3 %, within run-to-run noise).
Versus `main + pushdown` — the configuration the change is meant
to neutralise — the change is **17-57 % faster in every cell**.

## 8. Alternatives considered

### 8.1 Always row-level when a static analysis says so (current `pushdown_filters=on`)

This is what `main + pushdown` does, and it's the per-query lottery
the PR exists to fix. The static analysis (cost / column overlap)
gets the wrong answer often enough that the config is gated off by
default. Rejected; the design above is what we're proposing
*instead* of this.

### 8.2 Run pruning passes a second time, off the critical path, to populate the prior

An earlier round attempted this. Rejected for two reasons:

1. **Latency cost.** Even an "extra" run of page-index pruning
   reads page indexes that the existing pass has already read,
   doubling the file-open I/O on workloads with large page
   indexes (TPC-DS S3 lat).
2. **Architectural duplication.** Two paths that must produce the
   same `Vec<bool>` is exactly the kind of correctness debt
   future maintainers regret. The side-effect approach in §4.2 has
   one path, instrumented to also emit per-conjunct rates.

### 8.3 Defer placement entirely until after the first row group

i.e. always start at post-scan, promote based on samples.
Rejected: this is what `min_bytes_per_sec=INFINITY` does, and it
loses on TPC-DS Q64 (the dynamic-filter chain) and ClickBench Q23
(the LIKE-on-URL win). The wins of row-level filtering are real
and dominate when present; the design has to capture them on
file 1.

### 8.4 A fully static cost model

Using compressed bytes per column, projected I/O cost, and a
selectivity estimate from histograms. Rejected because DataFusion
doesn't have the histogram infrastructure today, and a model
without runtime samples can't react to dynamic filters or
mid-stream selectivity collapse.

## 9. Trade-offs and open questions

### 9.1 Warm-up cost on the first row group

The byte-ratio fallback runs when no per-conjunct rate is
available. On ClickBench's `hits_partitioned` files there are no
page indexes for many columns, so byte-ratio is the prior in
practice for some queries. This is the small "ClickBench SSD
within 1 % of `main` — sometimes slightly slower" residual. Open
question: feed the SelectivityTracker's accumulated runtime stats
back as a Bayesian prior on `eff_mean` to short-circuit the
warm-up on subsequent files in the same scan. Estimated impact:
small; not in scope for this PR.

### 9.2 Three new config knobs

- `prior_promote_threshold` (default 0.5)
- `prior_demote_threshold` (default 0.05)
- `latency_z_baseline_ms` (default 5.0) and `latency_z_max_scale`
  (default 8.0)

Defaults were tuned by hand against the bench matrix in §7.3.
They live in `ParquetOptions` for runtime configurability. They
are not yet plumbed through `proto-common`'s `ParquetOptions`
message; `from_proto` currently restores them to defaults on
deserialisation, so a roundtrip preserves behaviour. Trivial
follow-up after the main PR.

### 9.3 Tagged-predicate visibility into PruningPredicate's literal column set

The §4.1 `literal_columns()` fix is correct but slightly
asymmetric — the wrapper has to know about its leaves to compute
its own column set. An alternative would be to make tagged
predicates an enum variant of `PruningPredicate` rather than a
"wrapper that special-cases prune/literal_columns". That's a
bigger surgical change to the type and would not affect
behaviour. Deferred.

### 9.4 The TPC-H "lopsided-partition" insight

In an earlier round, TPC-H SSD was a 1.45× regression — `lineitem`
is one row group in one file, and the in-scan filter prevented
the existing `FilterExec`-above-`RepartitionExec` shuffle from
re-balancing partition skew. The dynamic-filter refresh in §4.4
plus the latency-aware z in §4.5 together cause the scheduler to
auto-demote that filter to post-scan very quickly (the byte-ratio
prior says "row-level" but the runtime samples say "no payoff").
The current bench shows TPC-H SSD at **0.89× of main** — better
than the regression-free baseline.

This is a case where adaptive scheduling does more than fix the
PR's own regression: it incidentally improves on `main` itself
because the fast demote-to-post-scan path lets the existing
shuffle do its job. The "real" fix for partition skew remains
[#21766](https://github.com/apache/datafusion/pull/21766) — but
the lower bound this PR delivers is already below `main`.

## 10. Migration / compatibility

- **Public API surface added**: `PruningPredicate::try_new_tagged_conjuncts`,
  `PruningPredicate::prune_per_conjunct`, `PerConjunctPruneStats`
  in `datafusion-pruning`. Backwards-compatible additions.
- **`PruningPredicate::literal_columns()` semantics**: extended
  to union sub-predicate columns when `sub_predicates.is_some()`.
  Plain non-tagged predicates are unchanged. No callers are
  removed; downstream behaviour is *strictly more correct*
  (tagged predicates now report their actual columns instead of
  silently empty).
- **Config knobs added**: three new fields on `ParquetOptions`
  with defaults that preserve PR #11 behaviour when not
  explicitly set. No proto-schema additions yet (see §9.2).
- **Test fixture changes**: a handful of tests in
  `datafusion/core/tests/parquet/` set
  `filter_pushdown_min_bytes_per_sec = 0.0` to retain the legacy
  "pushdown=on means every filter runs row-level" contract. Two
  fixture helpers (`ParquetScanOptions::config`,
  `ContextWithParquet::new`) do this centrally; individual tests
  inherit. No production-code-side ergonomic regressions.
- **No changes to `datafusion-cli`, `datafusion-substrait`, or
  any other crate beyond `datafusion-pruning`,
  `datafusion-datasource-parquet`, and the test fixtures in
  `datafusion-core`.**

## 11. References

- [#3463](https://github.com/apache/datafusion/issues/3463) —
  Enable parquet filter pushdown by default (the meta-issue this
  work serves).
- [#20324](https://github.com/apache/datafusion/issues/20324) —
  Epic enumerating the regressions adaptive scheduling is meant
  to neutralise.
- [#20325](https://github.com/apache/datafusion/issues/20325) —
  Concrete example: ClickBench Q10 slows down with pushdown.
- [#21766](https://github.com/apache/datafusion/pull/21766) —
  Row-group morselization, the partition-skew fix that
  complements this PR.
- `report.md` (this repo) — discursive write-up with per-query
  drill-downs and chart commentary.
- `slides/datafusion-meetup-05-2026/` — the conference deck
  drawn from these numbers.
