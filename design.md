# Adaptive filter scheduling in the Parquet decoder

**Status**: design proposal
**Targets**: `datafusion-pruning`, `datafusion-datasource-parquet`
**Related**: [#3463](https://github.com/apache/datafusion/issues/3463) (`pushdown_filters` on by default), [#20324](https://github.com/apache/datafusion/issues/20324) (push-down regression epic), [#20325](https://github.com/apache/datafusion/issues/20325) (ClickBench Q10 regression), [#21766](https://github.com/apache/datafusion/pull/21766) (row-group morselization, draft)

## 1. Problem

DataFusion's Parquet scan has two ways to apply a filter:

- **Post-scan** (`pushdown_filters = off`, today's default): decode every projected column for the whole row group, then evaluate the filter as a `FilterExec` above the scan.
- **Row-level** (`pushdown_filters = on`): decode the filter columns first, evaluate the predicate, build a `RowSelection`, then decode only the surviving rows of the projection — with page-skipping where the `RowSelection` is sparse.

Today the choice is per-scan and static. Row-level filtering is a big win when the filter is selective and its columns aren't fully covered by the projection — sparse `RowSelection` lets the decoder skip whole pages, which is the canonical "late materialization" win. It is a measurable loss when the filter is unselective, when the filter column overlaps the projection (you pay decode twice), or when many small page-ranged GETs amplify per-fetch latency on object storage.

The wins and losses coexist on the same workload, so the config is gated off by default. The long-running ask in [#3463](https://github.com/apache/datafusion/issues/3463) to flip it on by default has stayed open for years because every static placement rule we have evaluated still loses on some realistic query.

This document specifies a per-filter, adaptive, in-decoder scheduler that decides — and re-decides — whether each predicate runs at row-level or post-scan, on a row-group-by-row-group basis. The success bar is that with the change applied, `pushdown_filters = on` is **never slower than today's `pushdown_filters = off`** on the workloads we test, while still capturing the wins of row-level filtering when they exist.

## 2. Goals and non-goals

### Goals

1. **Success criterion**: across TPC-DS SF1, TPC-H SF1, and ClickBench-partitioned, on both local NVMe and a `--simulate-latency` profile that mimics object storage, `pushdown_filters = on` with this change ≤ `pushdown_filters = off` on `main` today.
2. Capture per-query wins of row-level filtering where present (sparse `RowSelection` + page-skipping + late materialisation).
3. Avoid per-query losses when filters are unselective, projection-overlapping, or paying many small range I/Os under cloud-storage latency.
4. Re-evaluate placement at every row-group boundary so the scan self-corrects mid-file when the runtime cost/benefit shifts.

### Non-goals

1. **Cross-partition row-group balance.** When `lineitem` is one row group in one file, moving a filter between in-scan and post-scan does not redistribute work across partitions. The fix is row-group-level morselization, tracked separately at [#21766](https://github.com/apache/datafusion/pull/21766).
2. **Sub-row-group adaptation.** The scheduler swaps strategy at row-group boundaries. A finer-grained "pause and resume mid-row-group" requires arrow-rs API surface (`ParquetRecordBatchReader::pause` returning the residual `RowSelection`) that is out of scope.
3. **Replacing `FilterExec` semantics.** Filters are never dropped; the scheduler only moves them between in-scan and post-scan. The single exception is `OptionalFilterPhysicalExpr` (today's hash-join dynamic-filter wrapper), which is *defined* to be droppable when its cost outweighs its selectivity.

## 3. Background

### 3.1 The lottery, mechanically

| Mode | Decode pattern per row group | Cost when the filter is unselective |
|---|---|---|
| `pushdown=off` | Decode all projected columns → coalesced ranged read → apply filter mask above the scan | None beyond the post-scan mask |
| `pushdown=on` | Decode filter columns → evaluate predicate → build `RowSelection` → skip pages / decode only surviving rows of the projection | ArrowPredicate eval cost per batch; extra I/O for filter columns not in the projection; possible double-decode of filter columns also in the projection |

Under object-storage latency, `pushdown=on` issues many small ranged GETs — typically one per surviving page — each paying a per-request RTT. This is what makes `pushdown=on` blow up on the `--simulate-latency` runs in §7 (TPC-H 2.2×, TPC-DS 1.8×).

### 3.2 The signal we already compute but don't reuse

`ParquetOpener` already runs three filter-aware pruning passes at file open:

- **Page-index pruning** — `PagePruningAccessPlanFilter::prune_plan_with_predicate`, finest container granularity.
- **Row-group min/max pruning** — `RowGroupAccessPlanFilter::prune_by_statistics`.
- **Bloom-filter pruning** — `RowGroupAccessPlanFilter::prune_by_bloom_filters`.

Each evaluates a *combined* predicate against a set of containers and emits a single `Vec<bool>` ("keep this container"). That collapsed output is enough to skip data, but it loses the per-conjunct information that would let a downstream consumer say "filter A pruned 90 % of pages; filter B pruned nothing." Those rates are exactly the signal needed to choose row-level vs. post-scan for each filter individually — and they are computed *before* the scan begins, so they're available for an initial placement decision.

### 3.3 Runtime selectivity and the per-file-run problem

Even with a per-conjunct prior from the pruning passes, the scheduler needs a runtime signal to correct itself once batches start flowing:

- Hash-join build-side filters (`DynamicFilterPhysicalExpr` wrapped in `OptionalFilterPhysicalExpr`) are populated by the time the probe-side `ParquetScan` opens a file — `HashJoinExec` blocks the probe side on `collect_build_side` until the build is fully done. `PruningPredicate::try_new` calls `snapshot_physical_expr_opt` unconditionally, and both wrapper types implement `snapshot()` to unwrap themselves, so the static path captures a per-conjunct rate correctly for the common `col >= lo AND col <= hi` shape. What the static path *can't* crack is the `col >= lo AND col <= hi AND hash_lookup(...)` shape — `build_predicate_expression` flattens the whole AND toward always-true because `hash_lookup` is unhandled. The dynamic-filter refresh in §4.4 fills that gap.
- A predicate can be statically "looks selective" but materially unselective on this file's actual data distribution.
- Object-storage latency makes a wrong placement much more expensive to hold than on local NVMe.

The scheduler maintains Welford running statistics per filter (rows seen, rows matched, eval ns, bytes seen) and gates promote/demote decisions on a one-sided confidence interval over a per-batch *scatter-aware bytes-saved-per-second* metric. The metric counts only sub-batch windows where the filter actually empties a page-sized run — a sparse-RowSelection sample, not just a row-matching rate — so it scores the page-skipping payoff directly.

## 4. Design

### 4.1 Per-conjunct PruningPredicate API (`datafusion-pruning`)

A new constructor on `PruningPredicate` accepts already-split top-level conjuncts, each carrying a caller-chosen tag:

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

The wrapper's own `predicate_expr` is a literal-true placeholder; per-conjunct logic lives in a new `sub_predicates: Option<Vec<TaggedSubPredicate>>` field. `prune()` is special-cased on `sub_predicates.is_some()` to AND the leaves into the same `Vec<bool>` a non-tagged predicate would have produced — no double evaluation. `prune_per_conjunct()` performs the same AND and additionally reports per-leaf pruned/seen counts.

**`literal_columns()` is load-bearing.** When `sub_predicates.is_some()`, `literal_columns()` must union the leaves' columns. Without that union, downstream consumers (notably `ParquetOpener::open` deciding which bloom filters to fetch) see "no columns of interest" and silently skip bloom-filter pruning altogether. This is the subtlest correctness implication of the API.

### 4.2 Side-effect rate emission (`datafusion-datasource-parquet`)

The three existing pruning passes each gain a `_with_per_conjunct_stats` variant that returns the existing output plus `Vec<PerConjunctPruneStats>`:

- `PagePruningAccessPlanFilter::prune_plan_with_per_conjunct_stats`
- `RowGroupAccessPlanFilter::prune_by_statistics_with_per_conjunct_stats`
- `RowGroupAccessPlanFilter::prune_by_bloom_filters_with_per_conjunct_stats`

`ParquetOpener` accumulates per-pass rates into a single `HashMap<FilterId, f64>` per file open and threads it through to the per-partition adaptive stream. The layering rule:

- Page-index rates (finest granularity) seed the map first.
- Row-group rates fill in for filters not covered by page-index pruning.
- Bloom-filter rates take the max with whatever rate was already present, since bloom is strictly more powerful than min/max for equality predicates.

The opener also reorders existing operations so `prune_by_limit` and page-index pruning run *before* the first call into the scheduler's initial-placement step. Rates are therefore available when placement happens, not after.

### 4.3 Initial placement consumer

For a newly seen filter, initial placement is:

```
let prior = per_conjunct_rates.get(&id).copied();
let row_level = match prior {
    Some(p) if p >= prior_promote_threshold (default 0.5) => RowFilter,
    Some(p) if p <= prior_demote_threshold  (default 0.05) => PostScan,
    _ => byte_ratio_heuristic(...),
};
```

The byte-ratio fallback — extra-bytes-for-filter-cols / projection-bytes — runs only when no prior is available (e.g. the file has no page index *and* the predicate isn't single-column, which makes row-group pruning a no-op). On the workloads tested this fallback is rare: most ClickBench files have page indexes; TPC-DS and TPC-H rely on row-group stats.

### 4.4 Refresh prior for populated dynamic filters

The §4.2 side-effect path handles the common `col >= lo AND col <= hi` bounds shape correctly: `PruningPredicate::try_new` calls `snapshot_physical_expr_opt` unconditionally, both `OptionalFilterPhysicalExpr` and `DynamicFilterPhysicalExpr` implement `snapshot()` to unwrap to the populated inner, and `build_predicate_expression` turns the result into a usable per-row-group predicate.

The shape it *can't* handle is `col >= lo AND col <= hi AND hash_lookup(set, col)` — the hash-lookup branch is unhandled, so `build_predicate_expression` flattens the whole AND toward always-true and `try_new_tagged_conjuncts` skips the conjunct entirely. The per-conjunct rate map is missing an entry for that filter.

`fresh_rate_for_dynamic_conjunct(expr, arrow_schema, parquet_schema, metadata)` runs for filters with `snapshot_generation > 0` to fill that gap. It tries two paths:

1. **Whole-conjunct retry.** `PruningPredicate::try_new(expr, schema)` against the populated expression. This is essentially redundant with what the static path already does — kept as a defensive cheap check before the fallback.
2. **Partial-AND fallback.** `snapshot_physical_expr_opt` materializes the populated inner, `split_conjunction()` descends into the inner AND (which it can't do through a live `DynamicFilterPhysicalExpr`), and each prunable sub-part is evaluated separately. The max pruning rate across sub-parts is returned as a **promote-only signal** — only at rate ≥ 0.5, because a partial AND undercounts the true rate and would mislead a demote.

The refresh runs only for dynamic-filter conjuncts, so the static path's cost is unchanged.

### 4.5 Latency-aware confidence-z shrink

Promote/demote uses a one-sided confidence interval on per-batch bytes-saved-per-second:

```
demote when upper_bound(eff, z_eff) < min_bytes_per_sec
promote when lower_bound(eff, z_eff) >= min_bytes_per_sec
```

Under cloud-storage RTT the cost of holding a wrong placement scales with per-fetch latency. The effective z shrinks when per-fetch wall time exceeds a baseline:

```rust
fn effective_z(&self) -> f64 {
    let avg_fetch_ms = total_fetch_ns / total_fetches / 1e6;
    if avg_fetch_ms <= latency_z_baseline_ms { return confidence_z; }
    let factor = (avg_fetch_ms / latency_z_baseline_ms)
        .clamp(1.0, latency_z_max_scale);
    confidence_z / factor
}
```

`ParquetOpener` feeds `record_fetch(n_ranges, elapsed_ns)` from each `get_byte_ranges` call. With defaults `latency_z_baseline_ms = 5.0` and `latency_z_max_scale = 8.0`, local NVMe (< 5 ms per fetch) is unaffected (`factor = 1`); under 100 ms RTT the effective z shrinks to ~0.4, which lets the scheduler demote on a point estimate instead of waiting for a tight confidence interval.

### 4.6 Mid-stream skip flag for optional filters

For predicates wrapped in `OptionalFilterPhysicalExpr` (today: hash-join dynamic filters), the hot per-batch `update()` path checks the CI upper bound after each batch and, if it falls below `min_bytes_per_sec`, sets a per-filter `AtomicBool` skip flag. Subsequent batches in both the row-level and post-scan paths short-circuit evaluation for that filter. Mandatory filters are never flagged.

## 5. Module structure

`datafusion-datasource-parquet`'s `selectivity` module is laid out as:

```
selectivity/
├── mod.rs        — submodule decls, public re-exports, tests submodule
├── skippable.rs  — count_skippable_bytes + SKIP_WINDOW_ROWS
├── types.rs      — FilterId, FilterState, PartitionedFilters,
│                   PartitionResult
├── stats.rs      — SelectivityStats Welford accumulator + CI bounds
├── config.rs     — TrackerConfig builder
├── tracker.rs    — SelectivityTracker outer API + Inner state machine
└── tests.rs      — moved verbatim with adjusted imports
```

Public-API names and signatures are kept stable; crate-internal items (`count_skippable_bytes`, `FilterState`) keep `pub(crate)` visibility through targeted re-exports in `mod.rs`. Submodule-internal items (`SelectivityStats`, the `Inner` state struct, helpers) are `pub(super)` — tighter than the implicit "everything sees everything" of a single-file layout.

## 6. Validation

### 6.1 Tests

`cargo test --workspace --no-fail-fast`: **9 240 passed, 0 failed, 106 ignored.** Notable subsets:

- `cargo test -p datafusion-pruning --lib`: 82 / 82.
- `cargo test -p datafusion-datasource-parquet --lib`: 143 / 143.
- `cargo test -p datafusion --test parquet_integration`: 200 / 200 (covers the bloom-filter and predicate-cache surfaces).

### 6.2 Lint

`cargo clippy -p datafusion-datasource-parquet -p datafusion-pruning --all-targets --all-features -- -D warnings` clean.

### 6.3 Bench matrix

Three workloads (TPC-DS SF1, TPC-H SF1, ClickBench partitioned), two latency modes (local NVMe at 5 iterations; `--simulate-latency` at 3 iterations), three configurations: `main + pushdown=off`, `main + pushdown=on`, and the change with `pushdown=on`. Sequential same-state runs.

**Local NVMe** — sum-of-medians, ms

| Workload | main | main + pushdown | change | change / main | change / main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench (43 q) | 21 020 | 21 699 | **17 919** | **0.85×** | **0.83×** |
| TPC-DS (99 q) | 17 003 | 38 961 | **16 852** | **0.99×** | **0.43×** |
| TPC-H (22 q) | 780 | 989 | **691** | **0.89×** | **0.70×** |

**`--simulate-latency`** (20–200 ms per object-store op)

| Workload | main | main + pushdown | change | change / main | change / main+pushdown |
|---|--:|--:|--:|--:|--:|
| ClickBench (43 q) | 86 562 | 111 321 | **88 947** | 1.03× | **0.80×** |
| TPC-DS (99 q) | 76 418 | 141 940 | **77 546** | 1.01× | **0.55×** |
| TPC-H (22 q) | 23 723 | 52 597 | **24 157** | 1.02× | **0.46×** |

On local SSD the change beats `main + pushdown=off` outright across all three workloads (15–30 % faster). On the simulated cloud-storage profile it's at parity within 1–3 % — inside run-to-run noise — while always remaining 17–57 % faster than `main + pushdown=on`.

## 7. Alternatives considered

### 7.1 Always row-level when a static analysis says so

This is what `pushdown_filters = on` does today, and it's the per-query lottery this design exists to fix. Static cost / column-overlap analysis gets the wrong answer often enough that the config has stayed gated off by default. Rejected; the design above is what we're proposing *instead* of this.

### 7.2 Run pruning passes a second time, off the critical path, to populate the prior

Rejected for two reasons. First, even an "extra" run of page-index pruning reads page indexes that the existing pass has already read, doubling the file-open I/O on workloads with large page indexes (TPC-DS S3). Second, two paths that must produce the same `Vec<bool>` is exactly the kind of correctness debt future maintainers regret. The side-effect approach in §4.2 has one path, instrumented to also emit per-conjunct rates.

### 7.3 Defer placement entirely until after the first row group

i.e. always start at post-scan, promote based on samples. This is effectively `min_bytes_per_sec = INFINITY` for newly seen filters. Rejected: it loses on TPC-DS Q64 (a dynamic-filter chain) and ClickBench Q23 (the `LIKE '%google%'` poster child), because the wins of row-level filtering are real and dominate when present. The design has to capture them on file 1, before runtime samples exist.

### 7.4 A fully static cost model

Using compressed bytes per column, projected I/O cost, and a selectivity estimate from histograms. Rejected because DataFusion doesn't have the histogram infrastructure today, and a model without runtime samples can't react to dynamic filters or to mid-stream selectivity collapse.

## 8. Trade-offs and open questions

### 8.1 Warm-up cost on the first row group

The byte-ratio fallback runs when no per-conjunct rate is available. On ClickBench's `hits_partitioned` files there are no page indexes for many columns, so byte-ratio is the prior in practice for some queries. This is the small "ClickBench SSD within 1 % of `main` — occasionally slightly slower on individual queries" residual. Open question: feed the scheduler's accumulated runtime stats back as a Bayesian prior on `eff_mean` to short-circuit warm-up on subsequent files within the same scan. Estimated impact: small.

### 8.2 Three new config knobs

- `prior_promote_threshold` (default 0.5)
- `prior_demote_threshold` (default 0.05)
- `latency_z_baseline_ms` (default 5.0) / `latency_z_max_scale` (default 8.0)

Defaults were tuned against the bench matrix in §6.3. They live in `ParquetOptions` for runtime configurability. They are not yet plumbed through `proto-common`'s `ParquetOptions` message; `from_proto` currently restores them to defaults on deserialisation, so a round-trip preserves *defaulted* behaviour. Trivial follow-up.

### 8.3 Tagged-predicate visibility into the literal-column set

The §4.1 `literal_columns()` change is correct but asymmetric — a wrapper has to know about its leaves to compute its own column set. An alternative would be to model tagged predicates as an enum variant of `PruningPredicate` rather than a wrapper that special-cases `prune()` / `literal_columns()`. That's a bigger surgical change to the type and would not affect behaviour; deferred.

### 8.4 Single-row-group files

When `lineitem` is one file of one row group there's no swap point inside the file, so initial placement is the only placement. TPC-H still lands at 0.89× of `main` (not just at parity) because the per-conjunct pruning-rate prior picks row-level for the TPC-H filters that genuinely benefit from page-skipping — most visibly Q18's `l_quantity IN (subquery)` dynamic filter (0.59× of `main`, accounting for ~46 ms of the 89 ms aggregate delta), plus smaller wins on selective date and range predicates in Q1, Q3, Q19 (0.81–0.86×). For the filters that land at post-scan, the scheduler applies them inside the parquet opener (`apply_post_scan_filters_with_stats`) instead of via a `FilterExec` operator above the scan as `main + pushdown=off` would — but the two paths are equivalent in cost, so the post-scan-placed filters wash. The win is the correct row-level placements, not the in-scan-vs-above-scan choice for the rest. There is no shuffle re-balance involved either way; the post-scan path is per-partition, same as `FilterExec`-above-scan would be. Row-group morselization at [#21766](https://github.com/apache/datafusion/pull/21766) is the complementary fix for cross-partition skew on single-row-group files.

## 9. Migration / compatibility

- **Public API surface added**: `PruningPredicate::try_new_tagged_conjuncts`, `PruningPredicate::prune_per_conjunct`, `PerConjunctPruneStats` in `datafusion-pruning`. Backwards-compatible additions; existing callers are untouched.
- **`PruningPredicate::literal_columns()` semantics**: extended to union sub-predicate columns when `sub_predicates.is_some()`. Plain non-tagged predicates are unchanged. No callers are removed; downstream behaviour is *strictly more correct* (tagged predicates now report their actual columns instead of silently empty).
- **Config knobs added**: three new fields on `ParquetOptions` with defaults that preserve the existing `pushdown=on` semantics for callers that don't set them. No proto-schema additions yet (see §8.2).
- **Test fixture changes**: a handful of tests in `datafusion/core/tests/parquet/` set `filter_pushdown_min_bytes_per_sec = 0.0` to retain the legacy "pushdown=on means every filter runs row-level" contract that those tests were written against. Two fixture helpers (`ParquetScanOptions::config`, `ContextWithParquet::new`) do this centrally; individual tests inherit. No production-code-side ergonomic regressions.
- **No changes** to `datafusion-cli`, `datafusion-substrait`, or other crates beyond `datafusion-pruning`, `datafusion-datasource-parquet`, and test fixtures in `datafusion-core`.

## 10. References

- [#3463](https://github.com/apache/datafusion/issues/3463) — Enable parquet filter pushdown by default.
- [#20324](https://github.com/apache/datafusion/issues/20324) — Epic enumerating the regressions adaptive scheduling is meant to neutralise.
- [#20325](https://github.com/apache/datafusion/issues/20325) — Concrete example: ClickBench Q10 slows down with pushdown.
- [#21766](https://github.com/apache/datafusion/pull/21766) — Row-group morselization, the complementary partition-skew fix.
- `report.md` — discursive write-up with per-query drill-downs and chart commentary.
- `slides/datafusion-meetup-05-2026/` — companion conference deck.
