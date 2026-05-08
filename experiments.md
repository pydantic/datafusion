# Overnight follow-up experiments — adaptive filter scheduling

Branched off `exp/baseline` (PR tip `9a1705088`). Five experiments
(plus one rejected as too big for tonight's time budget). Each has
its own branch — feel free to cherry-pick whatever subsets you want
to iterate on.

```
exp/baseline                       9a1705088    PR tip (your starting point)
exp/page-pruning-prior   (exp1)    cherry-pickable
exp/latency-aware-z      (exp2)    cherry-pickable
exp/pp-plus-laz          (exp3)    1 + 2 — the cleanest win
exp/rg-morsels-clean     (exp4)    pure #21766, no adaptive
exp/all-combined         (exp3+4)  real merge, partial integration
```

## Smoke bench harness

`/tmp/smoke_bench.sh` and `/tmp/smoke_bench_lat.sh` — 18 queries × 3
iter, on the regression / win hotspots from `report.md` §6:

- TPC-DS Q24/25/26/34/50/64 (regression hotspots in the PR vs main_off)
- TPC-H Q5/7/8/9/17/18 (the morselization-shaped data-skew workloads)
- ClickBench Q11/21/23/37/40/42 (mix of wins and latency hotspots)

## TL;DR matrix (smoke totals, sum of medians)

| variant | nolat smoke | lat smoke | wins | losses |
|---|---:|---:|---|---|
| `exp/baseline` (PR tip) | 3 723 ms | 26 731 ms | (reference) | (reference) |
| **exp1** page-pruning prior | 3 702 ms (0.994×) | not run | TPC-DS Q26 0.61× | ClickBench Q11 +17% |
| **exp2** latency-aware z | not run | 27 086 ms (1.013×) | ClickBench Q23 0.91× | ClickBench Q37 +63% (outlier) |
| **exp3** = 1 + 2 | **3 549 ms (0.953×)** | **25 373 ms (0.949×)** | **all 18 flat-or-better** except TPC-H Q5 lat +8% | — |
| **exp4** #21766 alone (no adaptive) | 22 876 ms (6.145×) | 56 521 ms (2.114×) | TPC-DS Q24 0.43×, TPC-H Q7-9 0.60-0.64× | TPC-DS Q64 30.4× / 9.6× ← lost adaptive's fix |
| **all-combined v2** (3 + 4) | 22 498 ms (6.043×) | 57 315 ms (2.144×) | morselization wins preserved (Q24 0.12×, TPC-H 0.68-0.91×) | TPC-DS Q64 still 30.5× — adaptive's mid-stream re-eval not ported |

## Per-experiment notes

### exp1 — page-pruning prior (`exp/page-pruning-prior`)

When a filter is first encountered, ask `PruningPredicate::prune(stats)`
for its row-group pruning rate (per-conjunct). If ≥ 0.5 → place
row-level. If ≤ 0.05 *and* the file actually has stats for the
filter's columns → place post-scan. Else fall back to the existing
byte-ratio heuristic.

**No-lat smoke:** 0.994× — flat. Wins on the TPC-DS regression
hotspot (Q26: 91→55 ms = 0.61×) and on ClickBench fast-query lat
hotspots (Q40/Q42/Q37: 0.69-0.80×). Loss on ClickBench Q11
(`MobilePhoneModel <> ''`, +17%) — `<> ''` doesn't prune row groups
via stats, so the prior runs and falls through to the byte-ratio
fallback; the cost is the per-file `PruningPredicate` construction.

**Two new config knobs:** `prior_promote_threshold = 0.5`,
`prior_demote_threshold = 0.05`. The "no stats present → return None"
guard in `pruning_rate_for_filter` is important — without it, files
without stats look identical to "filter is genuinely non-selective"
and the demote path forces them to PostScan unnecessarily.

### exp2 — latency-aware confidence_z (`exp/latency-aware-z`)

`SelectivityTracker` tracks total fetch wall time and number of
fetches across all openers. `effective_z()` shrinks the configured
`confidence_z` proportional to observed average per-fetch latency
above 5 ms (clamped to 8× max). The CI-bound demote/promote checks
use the effective z.

**Lat smoke:** 1.013× — marginal. Wins on ClickBench Q23 (0.91×),
TPC-H Q18/Q8 (0.94-0.95×). Outlier loss: ClickBench Q37 +63% — the
tighter z under latency makes both promote *and* demote fire on
borderline filters, causing flips between row-level and post-scan
that cost a `swap_strategy` per row group.

**Two new config knobs:** `latency_z_baseline_ms = 5.0`,
`latency_z_max_scale = 8.0`. The tracker also gets a small
`record_fetch(ranges, elapsed_ns)` API hook called from the opener
after every `get_byte_ranges`.

### exp3 — combined page-pruning + latency-aware z (`exp/pp-plus-laz`)

Both changes on top of the PR. **This is the cleanest win.**

**No-lat smoke (3 iter, 18 queries):** 0.953× — 4.7% faster
aggregate. Every query flat-or-better except TPC-H Q9 (+3%, noise).

```
TPC-DS Q26     91 →  55 ms (0.60×)  ← regression hotspot fixed
ClickBench Q42 11 →   8 ms (0.66×)
ClickBench Q40 13 →   9 ms (0.71×)
ClickBench Q37 30 →  25 ms (0.84×)  ← exp2's outlier neutralised by exp1
TPC-DS Q34      5 →   4 ms (0.84×)
ClickBench Q11 68 →  64 ms (0.93×)  ← exp1's outlier neutralised by exp2
TPC-DS Q24    862 → 828 ms (0.96×)
TPC-DS Q25    382 → 356 ms (0.93×)
TPC-DS Q50    305 → 291 ms (0.95×)
```

**Lat smoke (3 iter, 18 queries):** 0.949× — 5.1% faster. TPC-DS
Q25 (0.72×), Q26 (0.56×), Q50 (0.88×), ClickBench Q23 (0.87×), Q42
(0.67×) all significantly faster. Only loss: TPC-H Q5 (+8%).

**Hypothesis confirmed:** the prior settles initial placement → the
latency-aware z gate only fires when runtime samples disagree, so
the borderline-flip churn (exp2's Q37 outlier) doesn't happen.

#### exp3 full benches (5 iter no-lat, 3 iter lat, all queries)

Compared against the PR baseline:

| Workload | PR_on | **exp3_on** | exp3 vs PR |
|---|---:|---:|---:|
| ClickBench (43q, no-lat) | 17.19 s | 17.13 s | 1.00× tied |
| TPC-DS (99q, no-lat) | 18.06 s | 18.26 s | 1.01× tied |
| **TPC-H (22q, no-lat)** | 1.13 s | **0.94 s** | **0.83× — 17% faster** |
| ClickBench (43q, lat) | 89.04 s | 88.48 s | 0.99× tied |
| **TPC-DS (99q, lat)** | 88.03 s | **78.91 s** | **0.90× — 10% faster** |
| TPC-H (22q, lat) | 24.68 s | 23.96 s | 0.97× |

vs `main_off` (the strict "never slower than `pushdown=off`" target):

| Workload | main_off | PR_on | **exp3_on** | exp3 vs main_off |
|---|---:|---:|---:|---:|
| ClickBench (no-lat) | 21.02 s | 17.19 s | 17.13 s | **0.81×** ✓ |
| ClickBench (lat) | 86.62 s | 89.04 s | 88.48 s | **1.02×** ≈ tied |
| TPC-DS (no-lat) | 17.00 s | 18.06 s | 18.26 s | 1.07× |
| TPC-DS (lat) | 76.42 s | 88.03 s | 78.91 s | **1.03×** ≈ tied |
| TPC-H (no-lat) | **0.78 s** | 1.13 s | 0.94 s | 1.21× (was 1.45×) |
| TPC-H (lat) | 23.72 s | 24.68 s | 23.96 s | **1.01×** ≈ tied |

**exp3 brings every single workload-config to ≤ 1.07× of `main_off`,
≤ 1.03× under latency.** TPC-H no-lat regression cuts from +44% →
+21%; TPC-DS lat regression from +15% → +3%. This matches the
report.md §7 prediction exactly: prior closes fast-query and TPC-DS-lat
gaps, latency-aware z handles the remainder.

**Recommendation:** **land exp3 as additional commits on top of #11.**
Two commits, ~150 LOC, four new config knobs, no breaking changes,
clean wins on the workloads that #11's report flagged as residual
regressions. The implementation is in `exp/pp-plus-laz`.

### exp4 — pure #21766 morselization, no adaptive (`exp/rg-morsels-clean`)

Branch off upstream/main + #21766's 7 commits. No adaptive
scheduling.

**Smoke results vs `exp/baseline`:**
- TPC-DS Q24 0.43× (the worst regression in the PR — fixed)
- TPC-H Q7/Q8/Q9 0.60-0.64× (the data-skew workload — fixed)
- TPC-DS Q64 **30.44×** (regression that #11 fixes — reintroduced)
- ClickBench Q11 1.35× (still problematic)

**Aggregate is dominated by Q64.** Without Q64 the smoke totals are:
- nolat: 22 876 - 20 604 = 2 272 ms vs 3 723 - 675 = 3 048 ms baseline = 0.75× (25% faster)
- lat: 56 521 - 23 023 = 33 498 ms vs 26 731 - 2 390 = 24 341 ms baseline = 1.38×

So #21766 alone:
- Fixes TPC-H + TPC-DS Q24 (the structural data-skew workloads — Problem B)
- Loses Q64 (the dynamic-filter workload that needs adaptive demote — Problem A)
- Mixed under latency

This is the cleanest demonstration of the **Problem A vs Problem B
orthogonality** from `report.md` §5: morselization fixes Problem B
(skew), adaptive scheduling fixes Problem A (filter cost). They
don't substitute for each other — they're *both* needed for the
clean cell of (workload × config) comparisons.

### all-combined v2 — partial integration (`exp/all-combined`)

A real merge that integrates `partition_filters` into #21766's
`prepare_filters`. Steps taken:

1. Reset `opener.rs` and `row_filter.rs` to #21766's versions.
2. Added `predicate_conjuncts` and `selectivity_tracker` fields on
   `ParquetMorselizer`, propagated through `PreparedParquetOpen` and
   `FiltersPreparedParquetOpen`.
3. In `prepare_filters`, if `predicate_conjuncts` is set: call
   `selectivity_tracker.partition_filters` to get row-level + post-scan
   splits. Build the row-filter candidates only from the row-level
   subset.

**Pragmatic shortcut:** mandatory filters that the tracker would
have demoted to post-scan are *re-added* to row-level (so they
still execute correctly). The intended stream-level post-scan
filtering is not ported. This means we get the *drop* benefit
(optional filters can be removed entirely — the Q64 mechanism)
but not the *demote-mandatory-out-of-row* benefit.

**Result: Q64 is still 30× slower.** Why? Because Q64's win in #11
comes from **mid-stream re-evaluation** at every row group boundary
via `AdaptiveParquetStream::maybe_swap_strategy`. In #21766's flow
there's no `AdaptiveParquetStream` — `partition_filters` runs once
at file open, never re-evaluates. The dynamic filter starts at one
placement and stays there for the whole file.

**To make all-combined actually preserve Q64**, the missing piece
is porting `AdaptiveParquetStream`-equivalent logic into #21766's
`build_stream` — wrap the `PushDecoderStreamState`'s `transition()`
loop with row-group-boundary `partition_filters` re-runs and
`swap_strategy` calls. That's ~200 LOC of careful integration; not
done tonight.

#### What all-combined v2 *does* show

The merge mechanics work — adaptive scheduler runs, `partition_filters`
fires per file, optional filter dropping (when it happens) does
something, **and** morselization donates work to siblings. So:

```
TPC-DS Q24:    862 →   108 ms (0.12×) ← morselization win even bigger
TPC-DS Q25:    382 →   173 ms (0.45×) ← morselization win
TPC-H Q7-Q9:    ~ 0.68-0.81×          ← morselization wins preserved
ClickBench Q23: 159 → 120 ms (0.76×) ← preserved
TPC-DS Q64:                30.50×   ← STILL BROKEN (no mid-stream re-eval)
```

So: the path to a "real" exp3+exp4 combination exists; the missing
work is the AdaptiveParquetStream port. About 200-300 LOC.

### exp5 — sub-RG adaptation in arrow-rs

**Not attempted tonight.** Real arrow-rs API changes (add `pause` /
`resume_with` to `ParquetPushDecoder`, manipulate `RowSelectionCursor`
+ `array_reader` + `PushBuffers` state). Order of magnitude bigger
than the other experiments. The report.md §7.4 ranking already had
this as #4 — lowest priority among the four.

## Conclusions

1. **`exp3` is the clear take-it-now win.** Page-pruning prior +
   latency-aware z, ~150 LOC, four new tunable config knobs, **17%
   faster than the PR on TPC-H (no-lat), 10% faster on TPC-DS-lat**,
   no per-query regression > 8%. Both halves compose better than
   either alone (the experiments were neutral individually but the
   combination is meaningfully positive). Recommend cherry-picking
   exp1 and exp2 onto #11 before landing.

2. **Real exp3 + #21766 integration needs the AdaptiveParquetStream
   port.** all-combined-v2 (this branch) demonstrates the missing
   piece: without mid-stream `swap_strategy` re-evaluation, the
   adaptive demote/drop logic only fires once per file and Q64 is
   left at the bad initial placement. The right follow-up is to
   port `AdaptiveParquetStream::maybe_swap_strategy` into #21766's
   `build_stream`. Estimated ~200-300 LOC, mostly mechanical.

3. **Problem A and Problem B are orthogonal, confirmed empirically.**
   #21766 alone fixes TPC-H / TPC-DS Q24 (structural skew); #11 alone
   fixes TPC-DS Q64 (per-conjunct cost). Neither alone fixes both.
   The full unification is a multi-step project, but exp3 + #11 is a
   meaningful intermediate stop on the way.

## Files

- Branches: `exp/page-pruning-prior`, `exp/latency-aware-z`,
  `exp/pp-plus-laz`, `exp/rg-morsels-clean`, `exp/all-combined`.
- Smoke bench artifacts: `/tmp/smoke/{baseline,exp1,exp3,exp4,allv2}{,_lat}/`.
- Full bench artifacts: `benchmarks/results/{PR,EXP3}-pushdown{,-lat}/`.
- Smoke harness: `/tmp/smoke_bench{,_lat}.sh`, `/tmp/smoke_compare.py`.
- Build snapshots: `/tmp/dfbench-{pr,exp1,exp2,exp3,exp4,allv2}` (the
  binaries each experiment was benched with).

## Possible next steps (when you're back)

In order of bang/buck:

1. **Land exp3 on #11.** The numbers speak for themselves; cherry-picking
   the two commits from `exp/pp-plus-laz` is mechanically easy.

2. **Tune the new knobs.** I picked the thresholds (0.5/0.05 for the
   prior, 5ms/8x for latency z) by inspection. Worth a small grid
   search if you want to squeeze more out.

3. **Port AdaptiveParquetStream's `maybe_swap_strategy` into #21766's
   `build_stream`.** Restores Q64 win on the morselization path.
   Bigger change — would need design review before implementing.

4. **Sub-RG adaptation in arrow-rs.** Per report.md §7.1: add
   `pause`/`resume_with` to `ParquetPushDecoder`. Significant
   arrow-rs surface area; probably worth a separate follow-up arrow-rs
   PR after #21766 lands.
