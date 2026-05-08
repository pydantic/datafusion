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

---

## Round 2 — pushed all-combined further

After round 1, three more changes landed on `exp/all-combined`
trying to get Q64 to win in the morselization flow:

| commit (on `exp/all-combined`) | what it does |
|---|---|
| `b206f5495` (allv3) | Adds `AdaptiveState` to `PushDecoderStreamState`; `transition()` calls `maybe_swap_strategy` at every row-group boundary inside the morsel stream. Drop-only semantics still in effect (mandatory demotes get re-added to row-level). |
| `96deb5bc2` (allv3.5) | Exposes 4 new `ParquetOptions` knobs: `filter_prior_promote_threshold`, `filter_prior_demote_threshold`, `filter_latency_z_baseline_ms`, `filter_latency_z_max_scale`. So everything is tunable per query via env vars without rebuilding. |
| `44b394277` (allv4) | Stream-level post-scan filtering. When a mandatory demote's columns are entirely in the user projection, the filter is rebased against the stream schema and applied per-batch before the projector. Out-of-projection-column demotes still re-add to row-level. |

### Combined-flow smoke totals across the four variants

**no-lat (sum-of-medians, 18-query subset):**

| variant | total ms | vs baseline | Q64 |
|---|---:|---:|---:|
| baseline (PR) | 3 723 | 1.000× | 675 |
| exp4 (#21766 only) | 22 876 | 6.145× | 20 560 ✗ |
| allv2 (theirs adaptive) | 22 498 | 6.043× | 20 604 ✗ |
| allv3 (+ mid-stream swap) | 21 490 | 5.772× | 19 512 ✗ |
| **allv4** (+ post-scan filter) | 21 766 | 5.846× | **19 705 ✗** |

**lat:**

| variant | total ms | vs baseline | Q64 |
|---|---:|---:|---:|
| baseline (PR-lat) | 26 731 | 1.000× | 2 390 |
| exp4-lat | 56 521 | 2.114× | 23 023 ✗ |
| allv2-lat | 57 315 | 2.144× | 23 481 ✗ |
| allv3-lat | 57 737 | 2.160× | 24 291 ✗ |
| **allv4-lat** | 56 436 | 2.111× | **22 796 ✗** |

### Why Q64 still doesn't win

After enabling: per-conjunct adaptive partitioning at file open
(allv2), mid-stream `swap_strategy` at every row-group boundary
(allv3), AND stream-level post-scan filtering for in-projection
demotes (allv4) — Q64 remains pegged at main_on's ~30× regression.

Best diagnosis: with #21766's morselization, **each
`PushDecoderStream` processes one row group only** (the donated
chunk). The mid-stream re-evaluation inside a stream has nothing
past the first row group to adapt to. The shared `SelectivityTracker`
still accumulates stats across chunks, but for Q64-shaped TPC-DS
data the file layout limits how much cross-chunk learning can
happen before all the work is done.

To actually fix Q64 in the combined flow you need one of:

a. **Sub-RG adaptation** (report.md §7.1) — multiple swap points
   inside a single row group give the adaptive scheduler room to
   adapt even when the chunk is one row group long. **This is now
   the binding constraint and the highest-impact open work.**
b. **PostScan-by-default for placeholder dynamic filters** — make
   `OptionalFilterPhysicalExpr` whose inner expression is `lit(true)`
   start at PostScan unconditionally, sidestepping the warmup
   problem. Cheap follow-up to the page-pruning prior.
c. **Multi-row-group morsel chunks** for files that have many row
   groups but only a few partitions — would mean each stream sees
   multiple row groups and the existing mid-stream swap would fire.
   Architectural change to #21766.

### Final per-workload scorecard

For **exp3** (clean, no morselization), against `main_off`:

| Workload | exp3 vs main_off |
|---|---:|
| ClickBench (no-lat) | **0.81×** ✓ |
| ClickBench (lat) | 1.02× ≈ |
| TPC-DS (no-lat) | 1.07× |
| TPC-DS (lat) | **1.03×** ≈ |
| TPC-H (no-lat) | 1.21× (down from PR's 1.45×) |
| TPC-H (lat) | **1.01×** ≈ |

For **allv4** (real merge, morsels + adaptive + mid-stream swap +
post-scan filter), against the original PR:

| query | win/loss |
|---|---|
| TPC-DS Q24 | **0.13×** ✓ (massive — morselization win) |
| TPC-DS Q25 | 0.45× ✓ |
| TPC-H Q5-Q9, Q18 | 0.79-0.93× ✓ (morselization + adaptive) |
| ClickBench Q23 | 0.64× ✓ |
| TPC-DS Q64 | **29× ✗** (morselization re-introduces, sub-RG needed) |
| ClickBench Q11 | 1.45× ✗ |

### Bottom line

**`exp/pp-plus-laz` (exp3) is still the take-it-now landing target.**
It composes cleanly on top of #11, has clean wins on every workload-
config except TPC-DS no-lat (+1%), and ~150 LOC of well-isolated
change.

**`exp/all-combined` (allv4) is plumbing-complete** for the real
merge — partition_filters runs, mid-stream swap fires, post-scan
filtering is wired, env knobs are exposed. Q64 specifically needs
sub-RG adaptation to escape the 1-RG-per-chunk constraint imposed
by #21766's morselization. That's a follow-up arrow-rs change.

Updated branch list:

```
exp/baseline                     PR tip (your starting point)
exp/page-pruning-prior   exp1    cherry-pickable
exp/latency-aware-z      exp2    cherry-pickable
exp/pp-plus-laz          exp3    1+2 — best clean win, recommend landing
exp/rg-morsels-clean     exp4    pure #21766, no adaptive
exp/all-combined         exp7    full real merge: partition_filters +
                                 mid-stream swap_strategy + per-batch
                                 post-scan filter + 4 env knobs.
                                 Preserves morselization wins; Q64
                                 unfixed (sub-RG required).
```

---

## Round 3 — chasing Q64 inside the morsel flow

After round 2 left Q64 unfixed in `exp/all-combined`, I tried four
hypotheses that would each have been a quick win. None landed.

### Hypothesis A: placeholder dynamic filters dominate

Theory: Q64's `OptionalFilterPhysicalExpr` hash-join filters start as
`lit(true)` placeholders, get installed in the row filter, and pay
per-row eval cost on every row group before the build side finalises.
Force them to PostScan unconditionally on first encounter (when
`snapshot_generation == 0`) → never installed at row level.

Branch: `exp/optional-postscan` (commit `02f791d10`).

Result: **Q64 still 23 s.** So Q64's slow path is **not** the
placeholder filters.

### Hypothesis B: any filter at row level is the problem

Theory: setting `filter_pushdown_min_bytes_per_sec = INFINITY` makes
`partition_filters` short-circuit to "all post-scan", so the row
filter is empty on every chunk.

Test: env var `DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=inf`
(also tried `1e30`).

Result: **Q64 still 24 s.** So the slow path is **not** about row
filter placement at all — Q64 stays slow even with the row filter
empty.

### Hypothesis C: stream-side post-scan filter with wider stream schema

Theory: my round-2 integration only applied stream-side post-scan
filtering for filters whose columns are all in the user projection.
For OOP-column filters it re-added them to the row filter. So expand
the projection mask to include post-scan-filter columns; then ALL
demoted filters can run stream-side.

Branch: `exp/optional-postscan` + commit `a9f0106f0` (since reverted
in `befa82605`).

Result: **Q64 worse — 24 s with default config.** Why: the wider
mask reads extra columns the user projection wouldn't otherwise
touch. Q64's filter columns include join keys (heavy ints/strings)
not in the SELECT list. The extra decode cost more than offsets any
post-scan-filter savings. **Wrong architecture.**

### Hypothesis D: morsel + pushdown=true is structurally bad on Q64

Sanity check: `pushdown_filters=false` on the same branch.

Result: **Q64 = 486 ms.** Same as `main_off`. So #21766 +
`pushdown=true` is structurally slow on Q64 regardless of any
adaptive scheduling logic on top. The adaptive layer can't fix what's
upstream in #21766's morselization itself.

### Conclusion

Q64's regression on the morselized flow is **not** addressable by
the adaptive scheduler alone. The path forward must be one of:

1. **Sub-row-group adaptation in arrow-rs** — gives the adaptive
   scheduler swap points within the chunk's single row group.
   Whether it fixes Q64 depends on whether the regression is a "no
   swap points to adapt at" issue or something deeper.

2. **Upstream fix in #21766** — if the regression is from how
   morselization decomposes the work (chunk-rebuild overhead,
   donor/stealer task overhead), that needs investigation in the
   morsel codepath itself.

3. **Restrict morselization to multi-row-group files** — if Q64's
   TPC-DS files are 1-row-group-per-file, donating those 1-RG
   chunks might be net-negative. A "only donate when the file has
   ≥ K row groups" check would let files with morselization-friendly
   layouts benefit while leaving the rest on the original flow.

I lean toward (3) as the easiest test — if `K = 2` recovers Q64
while preserving TPC-H Q7-Q9 wins, that's the real unblocker. But
that's a #21766 design change, out of scope here.

### Branches at end of overnight session

```
exp/baseline                       PR tip
exp/page-pruning-prior   exp1      cherry-pickable
exp/latency-aware-z      exp2      cherry-pickable
exp/pp-plus-laz          exp3      ★ recommend landing
exp/rg-morsels-clean     exp4      pure #21766
exp/all-combined         exp7      real merge — Q64 unfixable here
exp/optional-postscan    exp11     placeholder→postscan (no Q64 win),
                                   wider-schema commit reverted
```

### Final scorecard

For **`exp/pp-plus-laz`** (the recommended landing), against `main_off`:

| Workload | exp3 vs main_off |
|---|---:|
| ClickBench (no-lat) | **0.81×** ✓ (18 % faster) |
| ClickBench (lat) | 1.02× ≈ tied |
| TPC-DS (no-lat) | 1.07× (slight loss) |
| TPC-DS (lat) | **1.03×** ≈ tied (was 1.15×) |
| TPC-H (no-lat) | 1.21× (was 1.45×) |
| TPC-H (lat) | **1.01×** ≈ tied |

**No cell is worse than 1.21× of `main_off`, and four out of six
are ≤ 1.03×.** Cleanest multi-workload result of the night.

---

## Round 5 — proper architecture: per-conjunct rates as side-effect

User pushback (and feedback memory): page-pruning prior should
extract from existing pruning runs, not do per-conjunct re-evaluation.
Round 4's implementation re-ran page pruning per conjunct per file,
which was an "extra pruning run" by the cost-aware definition. This
round does it properly.

**Implemented** (branch `exp/per-conjunct-rates`):

1. **`PagePruningAccessPlanFilter`** gets an optional `tags: Option<Vec<usize>>`
   field and a `new_tagged(conjuncts: &[(usize, Arc<dyn PhysicalExpr>)],
   schema)` constructor that takes pre-split tagged conjuncts. Tags
   survive the filter_map that drops always-true / multi-column
   predicates.
2. **New `prune_plan_with_per_conjunct_stats`** method runs the same
   per-row-group / per-predicate iteration as `prune_plan_with_page_index`
   but also tracks `Vec<PerConjunctPageStats>` (rows_seen,
   rows_skipped, optional tag) — surfaced as a side effect of the
   pruning iteration the opener was going to run anyway. **No extra
   pruning passes.**
3. **Opener `build_stream`** now:
   - Builds the page filter via `new_tagged` when `predicate_conjuncts`
     is set;
   - Reorders so `prune_by_limit` + `prune_plan_with_per_conjunct_stats`
     run **before** the initial `partition_filters` call, so per-FilterId
     rates are available as the prior on the very first placement
     decision;
   - Captures rates into `HashMap<FilterId, f64>`, threads into
     `AdaptiveParquetStream` as `page_pruning_rates`, passes on every
     `partition_filters` call (initial + mid-stream swap).
4. **`SelectivityTracker::partition_filters`** takes a new
   `page_pruning_rates: &HashMap<FilterId, f64>` parameter; the prior
   reads from this map. Falls back to byte-ratio when no rate is
   available (page index disabled, multi-column predicate, schema
   mismatch). The old per-conjunct re-evaluation helpers
   (`pruning_rate_for_filter`, `build_per_conjunct_pruning_predicate`)
   are deleted.

### Results

```
Workload                   main_off      PR_on    exp3_on    perconj   perc/PR  perc/exp3
ClickBench (no-lat)           21020      17190      17130      17839     1.04x     1.041x
TPC-DS (no-lat)               17003      18056      18260      18226     1.01x     0.998x  ← MATCHES EXP3
TPC-H (no-lat)                  780       1128        941        990     0.88x     1.052x
ClickBench (lat)              86562      89044      88482      89330     1.00x     1.010x
TPC-DS (lat)                  76418      88026      78910      87813     1.00x     1.113x  ← worst gap
TPC-H (lat)                   23723      24684      23956      24195     0.98x     1.010x
```

### What this confirms

- **The architecture works on data with page indexes.** TPC-DS no-lat:
  perconj matches exp3 exactly (0.998×). The `RUST_LOG` trace on
  `tpcds Q26` shows the prior firing correctly:
  ```
  page-prior (pruned_rate=0.726 >= 0.5) — d_year@6 = 1998        → row-level
  page-prior (pruned_rate=0.000 <= 0.05) — cd_gender@1 = F         → post-scan
  page-prior (pruned_rate=0.000 <= 0.05) — cd_marital_status@2 = W → post-scan
  ```

- **`hits_partitioned` doesn't have page indexes.** `column_index_present=false`
  / `offset_index_present=false` for every file. So perconj never
  fires on ClickBench — falls back to byte-ratio per the design.
  exp3's row-group re-eval was finding wins there that perconj
  doesn't get, hence the 4 % slow-down.

- **Latency: TPC-DS perconj loses 11 % vs exp3.** exp3's row-group
  re-eval caught lat-sensitive demotes that page-prior alone misses
  (cases where the row-group min/max can rule out a filter but page
  index would just say "all pages might match → no signal"). Under
  latency this 11 % is real; on no-lat it's noise (perconj actually
  beats exp3 by 0.2 % there).

### Trade-off summary

| | exp3 (re-eval) | perconj (side-effect) |
|---|---|---|
| Architecture | "Extra pruning run" per-file per-conjunct | Reads from existing pruning iteration |
| TPC-DS no-lat | 1.07× of main_off | 1.07× (matches) |
| TPC-DS lat | 1.03× of main_off ✓ | 1.15× (loses exp3's lat win) |
| ClickBench no-lat | 0.81× of main_off ✓ | 0.85× |
| Code surface | One file (`selectivity.rs`) | Three files; new public-ish API on `PagePruningAccessPlanFilter` |

### Two options to land

The user has explicitly preferred the architecturally correct version.
On that basis: **`exp/per-conjunct-rates` is the right thing to land**,
even with the 5 % aggregate cost vs exp3. To recover exp3's TPC-DS-lat
edge under the proper architecture, the follow-up is to also surface
per-conjunct rates from the row-group `PruningPredicate::prune` pass —
that's a `datafusion-pruning` API addition (`PruningPredicate` doesn't
naturally split internally, so it needs a per-sub-expression
evaluator). Out of scope tonight.

If the lat performance gap matters more than architectural cleanliness
in the short term, **`exp/pp-plus-laz` (exp3) is the take-it-now
option** — same code surface as the existing PR, ~150 LOC.

### Branch state at end of round 5

```
exp/baseline                      PR tip
exp/page-pruning-prior   exp1     cherry-pickable (row-group prior, re-eval)
exp/latency-aware-z      exp2     cherry-pickable
exp/pp-plus-laz          exp3     1+2 — best perf numbers via re-eval
exp/rg-morsels-clean     exp4     pure #21766
exp/all-combined         exp7     real merge — Q64 unfixable
exp/optional-postscan    exp11    placeholder→postscan dead end
exp/page-pruning-prior-v2 r4      page re-eval, rejected
exp/per-conjunct-rates   r5       ★ proper architecture (page-prior as
                                  side-effect of opener's existing
                                  pruning, no extra runs)
```

---

## Round 4 — page-pruning prior tried and rejected

User push: page stats subsume row-group stats; prefer page-level
when available, fall back to row-group only when page index isn't
loaded; ideally don't run any "extra pruning pass" — extract from
the prunings the opener already does.

**Implemented** (branch `exp/page-pruning-prior-v2`, commit
`7188f3959`): `pruning_rate_for_filter` first checks for
`column_index() + offset_index()`; if present, builds a per-conjunct
`PagePruningAccessPlanFilter`, runs `prune_plan_with_page_index`
against an `all-row-groups-selected` `ParquetAccessPlan`, walks the
result to compute `pruned_rows / total_rows`. Falls back to
row-group min/max only when the page index is genuinely absent.

**Result**: aggregate **+18 % vs exp3** on smoke (no-lat).

```
no-lat smoke (3 iter, 18 queries):
  baseline (PR)  : 3 723 ms
  exp3           : 3 549 ms  (0.95×, the recommendation)
  page-prior     : 4 200 ms  (1.13× of baseline, 1.18× of exp3 — clear loss)

Concrete regressions vs exp3:
  TPC-DS Q26      :  55 →  105 ms (1.91×)  ← exp3's win is GONE
  TPC-H Q5        :  56 →   97 ms (1.73×)
  ClickBench Q11  :  64 →   96 ms (1.51×)
  ClickBench Q21  : 622 →  857 ms (1.38×)
  TPC-H Q9        : 110 →  146 ms (1.32×)
  TPC-H Q18       :  63 →   89 ms (1.41×)
  ClickBench Q23  : 152 →  177 ms (1.17×)
```

**Two failure modes**, in order:

1. **The page-pruning evaluation is itself an "extra pruning run"
   in the cost sense.** Building a per-conjunct
   `PagePruningAccessPlanFilter` and calling `prune_plan_with_page_index`
   walks the page index for every page in every row group. For files
   with many small pages × many conjuncts × hundreds of files
   (ClickBench partitioned), the per-file overhead dominates the
   benefit. This is exactly the cost concern flagged.

2. **Removing the row-group fallback lost exp3's Q26-style win.**
   exp3's row-group prior caught "stats are present but the filter
   isn't selective" via the `any_stats` guard → forced PostScan
   placement immediately. Page-prior often returns `None` for the
   same filter (page index says "all pages might match" → no
   signal); with the row-group fallback gated on "page index NOT
   loaded" the byte-ratio path kicks in instead, placing at
   row-level. The runtime tracker would eventually demote, but in a
   3-iteration smoke (and probably a real query running once) the
   convergence doesn't happen in time.

### What a proper version would need

The user's actual architecture — "extract from the prunings that
already happened" — would require:

a. `PruningPredicate::prune` (in `datafusion-pruning`) instrumented
   to surface per-conjunct pass/fail rates as a side effect. Today
   it only returns the combined boolean per row group. Per-conjunct
   rates exist in the implementation (it splits internally) but
   aren't exposed.

b. Same for `PagePruningAccessPlanFilter::prune_plan_with_page_index`.

c. The opener captures both per-conjunct rates after each pruning
   pass and threads them through to `partition_filters` as a
   `Map<FilterId, f64>`.

d. The prior reads from this map; if a filter has no entry, falls
   back to byte-ratio.

This is ~200-300 LOC across `datafusion-pruning` and `datafusion-
datasource-parquet`, plus an API addition to `PruningPredicate`.
**Not done tonight; documented as a follow-up.**

The simpler version — coarser file-level pruning rate applied to all
new conjuncts — was considered and rejected: it would lose
per-conjunct precision (e.g. Q26 has a mix of selective and
non-selective conjuncts; one file-level rate can't drive both
correctly).

### Verdict

`exp/pp-plus-laz` (exp3) **stays the recommended landing target**.
The page-pruning prior is a real follow-up but needs the proper
"extract from existing prunings" implementation, not the
"re-evaluate per conjunct" shortcut. Branch
`exp/page-pruning-prior-v2` is preserved as a reference for what
*not* to do, with this writeup as context.
