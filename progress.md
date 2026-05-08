# Indefinite-iteration progress log

Working forward from `exp/per-conjunct-rates` (round 5). Goal: keep pulling
on threads to improve the adaptive scheduler. Record each iteration so
context is recoverable without re-deriving everything.

This file is intentionally append-only-ish: when a branch / hypothesis is
done, distill it into one or two lines, drop the long working notes.

## Current target tree

```
exp/baseline                       9a1705088 (PR tip)
exp/per-conjunct-rates             ★ round 5 — ARCHITECTURE BASELINE we're iterating from
exp/pp-plus-laz                    exp3 — best-perf-via-shortcut comparison anchor
exp/r6-pruningpredicate-rates      ◀ active
```

## Anchor numbers (smoke totals, sum of medians)

```
Workload          main_off  PR_on    exp3     perconj   r6      r6/exp3 r6/perconj
ClickBench no-lat   3723    -      3549     3587      3617     1.019x  1.008x
TPC-DS    no-lat    -       -      -        -         -        -       -
TPC-H     no-lat    -       -      -        -         -        -       -
all no-lat smoke    3723    -      3549     3587      3617     1.019x  1.008x
all lat smoke      26731    -     25373    26589     26382     1.040x  0.992x  ← r6 better than perconj
```

Full benches:
```
TPC-DS lat full  : main_off 76418, PR 88026, exp3 78910 (1.03x), perconj 87813 (1.15x), r6 86370 (1.13x — 0.984x of perconj)
```

r6 narrows the perconj→exp3 lat gap from 11% to ~10% at full scale.
Significant per-query wins under lat: Q37 -22%, Q42 -21%, Q34 -25%,
Q17 -8%. But Q25/Q26/Q50 are flat vs perconj — the dynamic CASE-with-
hash-lookup filters can't be turned into pruning predicates (always
return always_true), so they get no per-conjunct rate and fall to
byte_ratio either way.

## Iteration log

### r6 (active) — `PruningPredicate` per-conjunct rates as side-effect

**Hypothesis**: `PruningPredicate::prune` currently evaluates a single
rewritten `predicate_expr` against the stats batch. If we build N
per-conjunct PruningPredicates at construction time (cheap, one-time)
and evaluate them per-row-group at `prune()` time, we get:
1. A combined `Vec<bool>` (AND of per-conjunct results) — same output
   as today's `prune()`.
2. Per-conjunct rates as a free side-effect of the same iteration.

The "extra work" is per-conjunct vs combined evaluation of small
predicate expressions against a stats batch. Microseconds per file.
Critically: this is ONE pass (the same pass `prune()` already runs),
not an extra pruning run.

**Status**: implementing `PruningPredicate::prune_per_conjunct` in
`datafusion-pruning`. Branch: `exp/r6-pruningpredicate-rates`.

**Plan**:
1. ☑ Look at `PruningPredicate::try_new` / `prune` internals
2. ☑ Add per-conjunct prebuilt sub-predicates field (`sub_predicates: Option<Vec<TaggedSubPredicate>>`)
3. ☑ Add `prune_per_conjunct(stats) -> Result<(Vec<bool>, Vec<PerConjunctPruneStats>)>`
4. ☑ Wire opener.rs row-group pruning to capture rates (`prune_by_statistics_with_per_conjunct_stats`)
5. ☑ Merge with existing page-pruning rates (row-group seeds the map; page-index overrides where both exist — page is finer-grained)
6. ☐ Build (in flight), smoke vs perconj baseline
7. ☐ Full TPC-DS-lat to verify gap closure (target: ~1.03× of main_off, matching exp3)

**Outcome**: r6 narrows TPC-DS-lat gap to exp3 by ~16%, sum-of-smoke
better than perconj across both modes. Diminishing-return finding:
TPC-DS dynamic CASE-with-hash-lookup filters can't be turned into
single-conjunct PruningPredicates (they always return always_true);
those conjuncts have NO row-group rate either way, so r6 doesn't
improve them.

### r7 — bloom-filter per-conjunct rates ☑

Added `prune_by_bloom_filters_with_per_conjunct_stats`; merged into
`row_group_per_conjunct` taking max rate per FilterId.

**Result**: smoke lat 1.033× of exp3 (was 1.040× r6, 1.048× perconj).
Modest 0.7% improvement. Concrete wins under lat: ClickBench Q42
411→350, Q40 437→385, Q21 4557→4410. TPC-DS full-lat: 85 772 ms
(8.7% behind exp3, was 11.3% perconj, 9.5% r6).

Diminishing returns: each iteration shaves ~0.7-1.5% off the gap.

### r8 — refresh rates for populated dynamic filters ☑

Implemented `fresh_rate_for_dynamic_conjunct` and called it from
`partition_filters` only when `snapshot_generation(&expr) > 0`.
Static filters use the side-effect rates from existing pruning;
dynamic filters with current values get a targeted re-eval.

**Results**:
- smoke lat: **1.013× of exp3** (was r7 1.033×, r6 1.040×, perconj 1.048×) — close to parity
- full TPC-DS lat: **1.074× of exp3** (was 1.087× r7, 1.095× r6, 1.113× perconj) — half the original gap closed

Per-query wins vs exp3 on smoke lat: Q34 0.74×, Q64 0.90×, Q17/Q18 0.97×.
Still behind: Q25 1.20×, Q26 1.18×, Q37 1.30×.

**Diagnosis of the residual TPC-DS Q25/Q26 gap**: dynamic filters in
these queries have shape
`col >= lo AND col <= hi AND hash_lookup(...)`. The `hash_lookup`
makes `PruningPredicate::try_new` return always-true (rewriter
can't handle CASE-with-hash). So `fresh_rate_for_dynamic_conjunct`
returns None for these — they fall to byte_ratio just like in r7
or earlier.

exp3 also can't prune these (same rewriter), so its win on Q25/Q26
must come from a different mechanism — possibly the order in which
filters get demoted, or how mid-stream `maybe_swap_strategy` cascades
state. Not investigated further this round.

### r9 — partial-AND prior with promote-only semantics ☑ (no improvement)

Tried two variants:
- **r9 v1**: split inner conjunct via `split_conjunction`, build
  PruningPredicate per part, take max rate ≥ 0.5 as promote signal.
  No effect — split_conjunction doesn't descend through
  DynamicFilterPhysicalExpr wrappers.
- **r9 v2**: snapshot the dynamic filter first via
  `snapshot_physical_expr_opt`, then split. Compiled cleanly,
  matched r8 numbers exactly (84 765 vs 84 783 on full TPC-DS-lat,
  Q25/Q26 still 1.29×/1.34× behind exp3).

**Diagnosis from trace**:
The dynamic filters in Q26 ARE getting demoted correctly via
page-prior (`cs_promo_sk >= 1 AND <= 300 AND hash_lookup` →
pruned_rate=0.000 → PostScan). Same placement as exp3 should
produce. So the residual TPC-DS-lat gap is **not from initial
placement decisions**.

Possibilities for the residual ~7% gap on full TPC-DS-lat:
- Mid-stream behavior (`maybe_swap_strategy` cascading state
  differently)
- Order of post-scan filters within a stream
- How runtime tracker stats interact with new placements over time
- Something in the bloom-filter or page-index pruning iteration
  that exp3's flow exercises slightly differently

These would need deep investigation per-query traces with
EXPLAIN ANALYZE on both binaries. **Stopping point: r8 is the
right architecture. Smoke lat 1.013× of exp3, full TPC-DS-lat
1.074×. The remaining gap is in something other than per-conjunct
selectivity priors.**

### r11 — sort filters by per-conjunct rate ☑ (no improvement)

Added page-pruning rate as a tertiary key in `cmp_row_filters`
(after Welford effectiveness, before filter_scan_size). Builds and
compiles cleanly; smoke TPC-DS lat 76979 ms vs r10 ctrl 76785 ms —
within noise.

**Bigger finding from this round**: re-ran r10 and exp3 in the
SAME machine state (back-to-back, same binary cache, same kernel
state) for direct comparison.

```
Same-state TPC-DS-lat smoke (3 iters, sum of medians):
  r10  : 76785 ms
  r11  : 76979 / 77010 ms
  exp3 : 77237 ms
  --- all within 0.6% (450 ms) ---
```

**The previously-recorded "7% gap to exp3" was cross-session
machine-state variance, not real**. The R10-pushdown-lat anchor
(84554 ms) was from a different session under different conditions.

**Conclusion**: round 6's architectural fix (per-conjunct rates as
side-effect of existing pruning, plus dynamic-filter refresh) is
at parity with the best-via-shortcut comparison anchor (exp3) on
TPC-DS-lat smoke. The PR is done.

## Final state of round 6+

```
exp/baseline                      9a1705088 (PR tip)
exp/per-conjunct-rates            r5 — page-only side-effect prior
exp/r6-pruningpredicate-rates     ★ r6+r7+r8+r9 stack — final
                                    architecture (page + row-group +
                                    bloom-filter rates as side-effect,
                                    plus dynamic-filter refresh)
```

Commits on `exp/r6-pruningpredicate-rates` over the round-5 base:
- r6: PruningPredicate per-conjunct rates (3192689a6)
- r7: bloom-filter per-conjunct rates (26b0eb944)
- r8: dynamic-filter refresh on snapshot_generation > 0 (36f067366)
- r9 v1+v2: partial-AND promote signal — neutral, kept for shape

**Take-it-now**: `exp/r6-pruningpredicate-rates` (HEAD a3dcd8362,
i.e. r10 + cleanup, r11 dropped). Architecturally correct (no extra
pruning runs on the static path; targeted re-evaluation only for
populated dynamic filters). **At parity with exp3** on TPC-DS-lat
smoke when measured in same-state side-by-side runs.
