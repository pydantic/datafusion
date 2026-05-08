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

### r8 (next) — refresh rates on snapshot_generation change

**Hypothesis** (strongest remaining gap source): exp3's
per-conjunct re-eval runs at EVERY `maybe_swap_strategy` call
(every row-group boundary). When a dynamic filter (placeholder
at file open) gets populated by the join build mid-stream,
exp3's next call evaluates the now-populated conjunct against
stats and gets a fresh rate. Our cached `page_pruning_rates` is
fixed at file open — placeholder rates stay None forever.

This explains TPC-DS Q25/Q26/Q50 where dynamic filters mature
mid-stream and exp3 wins.

**Plan**:
1. ☐ Track snapshot_generation per FilterId in tracker
2. ☐ In maybe_swap_strategy, detect generation changes
3. ☐ For changed FilterIds, build a fresh per-conjunct
     PruningPredicate against the current snapshot and evaluate
     against the file's row-group stats
4. ☐ Update page_pruning_rates entry for that FilterId
5. ☐ Smoke + full TPC-DS-lat to verify gap closure

**Honest cost note**: this IS extra work per maybe_swap_strategy
call (typically per row group boundary). For dynamic conjuncts
only — typically 2-3 per file. Each is microseconds. Not a "new
pruning run" in the sense of fetching new bytes; just re-evaluating
predicates against stats already in memory.
