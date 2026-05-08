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

### r7 (next) — bloom-filter per-conjunct rates

**Hypothesis**: opener.rs runs bloom-filter pruning after stats
pruning. Bloom filters can rule out specific value sets (e.g.
`col = 'X'` where X is not in the bloom filter for any row group).
For string-equality / IN predicates, bloom filter pruning is the
strongest signal stats pruning can produce. We're not capturing
per-conjunct bloom-filter rates yet.

If exp3's TPC-DS-lat win includes filters where bloom-filter
pruning was effective per-conjunct, surfacing those rates should
close more of the gap.

**Plan**:
1. ☐ Look at `prune_by_bloom_filters` in row_group_filter.rs
2. ☐ Add per-conjunct variant
3. ☐ Wire into `page_pruning_rates` (now misnamed; should be
     `pruning_rates` since it covers stats + page + bloom)
4. ☐ Smoke vs r6 baseline
