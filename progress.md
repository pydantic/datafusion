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

## Anchor numbers (perconj baseline)

```
Workload          main_off  PR_on    exp3     perconj  perconj/exp3
ClickBench no-lat   21020   17190    17130    17839    1.041× (no page idx in data)
TPC-DS    no-lat    17003   18056    18260    18226    0.998× (matches)
TPC-H     no-lat      780    1128      941      990    1.052×
ClickBench lat      86562   89044    88482    89330    1.010×
TPC-DS    lat       76418   88026    78910    87813    1.113× ← biggest gap to close
TPC-H     lat       23723   24684    23956    24195    1.010×
```

The TPC-DS-lat 11% gap vs exp3 is the immediate target. exp3 closes it via
per-conjunct re-evaluation of `PruningPredicate::prune` (the row-group
prior); we want to recover that behavior without an "extra pruning run".

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
1. ☐ Look at `PruningPredicate::try_new` / `prune` internals
2. ☐ Add per-conjunct prebuilt sub-predicates field
3. ☐ Add `prune_per_conjunct(stats) -> (Vec<bool>, Vec<PerConjunctStats>)`
4. ☐ Wire opener.rs row-group pruning to capture rates
5. ☐ Merge with existing page-pruning rates in `partition_filters`
6. ☐ Build, smoke vs perconj baseline
7. ☐ Full TPC-DS-lat to verify gap closure
