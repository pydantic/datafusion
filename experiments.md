# Overnight experiments — adaptive filter scheduling follow-ups

Working from `report.md` §7's four directions. Tagged baseline:

```
exp/baseline  →  9a1705088  ("Revert: feat(parquet): coalesce post-scan-filtered batches")
```

All experiments branch off this tag. Each gets its own branch
`exp/<name>`. Successful changes get cherry-picked onto a
combined branch `exp/combined` for mix-and-match.

## Smoke-bench harness

To iterate quickly without paying full ClickBench/TPC-DS run cost,
use a targeted subset that hits known regression hotspots:

- **TPC-DS regression hotspots**: Q24, Q25, Q26, Q34, Q50, Q64
- **TPC-H regression hotspots (no-lat)**: Q5, Q7, Q8, Q9, Q17, Q18
- **ClickBench latency hotspots**: Q11, Q21, Q23, Q37, Q40, Q42

Smoke-bench scripts live in `/tmp/smoke_*.sh`. Iteration count: 3.

## Status

| # | Experiment | Branch | Status | Outcome |
|--:|------------|--------|:------:|---------|
| 1 | page-pruning prior              | `exp/page-pruning-prior` | pending | — |
| 2 | latency-aware confidence_z      | `exp/latency-aware-z`    | pending | — |
| 3 | (1) + (2) combined              | `exp/pp-plus-laz`        | pending | — |
| 4 | row-group morselization (#21766)| `exp/rg-morsels`         | pending | — |
| 5 | sub-RG adaptation (arrow-rs + DF)| `exp/sub-rg-adapt`      | pending | — |

Each row updated as work lands.
