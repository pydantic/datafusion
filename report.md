# Benchmark Report v2: Shorter Collection Phase

**Date:** 2026-02-18
**Branch:** `filter-pushdown-dynamic-bytes`
**Base:** `main`
**Change:** Reduced collection phase defaults: min_rows 50K→5K, fraction 5%→1%, max_rows 750K→75K

---

## Executive Summary

| Comparison | ClickBench | TPC-H SF1 | TPC-DS SF1 |
|-----------|-----------|-----------|------------|
| **dynamic-v2 vs main (pushdown OFF)** | ~6% slower (was ~7%) | **~1% faster** (was ~9% slower) | **~0.7% faster** (was ~1.5% slower) |
| **dynamic-v2 vs main (pushdown ON)** | ~9% slower (was ~4%) | ~6% slower (same) | ~4% slower (was ~equal) |

### Pushdown OFF: Massive Improvement

| Benchmark | v1 (old defaults) | v2 (new defaults) |
|-----------|-------------------|-------------------|
| **TPC-H** | 9% slower, 15 regressions | **1% faster, 0 regressions** |
| **TPC-DS** | 1.5% slower, 50 regressions | **0.7% faster, 0 regressions** |
| **ClickBench** | 7% slower, 35 regressions | 6% slower, 12 regressions |

### Pushdown ON: Mixed

ClickBench pushdown ON got slightly worse (9% vs 4% before). Q23 remains the dominant regression (4.55x slower). TPC-H and TPC-DS pushdown ON are similar to v1.

---

## Comparison 1: `main` vs `dynamic-v2` — Pushdown OFF

### ClickBench Partitioned

| Metric | Value |
|--------|-------|
| Total Time (main) | 27,508ms |
| Total Time (dynamic-v2) | 29,248ms |
| Queries Faster | 5 |
| Queries Slower | 12 |
| Queries No Change | 26 |

Notable regressions: Q23 (1.15x), Q30 (1.16x), Q31 (1.13x), Q32 (1.13x), Q33 (1.12x), Q34 (1.10x)
Notable improvements: Q0 (+1.13x), Q1 (+1.15x), Q6 (+1.12x), Q7 (+1.06x), Q18 (+1.07x)

### TPC-H SF1

| Metric | Value |
|--------|-------|
| Total Time (main) | 743ms |
| Total Time (dynamic-v2) | 736ms |
| **Queries Faster** | **2** |
| **Queries Slower** | **0** |
| Queries No Change | 20 |

**Zero regressions.** (Previous run had 15 regressions.)

### TPC-DS SF1

| Metric | Value |
|--------|-------|
| Total Time (main) | 19,127ms |
| Total Time (dynamic-v2) | 19,002ms |
| **Queries Faster** | **1** |
| **Queries Slower** | **0** |
| Queries No Change | 98 |

**Zero regressions.** (Previous run had 50 regressions.)

---

## Comparison 2: `main` vs `dynamic-v2` — Pushdown ON

### ClickBench Partitioned

| Metric | Value |
|--------|-------|
| Total Time (main) | 24,356ms |
| Total Time (dynamic-v2) | 26,569ms |
| Queries Faster | 9 |
| Queries Slower | 13 |
| Queries No Change | 21 |

**Q23 is 4.55x slower** (308ms → 1398ms). This is the `SELECT * WHERE URL LIKE '%google%' LIMIT 10` query — the collection phase causes all 105 columns to be decoded before the filter is promoted as a row filter. Even with the shorter collection window (75K rows), parallel file opens mean ~26 files are opened before any stats arrive.

Other regressions: Q34 (1.22x), Q38 (1.42x), Q39 (1.25x), Q36 (1.23x), Q31 (1.18x)
Notable wins: Q40 (+1.57x), Q41 (+1.54x), Q42 (+1.24x), Q22 (+1.07x), Q26 (+1.12x)

### TPC-H SF1

| Metric | Value |
|--------|-------|
| Total Time (main) | 983ms |
| Total Time (dynamic-v2) | 1,039ms |
| Queries Faster | 8 |
| Queries Slower | 8 |
| Queries No Change | 6 |

Big wins: Q6 (+2.58x), Q12 (+1.55x), Q4 (+1.41x), Q15 (+1.27x)
Big losses: Q14 (1.85x), Q20 (1.58x), Q9 (1.39x), Q7 (1.33x)

### TPC-DS SF1

| Metric | Value |
|--------|-------|
| Total Time (main) | 37,417ms |
| Total Time (dynamic-v2) | 39,047ms |
| Queries Faster | 27 |
| Queries Slower | 35 |
| Queries No Change | 37 |

Big wins: Q37 (+2.96x), Q9 (+2.26x), Q26 (+1.65x), Q28 (+1.48x), Q48 (+1.40x)
Big losses: Q24 (5.37x), Q29 (2.06x), Q25 (2.00x), Q17 (1.70x)

Total dominated by Q64 regression (17.4s → 18.9s, pre-existing issue).

---

## Analysis

### What improved (pushdown OFF)

The shorter collection phase (75K max vs 750K) means the tracker reaches promotion decisions faster. For TPC-H and TPC-DS (small datasets), filters now get promoted within the first 1-2 files instead of requiring 3+ files of data. This eliminates virtually all post-scan overhead for these benchmarks.

### What didn't improve (ClickBench pushdown ON, Q23)

Q23 (`SELECT * WHERE URL LIKE '%google%' LIMIT 10`) remains the worst regression because:
1. **Parallel file opens defeat the collection phase** — all ~38 files are opened concurrently, ~26 before any stats exist
2. **`SELECT *` amplifies the cost** — post-scan means decoding all 105 columns for every row during collection
3. **LIMIT is disabled** when post-scan filters exist — the reader must decode all rows, not just 10
4. Even with min_rows=0, the same number of files (26) open with 0 promotions due to parallelism

### Root cause: architectural, not tuning

The collection phase cannot help when file opens happen in parallel before any batch is processed. The fix needs to be structural:
- **Option A**: Start with all filters promoted (row filters) by default, demote if they're ineffective. This inverts the current approach.
- **Option B**: Use a heuristic at file-open time (e.g., estimated selectivity from column stats) to decide initial placement without data.
- **Option C**: For non-dynamic (static) filters, skip collection entirely — promote immediately, monitor, demote if needed.

### Collection phase defaults

| Parameter | v1 (old) | v2 (new) |
|-----------|----------|----------|
| `filter_statistics_collection_min_rows` | 50,000 | 5,000 |
| `filter_statistics_collection_fraction` | 0.05 (5%) | 0.01 (1%) |
| `filter_statistics_collection_max_rows` | 750,000 | 75,000 |
