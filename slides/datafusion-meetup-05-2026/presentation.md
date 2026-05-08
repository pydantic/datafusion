---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    font-size: 24px;
    padding: 28px 60px 28px 60px;
  }
  h1 { color: #1f3b57; margin: 0 0 8px 0; font-size: 34px; }
  h2 { color: #1f3b57; }
  .small { font-size: 18px; color: #666; }
  .lede { font-size: 32px; line-height: 1.35; }
  .highlight { color: #2e86c1; font-weight: 600; }
  .warn { color: #d4504e; font-weight: 600; }
  table { font-size: 22px; margin: 0 auto; }
  section.lead h1 { font-size: 56px; }
  img[alt~="center"] { display: block; margin: 0 auto; }
  .takeaway { font-size: 19px; margin-top: 8px; line-height: 1.35; }
  .takeaway p { margin: 4px 0; }
---

<!-- _class: lead -->

# Adaptive filter scheduling
# in the Parquet decoder

<br>

`pushdown_filters=on` shouldn't be a per-query lottery.

<br>
<br>

<span class="small">Adrian Garcia Badaracco · DataFusion · May 2026</span>

<!--
Slide 1 — title (~15s):

- pushdown_filters today: per-query lottery
- this PR: removes the lottery at the decoder level
- takeaway: never slower than main_on, within a few % of main_off
-->

---

# `pushdown_filters=on` is a per-query lottery

<br>

| Mode | Decode pattern per row group | Cost when filter is unselective |
|---|---|---|
| `pushdown=off` | decode all projected cols → one coalesced read, one big mask post-decode | none — filter is a `FilterExec` after the scan |
| `pushdown=on` | **decode filter cols first → evaluate predicate → build `RowSelection` → skip pages / decode only surviving rows for the projection** | per-batch ArrowPredicate eval cost; extra I/O for any filter col **not in the projection**; possible double-decode of filter cols **also in the projection** |

<br>

The community has wanted `pushdown_filters` on by default for years
([#3463](https://github.com/apache/datafusion/issues/3463)) — but you can't until
the lottery is fixed ([#20324](https://github.com/apache/datafusion/issues/20324)).

<!--
Slide 2 — the lottery (~60s):

- wins when: row-level eval → sparse RowSelection → page-skipping → late materialization (nothing to do with RG-stats pruning, that's separate)
- loses when: filter mandatory but unselective; or filter col overlaps projection → 2-stage decode + extra IO
- on object storage (20–200ms RTT) the loss case is catastrophic
- #3463 (open since 2022): "pushdown_filters on by default" — blocked by exactly this lottery
-->

---

# The solution: per-filter, adaptive, in-decoder

<br>

```
                ┌──────────┐                 per filter:
                │   New    │                   - rows seen / matched
                └─────┬────┘                   - eval time (ns)
                      │  initial placement     - bytes seen
                      │  from byte ratio       - Welford running stats
        ┌─────────────┴─────────────┐
        ▼                           ▼        decision metric:
  ┌──────────┐  promote   ┌──────────────┐     scatter-aware
  │ PostScan │ ◄────────► │  RowFilter   │     bytes-saved-per-second
  └────┬─────┘   demote   └──────┬───────┘     with one-sided CI
       │                         │
       └────► Dropped ◄──────────┘            (Dropped only for
                                              OptionalFilterPhysicalExpr)
```

<span class="highlight">Decoder swaps strategy at every row-group boundary</span> — same `ParquetPushDecoder`, same `BoxStream`, fresh `RowFilter`. `PushBuffers` carries through (already-fetched bytes that survive the swap are reused).

<!--
Slide 3 — solution (~60s):

- each conjunct gets a FilterId; SelectivityTracker per ParquetSource keeps Welford stats
- decision metric: scatter-aware bytes-saved-per-second (counts only sub-batch windows the filter empties)
- one-sided CI (z=2.0) gates promote/demote — no yo-yoing on noisy samples
- arrow-rs companion: can_swap_strategy / swap_strategy at RG boundaries; PushBuffers reused
- OptionalFilterPhysicalExpr (hash-join dyn filters) can be dropped when CPU-dominated
-->

---

# ClickBench partitioned · SSD

![w:60% center](img/clickbench_nolat.png)

<div class="takeaway">

**`PR_on` 17.2 s — 18 % faster than `main_off`, 21 % faster than `main_on`** in aggregate.
**Q23**: `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`. RG stats can't help; the win is **row-level eval → sparse `RowSelection` → page-skipping (late materialization)**.

</div>

<!--
Slide 4 — ClickBench SSD (~60s):

- aggregate: PR 17.2s vs main_off 21.0s vs main_on 21.7s (pushdown=on actually loses on main!)
- Q23 = row-level pushdown poster child: SELECT * + LIKE '%google%'
- mechanism: row-level eval → sparse RowSelection → page-skipping; NOT RG-stats (LIKE has no min/max)
- 3.6s → 121ms (main_on) → 167ms (PR) — PR captures the win minus a small confidence_z warmup
-->

---

# TPC-DS SF1 · SSD — Q64 carries the day

![w:60% center](img/tpcds_nolat.png)

<div class="takeaway">

**Q64**: `main_on` 20.0 s → `PR_on` 0.65 s — 30× single-query speedup.

</div>

<!--
Slide 5 — TPC-DS SSD (~45s):

- pushdown=on regresses 2.29× on main (39.0s vs 17.0s); PR closes it to ~flat vs main_off
- Q64: stacked dynamic filter chain; main_on evaluates at row-level with placeholder before build finishes
- PR keeps it PostScan until the build publishes a useful predicate → 20s → 0.65s
- honest: CI bench's "1.80× total speedup" is essentially this one query
-->

---

# TPC-H SF1 · SSD — the unaddressed case

![w:52% center](img/tpch_nolat.png)

<div class="takeaway">

**`PR_on` 1.13 s — <span class="warn">+45 % vs `main_off`</span>.** Not a row-filter cost: even with `min_bytes_per_sec=∞` the **in-scan** path loses to `FilterExec` above `RepartitionExec`. **Problem B (partition skew)**, not A. Fix: row-group morselization ([#21766](https://github.com/apache/datafusion/pull/21766)).

</div>

<!--
Slide 6 — TPC-H SSD (~60s):

- this PR can't help here — the regression is structural, not filter-cost
- verified by setting min_bytes_per_sec=∞ (every filter PostScan); TPC-H still regresses
- main_off runs the filter as FilterExec *after* RepartitionExec → shuffle re-balances skew
- in-scan filter (pushdown=on or PR's PostScan in-opener) skips the shuffle → one heavy file = one slow partition
- TPC-H SF1 lineitem = one file, one row group, one partition gating the stage
- fix: row-group morselization (#21766, draft). Adaptive scheduling could be off entirely; TPC-H still looks this way
-->

---

# Switch from SSD to S3: the picture changes

![w:52% center](img/tpch_lat.png)

<div class="takeaway">

Same TPC-H, **on S3.** `main_on` regresses **2.22×** (52.6 s vs 23.7 s). **`PR_on` 24.7 s — <span class="highlight">1.04× of `main_off` ≈ flat</span>; 0.47× of `main_on`.** The +45 % SSD loss collapses to +4 %.

</div>

<!--
Slide 7 — TPC-H S3 (~60s):

- "S3" = `--simulate-latency` (20-200ms per OS op); didn't hit real AWS, profile matches any cloud store
- big narrative shift: SSD benchmarks understate the cost of pushdown=on
- multipliers vs SSD: main_off 30×, main_on 53×, PR 22×
- pushdown=on issues many small IOs and pays per-RTT for each → blows up under latency
- PR ties main_off within 4%; 2.13× faster than main_on
- reframes previous slide: PR can't help on TPC-H *when I/O is free*; in any cloud deployment it does
-->

---

# What's missing / what's next

<br>

| Gap | Mechanism | Fix |
|---|---|---|
| **Sub-row-group adaptation** | swap point is row-group boundary | arrow-rs `ParquetRecordBatchReader::pause` returning residual `RowSelection` |
| **Cross-partition row-group balance** | file is the unit of distribution | row-group-level morselization → [PR #21766](https://github.com/apache/datafusion/pull/21766) (draft) |
| **Latency-aware `confidence_z`** | warmup cost per filter scales with RTT | infer initial placement from stats hit rate |

<br>

<!--
Slide 8 — what's next (~45s):

- sub-row-group pause/resume in arrow-rs → unlock single-row-group files (TPC-H without morsels)
- row-group morselization (#21766) → fix for Problem B / partition skew
- latency-aware confidence_z → close Q23-style warmup gaps; infer placement from RG-stats hit rate
- big picture: this PR (filter cost) + #21766 (data skew) = the unlock to flip #3463 (pushdown=on by default, open since 2022)
- thanks; questions?
-->
