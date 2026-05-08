# Investigation: TPCH Q18 hangs with dynamic filter pushdown

## Context

- Issue: [#21625](https://github.com/apache/datafusion/issues/21625)
- Symptom: TPCH Q18 hangs indefinitely when run with `DATAFUSION_EXECUTION_TARGET_PARTITIONS=24` (or any value ≥ ~12) *and* `datafusion.optimizer.enable_dynamic_filter_pushdown=true` (the default). Setting that option to `false` runs the query in ~220 ms.
- `git bisect` blames commit `6c5e241e62` ("Skip probe-side consumption when hash join build side is empty", #21068). That PR added an empty-build short-circuit to `HashJoinStream::state_after_build_ready` that transitions straight to `Completed` when the join type guarantees an empty result. But the short-circuit only *exposes* the bug — the real deadlock mechanism is older and lives in the hash-join ↔ RepartitionExec interaction.
- Existing fix attempts:
  - PR #21632 (merged as `1f5644dcbe` on branch `fix-hash-join-dynamic-filter-deadlock`) disables the short-circuit whenever `build_accumulator.is_some()`. It unhangs Q18 but gives up the #21068 optimization whenever dynamic filter pushdown is on.
  - PR #21641 (this PR, branch `remove-barrier`) replaces the `tokio::sync::Barrier` in `SharedBuildAccumulator` with an `AtomicUsize` + leader-election. It unhangs Q18 and restores the #21068 optimization, but causes a **60× regression** on the hash-join dynamic filter benchmark from the [DataFusion dynamic filters blog post](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/#hash-join-dynamic-filters) because probing starts before the leader publishes the filter.

We need to understand why the barrier in `SharedBuildAccumulator` causes a deadlock with `RepartitionExec`'s `distributor_channels` global gate, so we can fix the *real* bug without regressing either Q18 correctness or the scan-level filter optimization.

## What we know about the Q18 failure

### The plan

Q18 (with `prefer_hash_join=true`, 24 target partitions) produces a plan with three stacked `HashJoinExec` nodes in Partitioned mode:

1. **HashJoin 1** `customer ⋈ orders` (Inner) — build = customer, probe = orders.
2. **HashJoin 2** `(customer⋈orders) ⋈ lineitem` (Inner) — build = HashJoin 1 output, probe = lineitem.
3. **HashJoin 3** `filter(agg(lineitem)) RightSemi (HashJoin 2 output)` — build = `FilterExec → FinalAgg(FinalPartitioned) → RepartitionExec(Hash(l_orderkey), 24→24) → PartialAgg → lineitem-scan-L`, probe = HashJoin 2 output.

All three hash joins install a `SharedBuildAccumulator` and push a `DynamicFilterPhysicalExpr` into the same `lineitem` scans. The lineitem scan that feeds HashJoin 3's *build* has three stacked dynamic filter predicates on it (one from each HashJoin).

### Instrumented trace (on `main`, with the `tokio::sync::Barrier`)

Probes added:
- `SharedBuildAccumulator::report_build_data` entry, pre-barrier, post-barrier
- `HashJoinStream::collect_build_side` ENTRY, PENDING-on-left_fut, and "done" (after `ready!(left_fut.get_shared(cx))`)
- `HashJoinExec::execute` per (partition, mode, accumulator-is-some)

Observations at 24 partitions:
- **All 72 streams are created** — `HashJoinExec::execute` is called 3 × 24 times.
- **HashJoin 1 and HashJoin 2 fully release their barriers.** 24 `PRE-BARRIER` and 24 `POST-BARRIER` events for each accumulator. Filters finalized.
- **HashJoin 3's barrier has 21/24 arrivals** and 0 releases. Partitions **2, 14, 21** are missing.
- **Those same 3 partitions never reach `collect_build_side`'s "done" print.** `collect_build_side` is polled 30-45 times per missing partition (less than the ~60-120 polls per finishing partition), and each poll enters the function, hits `ready!(left_fut.get_shared(cx))`, and returns `Pending`.
- **Partitions 2, 14, 21 had `is_empty=true` for HashJoin 1 in the same run**, i.e. those exact partition IDs are the ones that produced an empty build in the earliest accumulator. (The subquery result has ~57 distinct `l_orderkey` values; hashed into 24 buckets, 3 end up with zero rows — consistently partitions 2, 14, 21 given the seeded hash.)

### Minimal non-blocking fix that *does* work

Replacing `self.barrier.wait().await.is_leader()` with a synchronous `self.remaining.fetch_sub(1, AcqRel) == 1` (AtomicUsize counter, leader is whoever brings it to zero, no waiting) unhangs Q18. The 21 partitions that used to park no longer park, and as a side effect the 3 stuck partitions drain their (empty) upstream and proceed. Verified end-to-end: `57 rows in 131 ms` (release build) on the exact reproducer from #21625.

### Why this is a workaround, not a fix

- From code inspection, the `distributor_channels` global gate at `datafusion/physical-plan/src/repartition/distributor_channels.rs` *should* allow the 3 empty-bucket channels to stay drainable. An empty channel contributes `+1` to `empty_channels`; with 3 structurally-empty channels in the 24-output RepartitionExec, `empty_channels ≥ 3` should hold forever, so the gate should never be closed and the input task should never park on `send()`.
- Yet empirically, parking 21 hash-join streams on the barrier stalls the other 3 streams' upstream draining. Un-parking them frees things.
- The fact that removing the barrier also causes a **60× regression** on an unrelated benchmark (the blog post's `small_table JOIN large_table` query: ~2 ms on `main` vs ~120 ms on `remove-barrier`; `bytes_scanned=174.9 K` vs `197.3 M`; `row_groups_pruned_statistics=999` vs `333`) tells us we are *papering over* a latent bug, not fixing it.

## The perf regression from removing the barrier

Benchmark (from the blog post):

```sql
COPY (SELECT i AS k, i AS v FROM generate_series(1, 10000) t(i)) TO 'small_table.parquet';
COPY (SELECT i AS k FROM generate_series(1, 100000000) t(i))      TO 'large_table.parquet';
-- large_table has max_row_group_size=100000, so 1000 row groups total.
SELECT count(*)
FROM   small_table JOIN large_table ON small_table.k = large_table.k
WHERE  small_table.v >= 50;
```

Both binaries in release mode, same parquet files, alternating runs, 10 measured iterations each:

| Metric | `main` (barrier) | `remove-barrier` (counter) | Ratio |
|---|---|---|---|
| Median latency | ~2 ms | ~120 ms | **60×** slower |
| Range | 2 – 9 ms | 77 – 163 ms | |
| `bytes_scanned` (large_table) | 174.9 K | 197.3 M | **1128×** |
| `row_groups_pruned_statistics` (of 1000) | 999 | 333 | ~3× less effective |
| Probe-side `output_rows` | 20.48 K | 66.62 M | 3252× |
| `time_elapsed_scanning_total` | 1.02 ms | 643.87 ms | 631× |
| `scan_efficiency_ratio` | 0.06% | 66.58% | |

The scan-level row-group pruning is what drives the 60× gap. On `main`, the barrier ensures the `DynamicFilterPhysicalExpr` has been `update()`'d to the finalized `k ≥ 50 AND k ≤ 10000 AND hash_lookup(...)` expression *before* any probe partition starts polling its scan, so Parquet row-group statistics can prune 999 / 1000 row groups. On the branch, probing starts against the placeholder `lit(true)` filter and most row groups get fully read before the leader publishes the real filter.

## Open questions (what the sub-agents should chase)

1. **Why do HashJoin 3 partitions 2/14/21 stall in `left_fut`?** Their upstream pipeline is `FilterExec → FinalAgg → RepartitionExec(Hash) → PartialAgg → lineitem-scan`. Something in that chain is not making progress while the other 21 HashJoin 3 streams are parked on the barrier. Specifically: *which future is parked*, *who is supposed to wake it*, and *why is the wake never delivered*?
2. **Is it `distributor_channels`'s global gate?** The gate's `empty_channels` counter and `send_wakers` list are the obvious candidates. Drop-semantics for `DistributionReceiver` with non-empty residual data look suspicious: the drop only calls `wake_channel_senders(self.channel.id)`, which wakes senders parked on *this specific channel id*. Senders parked on the *global* gate (empty_channels == 0) are not woken by a drop. Is there a scenario where dropping a non-empty channel should have freed a gate-parked sender, but doesn't?
3. **Is it a waker-registration race between `SendFuture::poll` and `DistributionReceiver::drop`?** The send future parks on both the gate and the channel. The receiver drop wakes the channel-specific senders. Is there a window where a sender registers on the gate, and then the receiver drops and `wake_channel_senders` fires before the sender's gate waker is registered?
4. **Does `OnceFut::get_shared` + `shared()` interact badly with tokio scheduling when 24 consumers hit it at once?** The build-side future is wrapped in `OnceFut`, which uses `futures::future::Shared`. All 24 HashJoin 3 build-side streams are driving the *same* `OnceFut` for their own partition's `collect_left_input` (well, each has its own `OnceFut` — but the underlying stream pipeline shares upstream tasks). Could there be a waker-lost scenario with `Shared` + multi-poller?
5. **Is the barrier parking fundamentally incompatible with holding `BuildSideReadyState { left_data: Arc<JoinLeftData> }`?** The 21 parked partitions each hold an `Arc<JoinLeftData>` containing the hash table. Does anything in that state keep upstream tasks alive?
6. **Does the failure need all three HashJoin stages, or is it reproducible with a simpler stacked plan?** Specifically: does the failure need a `RepartitionExec` that produces structurally-empty hash buckets, feeding a Partitioned `HashJoinExec` whose accumulator has at least one participant from each of `N=24` partitions? If we can reproduce with 2 partitions (1 empty, 1 non-empty) at tiny scale, the bug hunt becomes much easier.

## What's been ruled out

- `DynamicFilterPhysicalExpr::wait_complete` / `wait_update` are not called from any production code path (only from tests and the `topk` module). The scan does *not* block on filter completion. So the circular-filter-dependency theory is wrong — the scan is happy to read with the placeholder filter in place.
- The `#21068` short-circuit (`state_after_build_ready` → `Completed`) is *not* what's breaking things directly. PR #21632 already forces every partition to report before taking the short-circuit, and Q18 still hangs. The `remove-barrier` branch also restores the short-circuit and works, which means the short-circuit is innocent once the barrier is out of the picture.
- Cargo cache / OS page cache differences: I verified by alternating binaries on the same files in the same shell session. The 60× benchmark difference is real and reproducible.
- `OnceLock<Arc<SharedBuildAccumulator>>` reuse across query iterations: each `HashJoinExec::execute` allocation is fresh per query, so no state carries over.

## Data points for sub-agents

- **Trace dump (21/24, instrumented main):** `grep -c "ENTER report" trace.txt` = 69 (24 + 24 + 21). HashJoin 1 and 2 fully release; HashJoin 3 has 21 ENTER / 21 PRE-BARRIER / 0 POST-BARRIER.
- **Stuck partitions:** IDs 2, 14, 21 consistently across runs, matching the partitions that had `is_empty=true` in HashJoin 1.
- **Bytes scanned difference:** 174.9 K → 197.3 M (the finalized filter prunes row groups via statistics on `main`; the placeholder does not on the branch).
- **Key file paths:**
  - `datafusion/physical-plan/src/joins/hash_join/shared_bounds.rs` (accumulator)
  - `datafusion/physical-plan/src/joins/hash_join/stream.rs` (`collect_build_side`, `wait_for_partition_bounds_report`)
  - `datafusion/physical-plan/src/joins/hash_join/exec.rs` (`collect_left_input`, `execute()`)
  - `datafusion/physical-plan/src/repartition/mod.rs` (input task loop, `PerPartitionStream`)
  - `datafusion/physical-plan/src/repartition/distributor_channels.rs` (**prime suspect** — `Gate`, `SendFuture::poll`, `RecvFuture::poll`, `Drop for DistributionReceiver`, `wake_channel_senders`, `decr_empty_channels`)
  - `datafusion/physical-plan/src/joins/utils.rs` (`OnceFut::get_shared`)

## Sub-agent findings (first round)

### Agent 1 — `distributor_channels` gate/waker audit

Points at a race in `Drop for DistributionReceiver` at `datafusion/physical-plan/src/repartition/distributor_channels.rs:281-287`: the drop only decrements `empty_channels` when the channel was empty, and only wakes senders keyed by *its own* channel id (`wake_channel_senders(self.channel.id)`). The concrete deadlock they construct:

1. Sender for channel A polls, sees `empty_channels == 0`, parks in `send_wakers` with `channel_id = A` (line 229).
2. Receiver for channel B is dropped with non-empty data.
3. Drop of B leaves `empty_channels` unchanged (line 281 condition is false), only calls `wake_channel_senders(B)` (line 287), which filters by channel id (line 442) and does not touch the sender parked with `channel_id = A`.
4. Sender for A stays parked forever.

**This is suspicious, but does not cleanly match Q18.** The construction above requires `empty_channels == 0` at the moment the sender parked. With 3 structurally-empty channels (partitions 2/14/21 get zero rows all run), `empty_channels` should stay ≥ 3 and no sender should ever park globally. Some other mechanism is needed to bring `empty_channels` transiently to 0 — or the 3 channels are *not* actually structurally empty.

### Agent 2 — minimal reproducer attempts (negative result)

Tried 5 synthetic SQL shapes against a fresh `datafusion-cli` on `main` with `DATAFUSION_EXECUTION_TARGET_PARTITIONS=24`:

| Recipe | Hung? | Time |
|---|---|---|
| Single hash join, `small(24) ⋈ big(100k)` | No | 22 ms |
| Two stacked hash joins | No | 3 ms |
| Three stacked hash joins | No | 1 ms |
| Three joins + `FinalPartitioned` aggregate | No | 7 ms |
| `RightSemi` hash join | No | 1 ms |

**None hung.** The Q18 hang needs something more specific than the structural "three stacked hash joins with empty hash buckets on one build side" recipe. Likely candidates: (a) real Parquet scans (lazy file reads, async I/O, different backpressure behavior than `TestMemoryExec`), (b) the specific size distribution that makes row-group stats pruning interact with the dynamic filter, (c) the `FilterExec → FinalAgg → RepartitionExec → PartialAgg → scan` depth that Q18 has in HashJoin 3's build side.

### Agent 3 — ruled-out hypotheses

Confirmed via code reading:
- `futures::future::Shared` (v0.3.32) has no known waker-lost bug.
- `collect_left_input`'s `try_fold` + synchronous `state.reservation.try_grow` does not drop wakers.
- `HashJoinExec::execute` at `exec.rs:1341` really does call `self.left.execute(partition, ctx)` per-partition in `Partitioned` mode — no cross-partition sharing of upstream streams.
- `PerPartitionStream`'s `recv_wakers: Option<Vec<Waker>>` is per-channel; no cross-channel sharing.
- `datafusion-cli` uses `#[tokio::main]` → multi-thread runtime, not `current_thread`. No single-threaded starvation story.
- `tokio::sync::Barrier::wait` uses its own private `watch` channel internally and does not touch any other tokio primitive's waker list. No cross-primitive interference.

### What I verified directly

- `RepartitionExec::pull_from_input` is spawned as its own tokio task via `SpawnedTask::spawn` at `datafusion/physical-plan/src/repartition/mod.rs:391`. On the multi-thread runtime this means parked hash-join tasks cannot directly starve the input task via worker-thread exhaustion. The input task runs on its own worker.

## Still-open questions after round 1

1. **Does `empty_channels` actually reach 0 during a hanging Q18 run on `main`, despite 3 channels being structurally empty?** This is the decisive experiment. If yes → Agent 1's waker-race story applies. If no → the bug is somewhere else in the gate state machine (or not in the gate at all).
2. **Where is `pull_from_input` actually parked at the hang?** On `send().await` → which channel? On `stream.poll_next().await` → which upstream operator? Or somewhere else (e.g. `batch_partitioner` initialization, memory reservation)? A runtime log of task state would tell us.
3. **Is the Q18 trigger reproducible in memory at all?** Agent 2 couldn't reproduce with `TestMemoryExec`. Is it the async-I/O nature of the Parquet scan, or the specific data statistics? A `MemoryExec` with async artifical delays might reproduce it if timing is the trigger.
4. **Agent 1's race: even if it doesn't apply to this exact Q18 scenario, is it a latent bug worth fixing on its own?** If a gate-parked sender for A can be stranded by a drop on B, that's a bug regardless of whether Q18 triggers it.

## Sub-agent findings (round 2) — the decisive experiment

### Agent 4 — instrumented `distributor_channels.rs` on `main`, traced a hanging Q18

**Setup:** Added `DISTCHAN_TRACE`-gated `eprintln!` probes at every gate state transition (`decr_empty_channels`, `RecvFuture::poll fetch_add`, `SendFuture::poll PARK`, `SendFuture::poll OK`, `Drop for DistributionSender`, `Drop for DistributionReceiver`, `wake_channel_senders`). Built debug, ran Q18 with `DATAFUSION_EXECUTION_TARGET_PARTITIONS=24` + `gtimeout 30`, captured 27,306 events across 4 `Gate` instances (one per `RepartitionExec` in the plan).

**Headline result:** The `distributor_channels` gate is **innocent**.

- For **every** one of the 4 gates, by the moment of the hang **every** input channel has reached `SENDER DROP ... remaining_senders=0`. That is, all 4 `pull_from_input` tasks ran to completion and dropped their last senders.
- **No `SendFuture` is parked anywhere** at hang time. `send_wakers` is empty on every gate. SEND PARK events appear earlier in the trace but are all followed by matching SEND OK or SENDER DROP — every parking was resolved.
- `empty_channels` is **not stuck at 0 with a sender waiting on top of it**. The gate did its job. The decisive question from round 1 gets a definitive **no**.
- `RECV DROP` events only appear at process teardown (from the SIGTERM after `gtimeout`). Before the hang, all receivers are still alive.

**Interpretation.** Since the last sender on every channel has dropped, a freshly polled `RecvFuture` would observe `data.is_empty()` and `recv_wakers.is_none()` (the sender-drop path `take()`s the waker list) and return `Poll::Ready(None)`. So the stuck hash-join streams' `PerPartitionStream::recv()` calls would unstick themselves *if they were re-polled*. They are **not re-polled** after the sender drops.

Therefore the deadlock mechanism is one of:

1. **A missed wake between `Drop for DistributionSender` and its corresponding `PerPartitionStream::poll_next` waker registration.** If the last-sender drop runs before the receiver-side `RecvFuture::poll` has pushed its waker into `state.recv_wakers`, then the drop's "wake everyone in the list" call finds an empty list, wakes no one, and the subsequent `poll` would… well, it should see the closed channel and return `Ready(None)`. Unless the `poll` never happens.

2. **The waker that *was* registered is for a task that has since been cancelled / suspended / moved**, so `Waker::wake()` silently no-ops. For example if `collect_left_input`'s `try_fold` is polled under a context whose waker rotates between re-polls, and the old waker in `recv_wakers` is stale by the time sender-drop fires.

3. **`OnceFut::get_shared`'s `Shared<F>` wrapper is holding the receiver-side task and not rescheduling it** even though the inner waker fired. This is less likely (Agent 3 already ruled out known `Shared` bugs) but still in scope because the waker crosses a `Shared` boundary.

4. **Something else entirely** — e.g. `PerPartitionStream::poll_next_and_coalesce` has an early-return path that transitions out of `ReadingMemory` without re-polling the channel after a wake, stranding the task.

**Next experiment (unfinished):** instrument `PerPartitionStream::poll_next` in `datafusion/physical-plan/src/repartition/mod.rs` (around line 1600-1750), the `DistributionSender::drop` wake loop, and the `OnceFut::get_shared` consumer chain in `collect_build_side`. Specifically we need, for the 3 stuck HashJoin 3 build partitions:
- Did `PerPartitionStream::poll_next` ever see the `None` sentinel for the final sender drop?
- How many recv wakers were in the list at the moment of sender drop? Were the wakes delivered to live tasks or stale ones?
- Does the `HashJoinStream` task's waker chain survive the `Shared<collect_left_input>` boundary?

## Conclusion so far

The original `remove-barrier` fix is a workaround for a latent bug that lives on the **receiver side** of `RepartitionExec` (probably in the `DistributionSender::drop` → waker chain → `PerPartitionStream::poll_next` path), not in the `distributor_channels` `Gate` as initially hypothesized. The fact that removing the barrier fixes Q18 is explained by: when the barrier parks 21 tasks, those tasks' build-side receivers stay *alive* (`BuildSideReadyState` holds them indirectly through the hash table), and somehow this changes the scheduler's polling pattern enough that the 3 stuck partitions' wakes are ignored. When the 21 don't park, the scheduler polls the 3 stuck tasks often enough that they observe their channels as closed and return `Ready(None)`.

The **60× perf regression** on the blog benchmark is the price of papering over the latent bug. A proper fix is to find and fix the receiver-side missed wake, then restore the barrier (or an equivalent synchronous rendezvous) in `SharedBuildAccumulator`. That is the follow-up work to do before landing this PR.

## Reproducer

```bash
# Requires TPCH SF1 data at /Users/adrian/GitHub/datafusion/benchmarks/data/tpch_sf1/
cat > /tmp/tpch_q18.sql <<'EOF'
CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '/Users/adrian/GitHub/datafusion/benchmarks/data/tpch_sf1/lineitem';
CREATE EXTERNAL TABLE orders   STORED AS PARQUET LOCATION '/Users/adrian/GitHub/datafusion/benchmarks/data/tpch_sf1/orders';
CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '/Users/adrian/GitHub/datafusion/benchmarks/data/tpch_sf1/customer';
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
)
AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 10;
EOF

# On main: hangs forever
DATAFUSION_EXECUTION_TARGET_PARTITIONS=24 gtimeout 60 ./target/release/datafusion-cli -f /tmp/tpch_q18.sql

# On main with dynamic filter pushdown disabled: ~220 ms
DATAFUSION_EXECUTION_TARGET_PARTITIONS=24 ./target/release/datafusion-cli -c "\
  SET datafusion.optimizer.enable_dynamic_filter_pushdown = false; \
  SELECT ...;"
```
