## SharedBuildAccumulator Robustness Notes

Follow-up issues worth considering in `datafusion/physical-plan/src/joins/hash_join/shared_bounds.rs`.

### 1. `CollectLeft` finalization uses `==` instead of `>=`

`take_finalize_input_if_ready()` finalizes `CollectLeft` only when:

- `reported_count == expected_reports`

That is correct for the current control flow, but it is brittle. If a future change accidentally increments `reported_count` too many times, finalization would never trigger and the coordinator could wait forever.

Potential improvement:

- use `reported_count >= expected_reports`
- add a debug assertion that `reported_count` does not unexpectedly exceed `expected_reports`

### 2. `finish()` marks the dynamic filter complete before storing `Ready`

Current ordering in `finish()` is:

1. build filter result
2. `dynamic_filter.mark_complete()`
3. lock mutex
4. store `CompletionState::Ready(...)`
5. unlock
6. `notify_waiters()`

This creates a small window where external observers could see the dynamic filter as complete while the coordinator still reports `Finalizing`.

Potential improvement:

1. build filter result
2. lock mutex
3. store `CompletionState::Ready(...)`
4. unlock
5. `dynamic_filter.mark_complete()`
6. `notify_waiters()`

That keeps the coordinator's internal terminal state ahead of the external completion signal.

### 3. Add debug assertions for impossible state transitions

The current logic assumes several transitions never happen:

- no mutation after `CompletionState::Ready`
- `CollectLeft` never reaches `CanceledUnknown`
- finalization never sees `Pending`

Potential improvement:

- add `debug_assert!` checks in `store_build_data`, `store_canceled_partition`, and finalization paths to catch future regressions earlier

### 4. Panic during `build_filter()` can strand waiters in `Finalizing`

Normal `Result` errors are converted into `CompletionState::Ready(Err(...))`, but a panic inside `build_filter()` would bypass that and leave the coordinator in `Finalizing`.

Potential improvement:

- use a small guard / recovery pattern so entering finalization always ends in a terminal state, even if filter construction panics

### 5. Make post-completion cancellation reporting explicitly a no-op

`report_canceled_partition()` is effectively harmless after terminal completion because of the current state checks, but that behavior is implicit.

Potential improvement:

- add an early return when `completion != Pending` to make the intent obvious and reduce future ambiguity
