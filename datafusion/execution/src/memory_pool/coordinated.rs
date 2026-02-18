// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A coordinated memory pool with cooperative spilling and async waiting.
//!
//! Unlike [`GreedyMemoryPool`] and [`FairSpillPool`], this pool supports:
//! - **Cooperative spilling**: When a consumer needs memory and the pool is full,
//!   other consumers are signaled to spill via [`CoordinatedConsumer::should_spill`].
//! - **Async waiting**: Consumers can call [`CoordinatedConsumer::allocate`] to
//!   asynchronously wait for memory to become available instead of failing immediately.
//! - **Allocation type tracking**: Memory is classified as [`AllocationType::Required`]
//!   (cannot be reclaimed) or [`AllocationType::Spillable`] (can be freed by spilling).
//!
//! # Notification Flow
//!
//! ```text
//! Consumer A calls allocate() → pool full → enters FIFO waiter queue → returns Pending
//!
//! Consumer B checks should_spill() → queue non-empty → true → spills → frees allocation
//!                                                                          ↓
//!                          coordinator checks front of queue → enough memory? → wake front waiter
//!                                                                          ↓
//!                                                            Consumer A woken → succeeds
//! ```
//!
//! # Self-Deadlock Warning
//!
//! A consumer that holds spillable memory MUST spill before calling `allocate()`.
//! If a consumer holds spillable memory and calls `allocate()`, it may deadlock
//! waiting for itself to free memory that it cannot free because it is waiting.
//!
//! [`GreedyMemoryPool`]: super::GreedyMemoryPool
//! [`FairSpillPool`]: super::FairSpillPool

use datafusion_common::{Result, resources_datafusion_err};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::human_readable_size;

// ---------------------------------------------------------------------------
// AllocationType
// ---------------------------------------------------------------------------

/// Classifies a memory allocation's reclaimability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationType {
    /// Memory that cannot be reclaimed (e.g., hash tables during probe phase).
    Required,
    /// Memory that can be freed by spilling to disk.
    Spillable,
}

impl fmt::Display for AllocationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllocationType::Required => write!(f, "required"),
            AllocationType::Spillable => write!(f, "spillable"),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// A unique identifier for a waiter in the FIFO queue.
type WaiterId = u64;

struct WaitEntry {
    id: WaiterId,
    size: usize,
    #[allow(dead_code)] // stored for future diagnostics / per-type fairness policies
    alloc_type: AllocationType,
    waker: Waker,
}

struct CoordinatorState {
    used_required: usize,
    used_spillable: usize,
    waiter_queue: VecDeque<WaitEntry>,
    next_waiter_id: WaiterId,
}

impl CoordinatorState {
    fn used(&self) -> usize {
        self.used_required + self.used_spillable
    }

    fn available(&self, capacity: Option<usize>) -> usize {
        match capacity {
            Some(cap) => cap.saturating_sub(self.used()),
            None => usize::MAX,
        }
    }

    fn try_reserve(
        &mut self,
        size: usize,
        alloc_type: AllocationType,
        capacity: Option<usize>,
    ) -> bool {
        if size == 0 {
            return true;
        }
        let avail = self.available(capacity);
        if size <= avail {
            match alloc_type {
                AllocationType::Required => self.used_required += size,
                AllocationType::Spillable => self.used_spillable += size,
            }
            true
        } else {
            false
        }
    }

    fn release(&mut self, size: usize, alloc_type: AllocationType) {
        match alloc_type {
            AllocationType::Required => {
                self.used_required = self
                    .used_required
                    .checked_sub(size)
                    .expect("required memory underflow");
            }
            AllocationType::Spillable => {
                self.used_spillable = self
                    .used_spillable
                    .checked_sub(size)
                    .expect("spillable memory underflow");
            }
        }
    }
}

struct CoordinatorInner {
    capacity: Option<usize>, // None = unbounded
    state: Mutex<CoordinatorState>,
}

// ---------------------------------------------------------------------------
// MemoryCoordinator
// ---------------------------------------------------------------------------

/// A coordinated memory pool that supports cooperative spilling and async waiting.
///
/// Created via [`MemoryCoordinator::new_bounded`] or [`MemoryCoordinator::new_unbounded`].
/// Register consumers with [`MemoryCoordinator::register`].
#[derive(Clone)]
pub struct MemoryCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl fmt::Debug for MemoryCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.state.lock();
        f.debug_struct("MemoryCoordinator")
            .field("capacity", &self.inner.capacity)
            .field("used_required", &state.used_required)
            .field("used_spillable", &state.used_spillable)
            .field("num_waiters", &state.waiter_queue.len())
            .finish()
    }
}

impl MemoryCoordinator {
    /// Creates a bounded coordinator with the given capacity in bytes.
    pub fn new_bounded(capacity: usize) -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                capacity: Some(capacity),
                state: Mutex::new(CoordinatorState {
                    used_required: 0,
                    used_spillable: 0,
                    waiter_queue: VecDeque::new(),
                    next_waiter_id: 0,
                }),
            }),
        }
    }

    /// Creates an unbounded coordinator that never denies allocations.
    pub fn new_unbounded() -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                capacity: None,
                state: Mutex::new(CoordinatorState {
                    used_required: 0,
                    used_spillable: 0,
                    waiter_queue: VecDeque::new(),
                    next_waiter_id: 0,
                }),
            }),
        }
    }

    /// Registers a new consumer with this coordinator.
    pub fn register(&self, name: impl Into<String>) -> CoordinatedConsumer {
        CoordinatedConsumer {
            name: name.into(),
            coordinator: Arc::clone(&self.inner),
        }
    }

    /// Returns total memory currently in use (required + spillable).
    pub fn used(&self) -> usize {
        self.inner.state.lock().used()
    }

    /// Returns memory currently in use for required allocations.
    pub fn used_required(&self) -> usize {
        self.inner.state.lock().used_required
    }

    /// Returns memory currently in use for spillable allocations.
    pub fn used_spillable(&self) -> usize {
        self.inner.state.lock().used_spillable
    }

    /// Returns the amount of memory available for new allocations.
    pub fn available(&self) -> usize {
        let state = self.inner.state.lock();
        state.available(self.inner.capacity)
    }

    /// Returns the number of consumers currently waiting for memory.
    pub fn num_waiters(&self) -> usize {
        self.inner.state.lock().waiter_queue.len()
    }
}

// ---------------------------------------------------------------------------
// CoordinatedConsumer
// ---------------------------------------------------------------------------

/// A consumer registered with a [`MemoryCoordinator`].
///
/// Consumers create allocations via [`CoordinatedConsumer::try_allocate`] (immediate)
/// or [`CoordinatedConsumer::allocate`] (async, waits for memory).
///
/// Use [`CoordinatedConsumer::should_spill`] in processing loops to cooperatively
/// free memory when other consumers are waiting.
#[derive(Clone)]
pub struct CoordinatedConsumer {
    name: String,
    coordinator: Arc<CoordinatorInner>,
}

impl fmt::Debug for CoordinatedConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatedConsumer")
            .field("name", &self.name)
            .finish()
    }
}

impl CoordinatedConsumer {
    /// Returns the consumer's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new_empty(&self, alloc_type: AllocationType) -> MemoryAllocation {
        self.try_allocate(0, alloc_type).expect("always allocates")
    }

    /// Attempts to allocate `size` bytes immediately.
    ///
    /// Returns `ResourcesExhausted` if there is insufficient memory.
    pub fn try_allocate(
        &self,
        size: usize,
        alloc_type: AllocationType,
    ) -> Result<MemoryAllocation> {
        let mut state = self.coordinator.state.lock();
        if state.try_reserve(size, alloc_type, self.coordinator.capacity) {
            Ok(MemoryAllocation {
                coordinator: Arc::clone(&self.coordinator),
                consumer_name: Arc::from(self.name.as_str()),
                size,
                alloc_type,
            })
        } else {
            let available = state.available(self.coordinator.capacity);
            Err(resources_datafusion_err!(
                "Failed to allocate {} of {} memory for consumer '{}' - {} available (used: {} required + {} spillable = {} total, capacity: {})",
                human_readable_size(size),
                alloc_type,
                self.name,
                human_readable_size(available),
                human_readable_size(state.used_required),
                human_readable_size(state.used_spillable),
                human_readable_size(state.used()),
                self.coordinator
                    .capacity
                    .map_or("unbounded".to_string(), |c| human_readable_size(c))
            ))
        }
    }

    /// Returns a future that resolves to a [`MemoryAllocation`] once `size` bytes
    /// are available.
    ///
    /// If memory is available immediately, the future resolves on first poll.
    /// Otherwise, it enters a FIFO waiter queue and is woken when memory is freed.
    ///
    /// # Warning
    ///
    /// The calling consumer MUST spill its own spillable memory before awaiting
    /// this future. Failure to do so may cause deadlock.
    pub fn allocate(&self, size: usize, alloc_type: AllocationType) -> AllocateFuture {
        AllocateFuture {
            coordinator: Arc::clone(&self.coordinator),
            consumer_name: Arc::from(self.name.as_str()),
            size,
            alloc_type,
            waiter_id: None,
        }
    }

    /// Returns `true` if any consumer is waiting for memory.
    ///
    /// Consumers should check this in their processing loops and spill
    /// (free spillable allocations) when it returns `true`.
    pub fn should_spill(&self) -> bool {
        !self.coordinator.state.lock().waiter_queue.is_empty()
    }
}

// ---------------------------------------------------------------------------
// MemoryAllocation
// ---------------------------------------------------------------------------

/// An RAII handle to allocated memory. Memory is returned to the coordinator on drop.
pub struct MemoryAllocation {
    coordinator: Arc<CoordinatorInner>,
    consumer_name: Arc<str>,
    size: usize,
    alloc_type: AllocationType,
}

impl fmt::Debug for MemoryAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryAllocation")
            .field("size", &self.size)
            .field("alloc_type", &self.alloc_type)
            .finish()
    }
}

impl MemoryAllocation {
    /// Returns the current size of this allocation in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the allocation type.
    pub fn allocation_type(&self) -> AllocationType {
        self.alloc_type
    }

    /// Returns the name of the consumer of this allocation.
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    pub fn change_consumer(mut self, name: &str) -> Self {
        self.consumer_name = Arc::from(name);

        self
    }

    pub async fn grow(&mut self, additional: usize) -> Result<()> {
        let future = AllocateFuture {
            coordinator: Arc::clone(&self.coordinator),
            consumer_name: Arc::clone(&self.consumer_name),
            size: additional,
            alloc_type: self.alloc_type,
            waiter_id: None,
        };

        let mut result = future.await?.take();
        // zero out, we're taking it
        result.size = 0;

        self.size += additional;

        Ok(())
    }

    /// Attempts to grow this allocation by `additional` bytes.
    ///
    /// On failure, the allocation size is unchanged.
    pub fn try_grow(&mut self, additional: usize) -> Result<()> {
        let mut state = self.coordinator.state.lock();
        if state.try_reserve(additional, self.alloc_type, self.coordinator.capacity) {
            self.size += additional;
            Ok(())
        } else {

            let backtrace = std::backtrace::Backtrace::force_capture();

            let available = state.available(self.coordinator.capacity);
            Err(resources_datafusion_err!(
                "Failed to grow allocation by {} for consumer '{}' - {} available (used: {} required + {} spillable = {} total, capacity: {}).  Backtrace: {}",
                human_readable_size(additional),
                self.consumer_name,
                human_readable_size(available),
                human_readable_size(state.used_required),
                human_readable_size(state.used_spillable),
                human_readable_size(state.used()),
                self.coordinator
                    .capacity
                    .map_or("unbounded".to_string(), |c| human_readable_size(c)),
                backtrace
            ))
        }
    }

    /// Shrinks this allocation by `amount` bytes.
    ///
    /// Wakes the front waiter if their request can now be satisfied.
    ///
    /// # Panics
    ///
    /// Panics if `amount` exceeds the allocation size.
    pub fn shrink(&mut self, amount: usize) {
        assert!(
            amount <= self.size,
            "shrink amount ({amount}) exceeds allocation size ({})",
            self.size,
        );
        self.size -= amount;
        let waker = {
            let mut state = self.coordinator.state.lock();
            state.release(amount, self.alloc_type);
            wake_front_if_satisfiable(&state, self.coordinator.capacity)
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    pub fn new_empty(&mut self) -> MemoryAllocation {
        self.split(0)
    }

    pub fn split(&mut self, amount: usize) -> MemoryAllocation {
        assert!(
            amount <= self.size,
            "split amount ({amount}) exceeds allocation size ({})",
            self.size,
        );
        // Only adjust the local sizes — the coordinator's counters stay the same
        // because total tracked memory is unchanged (just split across two handles).
        self.size -= amount;

        Self {
            coordinator: self.coordinator.clone(),
            consumer_name: self.consumer_name.clone(),
            size: amount,
            alloc_type: self.alloc_type,
        }
    }

    pub fn take(&mut self) -> MemoryAllocation {
        self.split(self.size)
    }

    /// Resizes this allocation to exactly `new_size` bytes.
    ///
    /// If `new_size` is larger, grows the allocation (may fail).
    /// If `new_size` is smaller, shrinks the allocation.
    /// If equal, this is a no-op.
    pub fn try_resize(&mut self, new_size: usize) -> Result<()> {
        use std::cmp::Ordering;
        match new_size.cmp(&self.size) {
            Ordering::Greater => self.try_grow(new_size - self.size),
            Ordering::Less => {
                self.shrink(self.size - new_size);
                Ok(())
            }
            Ordering::Equal => Ok(()),
        }
    }

    /// Frees all memory in this allocation, returning the number of bytes freed.
    ///
    /// Subsequent calls return 0. Wakes the front waiter if their request fits.
    pub fn free(&mut self) -> usize {
        if self.size == 0 {
            return 0;
        }
        let size = self.size;
        self.size = 0;
        let waker = {
            let mut state = self.coordinator.state.lock();
            state.release(size, self.alloc_type);
            wake_front_if_satisfiable(&state, self.coordinator.capacity)
        };
        if let Some(w) = waker {
            w.wake();
        }
        size
    }
}

impl Drop for MemoryAllocation {
    fn drop(&mut self) {
        self.free();
    }
}

/// Peek at the front of the waiter queue. If its requested size fits in
/// available memory, clone its waker (to be woken outside the lock).
fn wake_front_if_satisfiable(
    state: &CoordinatorState,
    capacity: Option<usize>,
) -> Option<Waker> {
    if let Some(front) = state.waiter_queue.front() {
        let avail = state.available(capacity);
        if front.size <= avail {
            Some(front.waker.clone())
        } else {
            None
        }
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// AllocateFuture
// ---------------------------------------------------------------------------

/// A future that resolves to a [`MemoryAllocation`] once sufficient memory is available.
///
/// Created by [`CoordinatedConsumer::allocate`]. Implements cancellation safety:
/// dropping the future removes it from the waiter queue.
pub struct AllocateFuture {
    coordinator: Arc<CoordinatorInner>,
    consumer_name: Arc<str>,
    size: usize,
    alloc_type: AllocationType,
    /// `None` means not yet registered in the waiter queue.
    waiter_id: Option<WaiterId>,
}

impl Future for AllocateFuture {
    type Output = Result<MemoryAllocation>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut state = this.coordinator.state.lock();

        if let Some(id) = this.waiter_id {
            // Already registered — only attempt reservation if we're at the front
            // of the FIFO queue. This enforces strict ordering.
            let is_front = state
                .waiter_queue
                .front()
                .map(|f| f.id == id)
                .unwrap_or(false);

            if is_front
                && state.try_reserve(
                    this.size,
                    this.alloc_type,
                    this.coordinator.capacity,
                )
            {
                state.waiter_queue.pop_front();
                this.waiter_id = None;
                return Poll::Ready(Ok(MemoryAllocation {
                    coordinator: Arc::clone(&this.coordinator),
                    consumer_name: Arc::clone(&this.consumer_name),
                    size: this.size,
                    alloc_type: this.alloc_type,
                }));
            }

            // Update waker in case the executor changed it
            if let Some(entry) = state.waiter_queue.iter_mut().find(|e| e.id == id) {
                entry.waker = cx.waker().clone();
            }

            return Poll::Pending;
        }

        // First poll — try immediate reservation before entering the queue
        if state.try_reserve(this.size, this.alloc_type, this.coordinator.capacity) {
            return Poll::Ready(Ok(MemoryAllocation {
                coordinator: Arc::clone(&this.coordinator),
                consumer_name: Arc::clone(&this.consumer_name),
                size: this.size,
                alloc_type: this.alloc_type,
            }));
        }

        // Not enough memory — assign an ID and push to the back of the queue
        let id = state.next_waiter_id;
        state.next_waiter_id += 1;
        this.waiter_id = Some(id);
        state.waiter_queue.push_back(WaitEntry {
            id,
            size: this.size,
            alloc_type: this.alloc_type,
            waker: cx.waker().clone(),
        });

        Poll::Pending
    }
}

impl Drop for AllocateFuture {
    fn drop(&mut self) {
        // Cancellation safety: remove ourselves from the waiter queue
        if let Some(id) = self.waiter_id.take() {
            let waker = {
                let mut state = self.coordinator.state.lock();
                state.waiter_queue.retain(|e| e.id != id);
                // After removing ourselves, the next front waiter might be satisfiable
                wake_front_if_satisfiable(&state, self.coordinator.capacity)
            };
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{RawWaker, RawWakerVTable};

    // -- noop waker for manual poll tests --

    fn noop_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(data: *const ()) -> RawWaker {
            RawWaker::new(data, &VTABLE)
        }
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    fn noop_waker() -> Waker {
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    // -----------------------------------------------------------------------
    // Synchronous tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bounded_basic() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let alloc = consumer
            .try_allocate(40, AllocationType::Spillable)
            .unwrap();
        assert_eq!(alloc.size(), 40);
        assert_eq!(coord.used(), 40);
        assert_eq!(coord.available(), 60);

        drop(alloc);
        assert_eq!(coord.used(), 0);
        assert_eq!(coord.available(), 100);
    }

    #[test]
    fn test_unbounded() {
        let coord = MemoryCoordinator::new_unbounded();
        let consumer = coord.register("test");

        let a1 = consumer
            .try_allocate(1_000_000, AllocationType::Required)
            .unwrap();
        let a2 = consumer
            .try_allocate(2_000_000, AllocationType::Spillable)
            .unwrap();
        assert_eq!(coord.used(), 3_000_000);
        assert_eq!(a1.size(), 1_000_000);
        assert_eq!(a2.size(), 2_000_000);
    }

    #[test]
    fn test_allocation_types_tracked_separately() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let _a1 = consumer.try_allocate(30, AllocationType::Required).unwrap();
        let _a2 = consumer
            .try_allocate(50, AllocationType::Spillable)
            .unwrap();

        assert_eq!(coord.used_required(), 30);
        assert_eq!(coord.used_spillable(), 50);
        assert_eq!(coord.used(), 80);
    }

    #[test]
    fn test_try_allocate_fails_when_full() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let _alloc = consumer.try_allocate(80, AllocationType::Required).unwrap();
        let result = consumer.try_allocate(30, AllocationType::Spillable);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("test"),
            "error should contain consumer name"
        );
        assert!(
            err_msg.contains("spillable"),
            "error should contain allocation type"
        );
        // Original allocation should still be intact
        assert_eq!(coord.used(), 80);
    }

    #[test]
    fn test_raii_drop_frees_memory() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        {
            let _a1 = consumer.try_allocate(50, AllocationType::Required).unwrap();
            assert_eq!(coord.used(), 50);
        }
        assert_eq!(coord.used(), 0);
    }

    #[test]
    fn test_grow_and_shrink() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let mut alloc = consumer
            .try_allocate(30, AllocationType::Spillable)
            .unwrap();
        assert_eq!(alloc.size(), 30);

        alloc.try_grow(20).unwrap();
        assert_eq!(alloc.size(), 50);
        assert_eq!(coord.used(), 50);

        alloc.shrink(10);
        assert_eq!(alloc.size(), 40);
        assert_eq!(coord.used(), 40);
    }

    #[test]
    fn test_grow_beyond_capacity_fails() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let mut alloc = consumer.try_allocate(80, AllocationType::Required).unwrap();
        let result = alloc.try_grow(30);
        assert!(result.is_err());
        assert_eq!(alloc.size(), 80); // unchanged
        assert_eq!(coord.used(), 80);
    }

    #[test]
    fn test_free_returns_amount() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let mut alloc = consumer
            .try_allocate(60, AllocationType::Spillable)
            .unwrap();
        assert_eq!(alloc.free(), 60);
        assert_eq!(coord.used(), 0);

        // Double free returns 0
        assert_eq!(alloc.free(), 0);
    }

    #[test]
    fn test_zero_size_allocation() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let alloc = consumer.try_allocate(0, AllocationType::Required).unwrap();
        assert_eq!(alloc.size(), 0);
        assert_eq!(coord.used(), 0);
    }

    #[test]
    fn test_should_spill_no_waiters() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");
        assert!(!consumer.should_spill());
    }

    #[test]
    fn test_multiple_consumers_share_pool() {
        let coord = MemoryCoordinator::new_bounded(100);
        let c1 = coord.register("c1");
        let c2 = coord.register("c2");

        let _a1 = c1.try_allocate(60, AllocationType::Required).unwrap();
        let _a2 = c2.try_allocate(30, AllocationType::Spillable).unwrap();
        assert_eq!(coord.used(), 90);

        let result = c2.try_allocate(20, AllocationType::Required);
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "shrink amount (50) exceeds allocation size (30)")]
    fn test_shrink_panics_on_underflow() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let mut alloc = consumer
            .try_allocate(30, AllocationType::Spillable)
            .unwrap();
        alloc.shrink(50);
    }

    #[test]
    fn test_mixed_allocation_types() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let _a1 = consumer.try_allocate(30, AllocationType::Required).unwrap();
        let _a2 = consumer
            .try_allocate(40, AllocationType::Spillable)
            .unwrap();

        assert_eq!(coord.used_required(), 30);
        assert_eq!(coord.used_spillable(), 40);

        assert_eq!(_a1.allocation_type(), AllocationType::Required);
        assert_eq!(_a2.allocation_type(), AllocationType::Spillable);
    }

    #[test]
    fn test_error_messages() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("my_sort_op");

        let _alloc = consumer.try_allocate(90, AllocationType::Required).unwrap();
        let err = consumer
            .try_allocate(20, AllocationType::Spillable)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("my_sort_op"));
        assert!(msg.contains("spillable"));
    }

    #[test]
    fn test_display_allocation_type() {
        assert_eq!(AllocationType::Required.to_string(), "required");
        assert_eq!(AllocationType::Spillable.to_string(), "spillable");
    }

    #[test]
    fn test_debug_impls() {
        let coord = MemoryCoordinator::new_bounded(100);
        let debug = format!("{:?}", coord);
        assert!(debug.contains("MemoryCoordinator"));
        assert!(debug.contains("capacity"));

        let consumer = coord.register("test");
        let debug = format!("{:?}", consumer);
        assert!(debug.contains("CoordinatedConsumer"));
        assert!(debug.contains("test"));

        let alloc = consumer.try_allocate(10, AllocationType::Required).unwrap();
        let debug = format!("{:?}", alloc);
        assert!(debug.contains("MemoryAllocation"));
    }

    // -----------------------------------------------------------------------
    // Async tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_async_allocate_succeeds_immediately() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("test");

        let mut future = consumer.allocate(50, AllocationType::Spillable);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => {
                assert_eq!(alloc.size(), 50);
                assert_eq!(coord.used(), 50);
            }
            other => panic!("Expected Ready(Ok(...)), got {:?}", other),
        }
    }

    #[test]
    fn test_async_allocate_waits_then_succeeds() {
        // Fill the pool
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("waiter");
        let holder = coord.register("holder");

        let mut held = holder.try_allocate(80, AllocationType::Spillable).unwrap();

        // Try to allocate 40 — should pend
        let mut future = consumer.allocate(40, AllocationType::Required);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 1);

        // holder sees should_spill
        assert!(holder.should_spill());

        // holder frees memory
        held.free();
        assert_eq!(coord.used(), 0);

        // Now the future should succeed
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => {
                assert_eq!(alloc.size(), 40);
                assert_eq!(coord.used(), 40);
            }
            other => panic!("Expected Ready(Ok(...)), got {:?}", other),
        }
        assert_eq!(coord.num_waiters(), 0);
    }

    #[test]
    fn test_cancellation_safety() {
        let coord = MemoryCoordinator::new_bounded(100);
        let consumer = coord.register("waiter");

        let _held = consumer
            .try_allocate(100, AllocationType::Required)
            .unwrap();

        let mut future = consumer.allocate(50, AllocationType::Spillable);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 1);

        // Drop the future — should remove from queue
        drop(future);
        assert_eq!(coord.num_waiters(), 0);
    }

    #[test]
    fn test_fifo_ordering() {
        let coord = MemoryCoordinator::new_bounded(100);

        // Fill the pool
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        // Waiter A wants 30
        let consumer_a = coord.register("A");
        let mut future_a = consumer_a.allocate(30, AllocationType::Required);

        // Waiter B wants 20
        let consumer_b = coord.register("B");
        let mut future_b = consumer_b.allocate(20, AllocationType::Spillable);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Both should pend
        assert!(Pin::new(&mut future_a).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut future_b).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 2);

        // Free 30 — A is at front, needs 30, should be woken.
        // B needs 20 and there would be 0 left after A, so B stays pending.
        held.shrink(30);

        // A should succeed — keep alloc alive so memory stays reserved
        let _alloc_a = match Pin::new(&mut future_a).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => {
                assert_eq!(alloc.size(), 30);
                alloc
            }
            other => panic!("Expected A to be Ready, got {:?}", other),
        };

        // B should still be pending (70 used by held, 30 by A = 100, 0 available)
        assert!(Pin::new(&mut future_b).poll(&mut cx).is_pending());

        // Free more from held — now B can proceed
        held.shrink(30);
        let _alloc_b = match Pin::new(&mut future_b).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => {
                assert_eq!(alloc.size(), 20);
                alloc
            }
            other => panic!("Expected B to be Ready, got {:?}", other),
        };
        assert_eq!(coord.num_waiters(), 0);
    }

    #[test]
    fn test_multiple_waiters_all_served() {
        let coord = MemoryCoordinator::new_bounded(100);
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        let c1 = coord.register("w1");
        let c2 = coord.register("w2");
        let c3 = coord.register("w3");

        let mut f1 = c1.allocate(10, AllocationType::Required);
        let mut f2 = c2.allocate(10, AllocationType::Required);
        let mut f3 = c3.allocate(10, AllocationType::Required);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut f1).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut f2).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut f3).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 3);

        // Free all held memory
        held.free();

        // Each should succeed in turn — keep allocs alive
        let _a1 = match Pin::new(&mut f1).poll(&mut cx) {
            Poll::Ready(Ok(a)) => a,
            other => panic!("Expected f1 Ready, got {:?}", other),
        };
        let _a2 = match Pin::new(&mut f2).poll(&mut cx) {
            Poll::Ready(Ok(a)) => a,
            other => panic!("Expected f2 Ready, got {:?}", other),
        };
        let _a3 = match Pin::new(&mut f3).poll(&mut cx) {
            Poll::Ready(Ok(a)) => a,
            other => panic!("Expected f3 Ready, got {:?}", other),
        };
        assert_eq!(coord.num_waiters(), 0);
        assert_eq!(coord.used(), 30);
    }

    #[test]
    fn test_partial_free_wakes_front_when_fits() {
        let coord = MemoryCoordinator::new_bounded(100);
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        let consumer = coord.register("waiter");
        let mut future = consumer.allocate(20, AllocationType::Required);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());

        // Shrink 30 — front needs 20, which fits in 30 available
        held.shrink(30);

        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => assert_eq!(alloc.size(), 20),
            other => panic!("Expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn test_partial_free_does_not_wake_when_insufficient() {
        let coord = MemoryCoordinator::new_bounded(100);
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        let consumer = coord.register("waiter");
        let mut future = consumer.allocate(50, AllocationType::Required);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());

        // Shrink only 10 — front needs 50, which doesn't fit
        held.shrink(10);

        // Still pending (we poll to check, but it can't reserve)
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 1);

        // Now free enough
        held.shrink(50);
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => assert_eq!(alloc.size(), 50),
            other => panic!("Expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn test_async_with_real_threads() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let coord = Arc::new(MemoryCoordinator::new_bounded(100));
        let done = Arc::new(AtomicBool::new(false));
        let waiter_allocated = Arc::new(AtomicUsize::new(0));

        // Holder fills the pool
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        // Spawn a thread that will async-allocate
        let coord2 = Arc::clone(&coord);
        let done2 = Arc::clone(&done);
        let waiter_allocated2 = Arc::clone(&waiter_allocated);
        let waiter_thread = thread::spawn(move || {
            let consumer = coord2.register("waiter");
            let mut future = consumer.allocate(40, AllocationType::Required);

            // Manual polling loop
            loop {
                let flag = Arc::clone(&done2);
                let allocated = Arc::clone(&waiter_allocated2);
                let waker_fn = Arc::new(move || {
                    // Signal that we've been woken
                    let _ = &flag;
                    let _ = &allocated;
                });
                let _ = waker_fn;

                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);

                match Pin::new(&mut future).poll(&mut cx) {
                    Poll::Ready(Ok(alloc)) => {
                        waiter_allocated2.store(alloc.size(), Ordering::SeqCst);
                        done2.store(true, Ordering::SeqCst);
                        // Keep alloc alive to prevent drop
                        std::mem::forget(alloc);
                        return;
                    }
                    Poll::Ready(Err(e)) => panic!("Unexpected error: {e}"),
                    Poll::Pending => {
                        // Busy-wait a little and retry
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        });

        // Let waiter register
        thread::sleep(Duration::from_millis(10));

        // Holder frees memory
        held.free();

        waiter_thread.join().unwrap();

        assert!(done.load(Ordering::SeqCst));
        assert_eq!(waiter_allocated.load(Ordering::SeqCst), 40);
    }

    #[test]
    fn test_cancellation_wakes_next() {
        // When a future at the front of the queue is cancelled, the next waiter
        // should be considered for waking.
        let coord = MemoryCoordinator::new_bounded(100);
        let holder = coord.register("holder");
        let mut held = holder.try_allocate(100, AllocationType::Spillable).unwrap();

        let c1 = coord.register("w1");
        let c2 = coord.register("w2");

        let mut f1 = c1.allocate(30, AllocationType::Required);
        let mut f2 = c2.allocate(15, AllocationType::Required);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Both pend since pool is completely full
        assert!(Pin::new(&mut f1).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut f2).poll(&mut cx).is_pending());
        assert_eq!(coord.num_waiters(), 2);

        // Free 20 bytes — front waiter (f1) needs 30, so no one is woken yet
        held.shrink(20);
        assert!(Pin::new(&mut f1).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut f2).poll(&mut cx).is_pending());

        // Drop f1 — f2 becomes front. Available is 20, f2 needs 15 → satisfiable
        drop(f1);
        assert_eq!(coord.num_waiters(), 1);

        // f2 should now succeed
        match Pin::new(&mut f2).poll(&mut cx) {
            Poll::Ready(Ok(alloc)) => assert_eq!(alloc.size(), 15),
            other => panic!("Expected Ready, got {:?}", other),
        }
    }
}
