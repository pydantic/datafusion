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

//! Concurrent registry of per-predicate [`SelectivityStats`] plus the
//! "skip" flags that let an *optional* predicate be turned into a no-op
//! mid-stream.
//!
//! This is shared plumbing, free of any placement/ordering policy. A consumer
//! [`register`](AdaptiveStatsRegistry::register)s the predicates it tracks,
//! calls [`record`](AdaptiveStatsRegistry::record) on the per-batch hot path,
//! and reads back [`snapshot`](AdaptiveStatsRegistry::snapshot)s when it
//! periodically re-decides (placement, ordering, drop, …).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::{Mutex, RwLock};

use super::stats::{FilterId, SelectivityStats};

/// Thread-safe map of [`FilterId`] → online [`SelectivityStats`], with
/// per-predicate skip flags.
///
/// # Locking
///
/// The outer [`RwLock`] over the stats map is almost always *read*-locked: both
/// [`record`](Self::record) (hot, per-batch) and the snapshot readers only need
/// shared access to look up an existing entry. The write lock is taken only by
/// [`register`](Self::register) when a new [`FilterId`] is first seen — a brief,
/// infrequent operation.
///
/// Each entry is an independent [`Mutex<SelectivityStats>`], so concurrent
/// `record` calls on *different* predicates proceed in parallel with zero
/// contention.
#[derive(Debug, Default)]
pub struct AdaptiveStatsRegistry {
    /// Per-predicate selectivity statistics, each individually `Mutex`-guarded.
    stats: RwLock<HashMap<FilterId, Mutex<SelectivityStats>>>,
    /// Per-predicate "skip" flags. When set, the consumer treats the predicate
    /// as a no-op for subsequent batches. Only ever set for predicates whose
    /// [`SelectivityStats::is_optional`] is `true` — mandatory predicates must
    /// always execute or queries return wrong rows.
    skip_flags: RwLock<HashMap<FilterId, Arc<AtomicBool>>>,
}

impl AdaptiveStatsRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a predicate so future [`record`](Self::record) calls can find
    /// it. Idempotent: an already-registered id keeps its accumulated stats and
    /// its existing optional flag.
    ///
    /// `is_optional` records whether the predicate may be dropped without
    /// affecting correctness (see [`SelectivityStats::is_optional`]).
    pub fn register(&self, id: FilterId, is_optional: bool) {
        if self.stats.read().contains_key(&id) {
            return;
        }
        let mut stats = self.stats.write();
        stats
            .entry(id)
            .or_insert_with(|| Mutex::new(SelectivityStats::new(is_optional)));
        self.skip_flags
            .write()
            .entry(id)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)));
    }

    /// Register many predicates at once (see [`register`](Self::register)).
    pub fn register_all(&self, entries: impl IntoIterator<Item = (FilterId, bool)>) {
        let mut stats = self.stats.write();
        let mut flags = self.skip_flags.write();
        for (id, is_optional) in entries {
            stats
                .entry(id)
                .or_insert_with(|| Mutex::new(SelectivityStats::new(is_optional)));
            flags
                .entry(id)
                .or_insert_with(|| Arc::new(AtomicBool::new(false)));
        }
    }

    /// Record one batch of observations for `id` (per-batch hot path).
    ///
    /// Takes only a shared lock on the map plus the per-predicate mutex, so it
    /// never contends with `record` calls on other predicates. A no-op if `id`
    /// was never [`register`](Self::register)ed.
    pub fn record(
        &self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        effectiveness_sample: f64,
    ) {
        let map = self.stats.read();
        if let Some(entry) = map.get(&id) {
            entry
                .lock()
                .record(matched, total, eval_nanos, effectiveness_sample);
        }
    }

    /// Merge a locally-accumulated delta into `id`'s shared stats (see
    /// [`SelectivityStats::merge`]). A no-op if `id` was never
    /// [`register`](Self::register)ed.
    ///
    /// Lets a consumer record per-batch observations into a private
    /// accumulator with no locking and fold them in here periodically.
    pub fn merge(&self, id: FilterId, delta: &SelectivityStats) {
        let map = self.stats.read();
        if let Some(entry) = map.get(&id) {
            entry.lock().merge(delta);
        }
    }

    /// Copy out the current stats for `id`, or `None` if unregistered.
    ///
    /// [`SelectivityStats`] is `Copy`, so consumers read every derived metric
    /// (pass rate, cost-per-row, effectiveness, confidence bounds) off the
    /// returned value without holding any lock.
    pub fn snapshot(&self, id: FilterId) -> Option<SelectivityStats> {
        self.stats.read().get(&id).map(|entry| *entry.lock())
    }

    /// Clear the accumulated stats for `id`, preserving its optional flag and
    /// skip flag. Used when a dynamic predicate re-arms under a stable id (see
    /// [`SelectivityStats::reset`]). A no-op if `id` is unregistered.
    pub fn reset(&self, id: FilterId) {
        if let Some(entry) = self.stats.read().get(&id) {
            entry.lock().reset();
        }
    }

    /// The shared skip flag for `id`, registering `id` as optional if it was
    /// not already present. The returned `Arc` can be cached by an evaluator so
    /// it can cheaply check the flag without touching the registry.
    pub fn skip_flag(&self, id: FilterId) -> Arc<AtomicBool> {
        if let Some(flag) = self.skip_flags.read().get(&id) {
            return Arc::clone(flag);
        }
        // First sighting: register as optional and create the flag.
        self.register(id, true);
        Arc::clone(
            self.skip_flags
                .read()
                .get(&id)
                .expect("skip flag inserted by register"),
        )
    }

    /// Whether `id`'s skip flag is currently set. `false` if unregistered.
    pub fn is_skipped(&self, id: FilterId) -> bool {
        self.skip_flags
            .read()
            .get(&id)
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
    }

    /// Set or clear `id`'s skip flag. A no-op if `id` is unregistered.
    pub fn set_skipped(&self, id: FilterId, skipped: bool) {
        if let Some(flag) = self.skip_flags.read().get(&id) {
            flag.store(skipped, Ordering::Relaxed);
        }
    }

    /// Whether `id` has been registered.
    pub fn contains(&self, id: FilterId) -> bool {
        self.stats.read().contains_key(&id)
    }

    /// Number of registered predicates.
    pub fn len(&self) -> usize {
        self.stats.read().len()
    }

    /// Whether no predicates are registered.
    pub fn is_empty(&self) -> bool {
        self.stats.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_requires_registration() {
        let reg = AdaptiveStatsRegistry::new();
        // recording an unregistered id is a silent no-op
        reg.record(0, 1, 10, 100, 5.0);
        assert!(reg.snapshot(0).is_none());
        assert_eq!(reg.len(), 0);

        reg.register(0, false);
        reg.record(0, 1, 10, 100, 5.0);
        let s = reg.snapshot(0).unwrap();
        assert_eq!(s.pass_rate(), Some(0.1));
        assert_eq!(s.sample_count(), 1);
    }

    #[test]
    fn register_is_idempotent_and_keeps_stats() {
        let reg = AdaptiveStatsRegistry::new();
        reg.register(7, false);
        reg.record(7, 2, 10, 100, 9.0);
        // re-register must not wipe accumulated stats
        reg.register(7, true);
        let s = reg.snapshot(7).unwrap();
        assert_eq!(s.sample_count(), 1);
        // optional flag is not flipped by a redundant register
        assert!(!s.is_optional());
    }

    #[test]
    fn register_all_bulk() {
        let reg = AdaptiveStatsRegistry::new();
        reg.register_all([(0, false), (1, true), (2, false)]);
        assert_eq!(reg.len(), 3);
        assert!(reg.snapshot(1).unwrap().is_optional());
        assert!(!reg.snapshot(0).unwrap().is_optional());
    }

    #[test]
    fn merge_folds_local_delta_into_shared() {
        let reg = AdaptiveStatsRegistry::new();
        reg.register(0, false);
        reg.record(0, 1, 10, 100, 4.0);

        let mut local = SelectivityStats::default();
        local.record(9, 10, 300, 8.0);
        reg.merge(0, &local);

        let s = reg.snapshot(0).unwrap();
        assert_eq!(s.pass_rate(), Some(0.5)); // 10/20
        assert_eq!(s.sample_count(), 2);
        assert!((s.effectiveness().unwrap() - 6.0).abs() < 1e-9);

        // merging an unregistered id is a silent no-op
        reg.merge(42, &local);
        assert!(reg.snapshot(42).is_none());
    }

    #[test]
    fn skip_flag_round_trips_and_is_shared() {
        let reg = AdaptiveStatsRegistry::new();
        reg.register(3, true);
        assert!(!reg.is_skipped(3));
        let flag = reg.skip_flag(3);
        reg.set_skipped(3, true);
        // the cached Arc observes the change made through the registry
        assert!(flag.load(Ordering::Relaxed));
        assert!(reg.is_skipped(3));
    }

    #[test]
    fn skip_flag_autoregisters_as_optional() {
        let reg = AdaptiveStatsRegistry::new();
        let _ = reg.skip_flag(42);
        assert!(reg.contains(42));
        assert!(reg.snapshot(42).unwrap().is_optional());
    }

    #[test]
    fn reset_clears_stats_keeps_flag() {
        let reg = AdaptiveStatsRegistry::new();
        reg.register(1, true);
        reg.record(1, 5, 10, 100, 3.0);
        reg.set_skipped(1, true);
        reg.reset(1);
        let s = reg.snapshot(1).unwrap();
        assert_eq!(s.sample_count(), 0);
        assert_eq!(s.pass_rate(), None);
        assert!(s.is_optional());
        // skip flag is independent of stats reset
        assert!(reg.is_skipped(1));
    }

    #[test]
    fn concurrent_records_on_distinct_ids() {
        use std::thread;
        let reg = Arc::new(AdaptiveStatsRegistry::new());
        reg.register_all((0..8).map(|i| (i, false)));
        thread::scope(|scope| {
            for id in 0..8usize {
                let reg = Arc::clone(&reg);
                scope.spawn(move || {
                    for _ in 0..1000 {
                        reg.record(id, 1, 2, 10, id as f64);
                    }
                });
            }
        });
        for id in 0..8usize {
            let s = reg.snapshot(id).unwrap();
            assert_eq!(s.sample_count(), 1000);
            assert_eq!(s.pass_rate(), Some(0.5));
            assert!((s.effectiveness().unwrap() - id as f64).abs() < 1e-9);
        }
    }
}
