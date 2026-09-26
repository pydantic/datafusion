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

//! Measurements of filter evaluations at runtime.
//!
//! Operators that adapt to the data measure the filters that they evaluate:
//! the rows in, the rows that pass and the evaluation time. This module has
//! the shared parts:
//!
//! * [`Clock`]: a monotonic clock that tests can replace, so that decisions
//!   that use time are deterministic in tests. [`SystemClock`] is the real
//!   clock and [`ManualClock`] is a clock that only moves when a test moves
//!   it.
//! * [`FilterCost`]: the counts and the time of one filter, and the values
//!   derived from them (cost for each row, rows removed for each
//!   nanosecond).
//! * [`RemovedRowWork`] and [`DownstreamWork`]: the work that a row that a
//!   filter removes saves after the filter, as the producer of the filter
//!   and the scan that evaluates it measure it.
//!
//! For example, an operator can use them to pause a filter that costs more
//! than it saves, or to change the order of the conjuncts of a predicate.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use datafusion_common::instant::Instant;
use parking_lot::Mutex;

/// A monotonic clock in nanoseconds.
///
/// Production code uses [`SystemClock`]. Tests use [`ManualClock`] (or their
/// own implementation), thus decisions that use time are deterministic in
/// tests.
pub trait Clock: Debug + Send + Sync {
    /// Nanoseconds since an arbitrary fixed point. The value never
    /// decreases.
    fn now_nanos(&self) -> u64;
}

/// The real monotonic [`Clock`].
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Creates a clock whose zero is now.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// A shared [`SystemClock`], as a trait object.
    pub fn shared() -> Arc<dyn Clock> {
        Arc::new(Self::new())
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now_nanos(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// A [`Clock`] that moves only when [`Self::advance`] is called. For tests.
#[derive(Debug, Default)]
pub struct ManualClock {
    nanos: AtomicU64,
}

impl ManualClock {
    /// Creates a clock at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves the clock forward by `nanos` nanoseconds.
    pub fn advance(&self, nanos: u64) {
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_nanos(&self) -> u64 {
        self.nanos.load(Ordering::Relaxed)
    }
}

/// Minimum number of evaluated rows before an adaptive decision uses the
/// measurements of a filter. It is one batch of the default
/// `datafusion.execution.batch_size`. With fewer rows, the evaluation time
/// is dominated by the fixed cost of each call (for example 2 to 7 rows of
/// a batch after a selective row filter took 600 to 8000 ns for each row in
/// ClickBench Q23, against 0.4 ns for each row on full batches), and the
/// fraction of removed rows is not reliable.
pub const MIN_OBSERVED_ROWS: u64 = 8192;

/// Returns the nanoseconds in `elapsed`, saturated to `u64::MAX`.
pub fn duration_nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

/// The measurements of one filter: the rows that it was evaluated on, the
/// rows that passed it, and the evaluation time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FilterCost {
    /// Rows that the filter was evaluated on.
    pub rows_in: u64,
    /// Rows that passed the filter (`true`; `null` does not pass).
    pub rows_out: u64,
    /// Evaluation time in nanoseconds.
    pub nanos: u64,
}

impl FilterCost {
    /// Adds the result of one evaluation. `rows_out` larger than `rows_in`
    /// is used as `rows_in`.
    pub fn add(&mut self, rows_in: u64, rows_out: u64, nanos: u64) {
        self.rows_in = self.rows_in.saturating_add(rows_in);
        self.rows_out = self.rows_out.saturating_add(rows_out.min(rows_in));
        self.nanos = self.nanos.saturating_add(nanos);
    }

    /// Rows that the filter removed.
    pub fn rows_removed(&self) -> u64 {
        self.rows_in.saturating_sub(self.rows_out)
    }

    /// Nanoseconds for each evaluated row, or `None` if the filter was not
    /// evaluated on any row.
    pub fn nanos_per_row(&self) -> Option<f64> {
        (self.rows_in > 0).then(|| self.nanos as f64 / self.rows_in as f64)
    }

    /// Rows removed for each nanosecond, `(1 + rows_in - rows_out) / nanos`,
    /// or `None` if the filter was not evaluated on any row. A larger value
    /// is a better filter to evaluate first. This is the ranking key of
    /// Velox (Pedreira et al., VLDB 2022). The `1 +` ranks a filter that
    /// removes no rows by its cost, and a zero time is used as 1 ns.
    pub fn rows_removed_per_nano(&self) -> Option<f64> {
        (self.rows_in > 0)
            .then(|| (1 + self.rows_removed()) as f64 / self.nanos.max(1) as f64)
    }
}

/// The work, in nanoseconds for each row, that an operator does on the rows
/// that a filter removes before them, as measured by that operator.
///
/// For example, a hash join computes the hashes of the join keys of each
/// probe row and looks them up in its hash table. A row that the dynamic
/// filter of the join removes in the scan does not get this work, thus this
/// is the saving of a removed row. The work that depends on a match (the
/// check of the candidates of the lookup and the output of a matched row)
/// is not in it: the filter removes only rows without a match. While the
/// filter is on, most rows that reach the producer are matches, thus work
/// that only matches get would make the saving too large (TPC-DS SF1 Q31:
/// 6 ns for each probe row with the check, 0.1 to 1.2 ns without it). The
/// producer of a dynamic filter measures it and the consumers of the filter
/// read it (see
/// [`DynamicFilterPhysicalExpr::removed_row_work`]).
///
/// Lock-free.
///
/// [`DynamicFilterPhysicalExpr::removed_row_work`]: crate::expressions::DynamicFilterPhysicalExpr::removed_row_work
#[derive(Debug, Default)]
pub struct RemovedRowWork {
    rows: AtomicU64,
    nanos: AtomicU64,
}

impl RemovedRowWork {
    /// Creates an empty measurement.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `rows` rows and `nanos` nanoseconds of work. The two can be
    /// recorded separately (for example the rows when a batch arrives and
    /// the time of each step).
    pub fn record(&self, rows: u64, nanos: u64) {
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// The work for each row, or `None` before [`MIN_OBSERVED_ROWS`] rows.
    pub fn ns_per_row(&self) -> Option<f64> {
        let rows = self.rows.load(Ordering::Relaxed);
        (rows >= MIN_OBSERVED_ROWS)
            .then(|| self.nanos.load(Ordering::Relaxed) as f64 / rows as f64)
    }
}

/// The work that the operators after a scan do for the rows of the scan,
/// measured by the scan while one optional filter is on and while it is
/// off. The difference is the work that the rows that the filter removes
/// save after the scan.
///
/// A row that the filter removes is not seen after the scan, thus its
/// saving can be measured only while the filter is off. The producer of
/// the filter measures only its own work ([`RemovedRowWork`]), but a
/// removed row also saves the work of all operators between the scan and
/// the producer. And a selective filter can remove almost all rows before
/// its producer sees them: then the producer cannot measure its work.
///
/// For each input batch of the scan, the scan records whether the filter
/// was evaluated (on) or skipped (off), the input rows, the rows that the
/// filter removed, and the time from the return of the output batch to the
/// next poll of the scan (the synchronous work of the operators above the
/// scan on that batch, 0 without output rows). Then:
///
/// ```text
/// work(state) = median over the batches of downstream ns / input rows
/// removed     = rows that the filter removed / input rows, while on
/// ns for each removed row = (work(off) - work(on)) / removed
/// ```
///
/// This is the saving for each row that the filter itself removes, as its
/// gate counts them. When other conjuncts remove most of these rows too,
/// the difference is small, and so is the saving.
///
/// The median, not the mean: under load, the thread can stop between the
/// return of a batch and the next poll, and the first batches after the
/// scan are slow (TPC-DS SF1 Q10: 54 µs for a batch of 2 rows, then 3 µs).
/// Each state needs [`MIN_OBSERVED_ROWS`] input rows, and as many batches
/// as a probe pause of the gate (`samples`).
///
/// The time stops at an exchange (for example a `RepartitionExec` that
/// sends the batch to another task): the work after the exchange is not in
/// it. Thus it is at least the [`RemovedRowWork`] of the producer for the
/// consumers (see
/// [`OptionalFilterGate`](crate::optional_filter_gate::OptionalFilterGate)).
///
/// Shared by the gates of one filter in all files and partitions of a scan.
#[derive(Debug)]
pub struct DownstreamWork {
    on: Mutex<DownstreamSamples>,
    off: Mutex<DownstreamSamples>,
    /// The batches that each state needs.
    samples: usize,
    probe_claimed: AtomicBool,
}

/// The input batches of one state of a [`DownstreamWork`].
#[derive(Debug, Default)]
struct DownstreamSamples {
    input_rows: u64,
    removed_rows: u64,
    /// Downstream nanoseconds for each input row, one value for each batch.
    ns_per_input_row: Vec<f64>,
}

impl DownstreamSamples {
    fn is_complete(&self, samples: usize) -> bool {
        self.input_rows >= MIN_OBSERVED_ROWS && self.ns_per_input_row.len() >= samples
    }

    fn median(&mut self) -> f64 {
        let values = &mut self.ns_per_input_row;
        if values.is_empty() {
            return 0.0;
        }
        let middle = values.len() / 2;
        *values.select_nth_unstable_by(middle, f64::total_cmp).1
    }
}

impl DownstreamWork {
    /// Creates an empty measurement. Each state needs `samples` batches
    /// (at least 1).
    pub fn new(samples: usize) -> Self {
        Self {
            on: Mutex::default(),
            off: Mutex::default(),
            samples: samples.max(1),
            probe_claimed: AtomicBool::new(false),
        }
    }

    /// Records one input batch of `input_rows` rows: the filter was
    /// `evaluated` (and removed `removed_rows` rows) or skipped, and the
    /// operators above the scan took `nanos` with its output (0 without
    /// output rows).
    pub fn record(
        &self,
        evaluated: bool,
        input_rows: usize,
        removed_rows: usize,
        nanos: u64,
    ) {
        if input_rows == 0 {
            return;
        }
        let samples = if evaluated { &self.on } else { &self.off };
        let mut samples = samples.lock();
        samples.input_rows += input_rows as u64;
        samples.removed_rows += removed_rows as u64;
        samples
            .ns_per_input_row
            .push(nanos as f64 / input_rows as f64);
    }

    /// True until each state has its samples.
    pub fn needs_samples(&self) -> bool {
        !(self.on.lock().is_complete(self.samples)
            && self.off.lock().is_complete(self.samples))
    }

    /// True until the "off" state has its samples.
    pub fn needs_off_samples(&self) -> bool {
        !self.off.lock().is_complete(self.samples)
    }

    /// True once a consumer recorded a batch: only then a probe can give
    /// samples. A consumer that evaluates the filter where it cannot
    /// measure the work after it records nothing.
    pub fn has_samples(&self) -> bool {
        !self.on.lock().ns_per_input_row.is_empty()
            || !self.off.lock().ns_per_input_row.is_empty()
    }

    /// Returns true for the first caller only: the gate that pauses the
    /// filter one time to measure the "off" state for all gates.
    pub fn claim_probe(&self) -> bool {
        !self.probe_claimed.swap(true, Ordering::Relaxed)
    }

    /// The work, in nanoseconds, that each row that the filter removes
    /// saves after the scan, or `None` before each state has its samples.
    /// 0 if the filter removed no rows.
    pub fn ns_per_removed_row(&self) -> Option<f64> {
        let mut on = self.on.lock();
        let mut off = self.off.lock();
        if !(on.is_complete(self.samples) && off.is_complete(self.samples)) {
            return None;
        }
        if on.removed_rows == 0 {
            return Some(0.0);
        }
        let removed = on.removed_rows as f64 / on.input_rows as f64;
        Some(((off.median() - on.median()) / removed).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removed_row_work_needs_min_observed_rows() {
        let work = RemovedRowWork::new();
        work.record(MIN_OBSERVED_ROWS - 1, 0);
        work.record(0, 3 * (MIN_OBSERVED_ROWS - 1));
        assert_eq!(work.ns_per_row(), None);
        work.record(1, 3);
        assert_eq!(work.ns_per_row(), Some(3.0));
    }

    #[test]
    fn downstream_work_is_the_difference_of_the_states() {
        let work = DownstreamWork::new(3);
        assert!(work.needs_samples());
        assert!(!work.has_samples());
        let rows = MIN_OBSERVED_ROWS as usize;
        // Filter on: it removes 90% of the rows, 1 ns for each input row
        // after the scan.
        for _ in 0..3 {
            work.record(true, rows, rows * 9 / 10, rows as u64);
        }
        assert!(work.has_samples());
        assert!(work.needs_samples());
        assert!(work.needs_off_samples());
        assert_eq!(work.ns_per_removed_row(), None);
        // Filter off: 10 ns for each input row, and one batch where the
        // thread stopped: the median ignores it.
        work.record(false, rows, 0, 10 * rows as u64);
        work.record(false, rows, 0, 1000 * rows as u64);
        assert!(work.needs_off_samples());
        work.record(false, rows, 0, 10 * rows as u64);
        assert!(!work.needs_samples());
        // (10 - 1) / 0.9 = 10 ns for each removed row.
        let ns = work.ns_per_removed_row().unwrap();
        assert!((ns - 10.0).abs() < 1e-2, "{ns}");
        assert!(work.claim_probe());
        assert!(!work.claim_probe());
    }

    #[test]
    fn downstream_work_without_removed_rows_is_zero() {
        let work = DownstreamWork::new(1);
        let rows = MIN_OBSERVED_ROWS as usize;
        work.record(true, rows, 0, 5 * rows as u64);
        work.record(false, rows, 0, 7 * rows as u64);
        assert_eq!(work.ns_per_removed_row(), Some(0.0));
    }

    #[test]
    fn filter_cost_derived_values() {
        let empty = FilterCost::default();
        assert_eq!(empty.nanos_per_row(), None);
        assert_eq!(empty.rows_removed_per_nano(), None);

        let mut cost = FilterCost::default();
        cost.add(100, 25, 1_000);
        cost.add(100, 200, 1_000);
        assert_eq!(cost.rows_in, 200);
        // `rows_out` is at most `rows_in` for each evaluation.
        assert_eq!(cost.rows_out, 125);
        assert_eq!(cost.rows_removed(), 75);
        assert_eq!(cost.nanos_per_row(), Some(10.0));
        assert_eq!(cost.rows_removed_per_nano(), Some(76.0 / 2_000.0));

        // A zero time is used as 1 ns.
        let mut free = FilterCost::default();
        free.add(10, 0, 0);
        assert_eq!(free.rows_removed_per_nano(), Some(11.0));
    }

    #[test]
    fn manual_clock_moves_only_when_advanced() {
        let clock = ManualClock::new();
        assert_eq!(clock.now_nanos(), 0);
        clock.advance(5);
        clock.advance(7);
        assert_eq!(clock.now_nanos(), 12);
    }

    #[test]
    fn system_clock_is_monotonic() {
        let clock = SystemClock::new();
        let first = clock.now_nanos();
        assert!(clock.now_nanos() >= first);
        assert_eq!(duration_nanos(Duration::from_micros(3)), 3_000);
    }
}
