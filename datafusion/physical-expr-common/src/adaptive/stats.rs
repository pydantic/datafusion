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

//! Online per-predicate selectivity and cost accumulator.
//!
//! This is the domain-agnostic measurement substrate shared by the adaptive
//! filter machinery. It records, for a single predicate (conjunct), the
//! quantities every adaptive policy needs:
//!
//! - **selectivity** — `rows_matched / rows_total` (the *pass rate*),
//! - **cost** — cumulative `eval_nanos`, from which a per-row cost is derived,
//! - **effectiveness** — a caller-supplied per-batch scalar, accumulated with
//!   [Welford's online algorithm] so callers can put a confidence interval on
//!   its mean.
//!
//! The accumulator deliberately does *not* define what "effectiveness" means or
//! what to do with these numbers — that is policy, and lives with the consumer:
//!
//! - the parquet scan ranks filters by *bytes-saved-per-second* to decide
//!   row-level vs. post-scan **placement**;
//! - an adaptive `FilterExec` ranks conjuncts by `cost_per_row / (1 - pass_rate)`
//!   to decide evaluation **order**.
//!
//! Both feed the same accumulator; only the per-batch sample and the ranking
//! function differ.
//!
//! [Welford's online algorithm]: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

/// Identifier for a tracked predicate, **local to a single
/// [`AdaptiveStatsRegistry`](super::registry::AdaptiveStatsRegistry)**.
///
/// There is no global expression id. Each consumer mints its own ids — in
/// practice the index of a conjunct in *that consumer's* predicate `Vec` — and
/// the same numeric value in two different registries refers to two unrelated
/// predicates. Ids are opaque here so the accumulator works regardless of how a
/// consumer enumerates its predicates.
pub type FilterId = usize;

/// Online selectivity + cost statistics for a single predicate expression.
///
/// Cheap to copy; a consumer typically owns one behind a per-predicate lock
/// (see [`AdaptiveStatsRegistry`](super::registry::AdaptiveStatsRegistry)) so
/// concurrent updates on *different* predicates never contend.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectivityStats {
    /// Number of rows that passed (matched) the predicate.
    rows_matched: u64,
    /// Total number of rows the predicate was evaluated on.
    rows_total: u64,
    /// Cumulative evaluation time in nanoseconds.
    eval_nanos: u64,
    /// Welford's online algorithm: number of per-batch effectiveness samples.
    sample_count: u64,
    /// Welford's online algorithm: running mean of the per-batch effectiveness
    /// sample.
    eff_mean: f64,
    /// Welford's online algorithm: running sum of squared deviations (M2).
    eff_m2: f64,
    /// Whether the underlying predicate is *optional* — i.e. may be skipped
    /// entirely without affecting query results (e.g. a dynamic join filter).
    ///
    /// Cached here so the per-batch hot path can decide whether the
    /// skip/drop logic applies with a single field load, without re-inspecting
    /// the expression. Mandatory predicates must always execute or queries
    /// return wrong rows.
    is_optional: bool,
}

impl Default for SelectivityStats {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SelectivityStats {
    /// Create an empty accumulator. `is_optional` records whether the predicate
    /// may be dropped without affecting correctness.
    pub fn new(is_optional: bool) -> Self {
        Self {
            rows_matched: 0,
            rows_total: 0,
            eval_nanos: 0,
            sample_count: 0,
            eff_mean: 0.0,
            eff_m2: 0.0,
            is_optional,
        }
    }

    /// Whether the predicate may be dropped without affecting correctness.
    pub fn is_optional(&self) -> bool {
        self.is_optional
    }

    /// Record one batch of observations.
    ///
    /// - `matched` / `total` — rows that passed / were evaluated this batch.
    /// - `eval_nanos` — wall time spent evaluating the predicate this batch.
    /// - `effectiveness_sample` — the caller's per-batch effectiveness metric
    ///   (e.g. bytes-saved-per-second for placement, or any scalar the
    ///   consumer wants confidence intervals on). Its unit is opaque here.
    ///
    /// The raw counters are always updated. The Welford accumulator only
    /// ingests the sample when `total > 0 && eval_nanos > 0` and the sample is
    /// finite — an empty or zero-time batch is not a meaningful sample.
    pub fn record(
        &mut self,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        effectiveness_sample: f64,
    ) {
        self.rows_matched += matched;
        self.rows_total += total;
        self.eval_nanos += eval_nanos;

        if total > 0 && eval_nanos > 0 && effectiveness_sample.is_finite() {
            self.sample_count += 1;
            let delta = effectiveness_sample - self.eff_mean;
            self.eff_mean += delta / self.sample_count as f64;
            let delta2 = effectiveness_sample - self.eff_mean;
            self.eff_m2 += delta * delta2;
        }
    }

    /// Cumulative pass rate `rows_matched / rows_total` in `[0, 1]`.
    ///
    /// `None` until at least one row has been evaluated. Lower means more
    /// selective — `1 - pass_rate()` is the fraction of rows discarded.
    pub fn pass_rate(&self) -> Option<f64> {
        if self.rows_total == 0 {
            return None;
        }
        Some(self.rows_matched as f64 / self.rows_total as f64)
    }

    /// Average evaluation cost per row, in nanoseconds.
    ///
    /// Unlike [`pass_rate`](Self::pass_rate) this is roughly independent of the
    /// predicate's position in a conjunction (it is a property of the predicate
    /// and the data it sees), which makes it the stable term to rank on.
    /// `None` until at least one row has been evaluated.
    pub fn cost_per_row_nanos(&self) -> Option<f64> {
        if self.rows_total == 0 {
            return None;
        }
        Some(self.eval_nanos as f64 / self.rows_total as f64)
    }

    /// Mean of the per-batch effectiveness samples (Welford `eff_mean`).
    ///
    /// `None` until at least one sample has been recorded. The unit is whatever
    /// the consumer fed to [`record`](Self::record); callers should not assume
    /// it.
    pub fn effectiveness(&self) -> Option<f64> {
        if self.sample_count == 0 {
            return None;
        }
        Some(self.eff_mean)
    }

    /// Number of per-batch effectiveness samples recorded so far.
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Sample variance of the per-batch effectiveness samples.
    ///
    /// Uses the unbiased (`n - 1`) estimator; `None` with fewer than 2 samples.
    fn variance(&self) -> Option<f64> {
        if self.sample_count < 2 {
            return None;
        }
        Some(self.eff_m2 / (self.sample_count - 1) as f64)
    }

    /// Lower bound of a one-sided confidence interval on mean effectiveness:
    /// `mean - z * stderr`.
    ///
    /// `z` is the caller's confidence multiplier (e.g. `2.0` ≈ 97.5% one-sided).
    /// `None` with fewer than 2 samples.
    pub fn confidence_lower_bound(&self, z: f64) -> Option<f64> {
        let stderr = (self.variance()? / self.sample_count as f64).sqrt();
        Some(self.eff_mean - z * stderr)
    }

    /// Upper bound of a one-sided confidence interval on mean effectiveness:
    /// `mean + z * stderr`.
    ///
    /// `None` with fewer than 2 samples.
    pub fn confidence_upper_bound(&self, z: f64) -> Option<f64> {
        let stderr = (self.variance()? / self.sample_count as f64).sqrt();
        Some(self.eff_mean + z * stderr)
    }

    /// Merge another accumulator's observations into this one, as if every
    /// batch recorded on `other` had been recorded here.
    ///
    /// The Welford state is combined with the standard parallel-variance
    /// formula (Chan et al.), so mean, variance and the confidence bounds match
    /// what a single accumulator over the union of samples would report. Lets
    /// concurrent consumers accumulate locally (lock-free) and periodically
    /// fold their pending observations into a shared accumulator.
    ///
    /// `self.is_optional` is preserved; it is a property of the predicate, not
    /// of the observations.
    pub fn merge(&mut self, other: &Self) {
        self.rows_matched += other.rows_matched;
        self.rows_total += other.rows_total;
        self.eval_nanos += other.eval_nanos;

        if other.sample_count == 0 {
            return;
        }
        if self.sample_count == 0 {
            self.sample_count = other.sample_count;
            self.eff_mean = other.eff_mean;
            self.eff_m2 = other.eff_m2;
            return;
        }
        let n1 = self.sample_count as f64;
        let n2 = other.sample_count as f64;
        let delta = other.eff_mean - self.eff_mean;
        self.eff_mean += delta * n2 / (n1 + n2);
        self.eff_m2 += other.eff_m2 + delta * delta * n1 * n2 / (n1 + n2);
        self.sample_count += other.sample_count;
    }

    /// Clear all accumulated observations, preserving [`is_optional`].
    ///
    /// Used when a predicate's identity changes underneath a stable
    /// [`FilterId`] — e.g. a dynamic filter that re-arms with new values — so
    /// stale measurements don't bias the new predicate.
    ///
    /// [`is_optional`]: Self::is_optional
    pub fn reset(&mut self) {
        *self = Self::new(self.is_optional);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stats_report_none() {
        let s = SelectivityStats::default();
        assert_eq!(s.pass_rate(), None);
        assert_eq!(s.cost_per_row_nanos(), None);
        assert_eq!(s.effectiveness(), None);
        assert_eq!(s.confidence_lower_bound(2.0), None);
        assert_eq!(s.sample_count(), 0);
        assert!(!s.is_optional());
    }

    #[test]
    fn pass_rate_and_cost_per_row() {
        let mut s = SelectivityStats::default();
        // 2 of 10 rows pass, 1000ns spent.
        s.record(2, 10, 1000, 0.0);
        assert_eq!(s.pass_rate(), Some(0.2));
        assert_eq!(s.cost_per_row_nanos(), Some(100.0));
        // accumulates across batches
        s.record(8, 10, 1000, 0.0);
        assert_eq!(s.pass_rate(), Some(0.5)); // 10/20
        assert_eq!(s.cost_per_row_nanos(), Some(100.0)); // 2000/20
    }

    #[test]
    fn welford_mean_matches_naive_average() {
        let mut s = SelectivityStats::default();
        let samples = [10.0, 20.0, 30.0, 40.0];
        for &x in &samples {
            s.record(1, 1, 1, x);
        }
        assert_eq!(s.sample_count(), 4);
        let mean = s.effectiveness().unwrap();
        assert!((mean - 25.0).abs() < 1e-9, "mean was {mean}");
    }

    #[test]
    fn welford_variance_matches_naive() {
        let mut s = SelectivityStats::default();
        let samples = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for &x in &samples {
            s.record(1, 1, 1, x);
        }
        // Known sample variance (n-1) of this set is 4.571428...
        let var = s.variance().unwrap();
        assert!((var - 32.0 / 7.0).abs() < 1e-9, "variance was {var}");
    }

    #[test]
    fn confidence_interval_brackets_mean() {
        let mut s = SelectivityStats::default();
        for &x in &[10.0, 12.0, 8.0, 11.0, 9.0] {
            s.record(1, 1, 1, x);
        }
        let mean = s.effectiveness().unwrap();
        let lo = s.confidence_lower_bound(2.0).unwrap();
        let hi = s.confidence_upper_bound(2.0).unwrap();
        assert!(lo < mean && mean < hi, "lo={lo} mean={mean} hi={hi}");
        // symmetric around the mean
        assert!(((hi - mean) - (mean - lo)).abs() < 1e-9);
    }

    #[test]
    fn ci_needs_two_samples() {
        let mut s = SelectivityStats::default();
        s.record(1, 1, 1, 5.0);
        assert_eq!(s.confidence_lower_bound(2.0), None);
        assert_eq!(s.confidence_upper_bound(2.0), None);
    }

    #[test]
    fn empty_or_zero_time_batches_are_not_samples() {
        let mut s = SelectivityStats::default();
        s.record(0, 0, 1000, 5.0); // no rows
        s.record(5, 10, 0, 5.0); // no time
        s.record(5, 10, 100, f64::NAN); // non-finite sample
        assert_eq!(s.sample_count(), 0);
        // but raw counters still moved where rows/time were present
        assert_eq!(s.pass_rate(), Some(0.5)); // from the latter two row-bearing batches
    }

    #[test]
    fn merge_matches_single_accumulator() {
        let samples = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mut whole = SelectivityStats::default();
        let mut left = SelectivityStats::default();
        let mut right = SelectivityStats::default();
        for (i, &x) in samples.iter().enumerate() {
            whole.record(1, 2, 10, x);
            if i < 3 {
                left.record(1, 2, 10, x);
            } else {
                right.record(1, 2, 10, x);
            }
        }
        left.merge(&right);
        assert_eq!(left.sample_count(), whole.sample_count());
        assert_eq!(left.pass_rate(), whole.pass_rate());
        assert_eq!(left.cost_per_row_nanos(), whole.cost_per_row_nanos());
        let (m1, m2) = (
            left.effectiveness().unwrap(),
            whole.effectiveness().unwrap(),
        );
        assert!((m1 - m2).abs() < 1e-9, "means {m1} vs {m2}");
        let (v1, v2) = (left.variance().unwrap(), whole.variance().unwrap());
        assert!((v1 - v2).abs() < 1e-9, "variances {v1} vs {v2}");
    }

    #[test]
    fn merge_with_empty_is_identity() {
        let mut s = SelectivityStats::default();
        s.record(2, 10, 100, 7.0);
        let snapshot = s;
        s.merge(&SelectivityStats::default());
        assert_eq!(s, snapshot);

        let mut empty = SelectivityStats::default();
        empty.merge(&snapshot);
        assert_eq!(empty, snapshot);
    }

    #[test]
    fn reset_preserves_optional_flag() {
        let mut s = SelectivityStats::new(true);
        s.record(2, 10, 1000, 7.0);
        s.reset();
        assert_eq!(s.sample_count(), 0);
        assert_eq!(s.pass_rate(), None);
        assert!(s.is_optional());
    }
}
