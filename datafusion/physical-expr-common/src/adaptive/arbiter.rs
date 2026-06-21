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

//! Shared champion/challenger coordination for adaptive filter execution.
//!
//! An *arrangement* is whatever a consumer can execute a batch of filters
//! with: the evaluation **order** of a conjunction (an adaptive
//! `FilterExec`), the **placement** of each filter as a row filter or a
//! post-scan filter (an adaptive parquet scan deciding late
//! materialization), or any other executable configuration. The arbiter is
//! deliberately ignorant of what the arrangement *means* — it only
//! coordinates the experiment that decides between two of them:
//!
//! - the **champion** is the arrangement validated by the most recent
//!   adopted trial, broadcast through a lock-free epoch so every stream
//!   notices an adoption with one atomic load per batch;
//! - a **trial** pits one candidate against the incumbent. Streams run the
//!   two arms on consecutive batches, time them end-to-end, and submit each
//!   pair's `ln(candidate / incumbent)` cost ratio (pairing cancels cold
//!   caches and concurrent-query interference, which otherwise dwarf the
//!   arms' true difference). The trial is shared: all streams feed the same
//!   ledger, so streams shorter than a whole trial still produce a verdict;
//! - the candidate is **adopted** only if its mean log-ratio is below zero
//!   with statistical confidence; ties favour the incumbent. A **rejected**
//!   candidate is remembered so the consumer can avoid re-running a lost
//!   experiment.
//!
//! What to propose, when to measure, and how to schedule re-checks is the
//! consumer's policy; the arbiter only guarantees that arrangements change
//! solely on a measured, shared, statistically separated win.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::stats::SelectivityStats;

/// Paired samples before a trial verdict. The challenger must beat the
/// incumbent with confidence within this many pairs or it is rejected (ties
/// favour the incumbent).
pub const TRIAL_PAIRS: u64 = 8;
/// Confidence multiplier for the interval on the mean paired log-ratio
/// (~95% two-sided at 2.0).
const CONFIDENCE_Z: f64 = 2.0;

/// The arbiter's answer to a submitted trial pair.
#[derive(Debug, Clone, PartialEq)]
pub enum TrialUpdate<A> {
    /// The trial needs more pairs.
    Running,
    /// This pair concluded the trial: the candidate won and is now the
    /// champion (the epoch has been bumped for the other streams).
    Adopted(A),
    /// This pair concluded the trial: the incumbent stands, and the
    /// candidate is remembered as rejected.
    Rejected(A),
    /// The trial this pair belonged to no longer exists (it concluded on
    /// another stream, or a different candidate's trial replaced it).
    Superseded,
}

/// A challenger arrangement under end-to-end A/B test against the incumbent.
#[derive(Debug)]
struct Trial<A> {
    candidate: A,
    /// Welford accumulator over per-pair `ln(candidate / incumbent)` cost
    /// ratios. Negative mean means the candidate is faster.
    pairs: SelectivityStats,
}

#[derive(Debug)]
struct State<A> {
    champion: Option<A>,
    rejected: Option<A>,
    trial: Option<Trial<A>>,
}

impl<A> Default for State<A> {
    fn default() -> Self {
        Self {
            champion: None,
            rejected: None,
            trial: None,
        }
    }
}

/// Champion/challenger coordination shared by every stream of one consumer
/// (e.g. all partition streams of a `FilterExec`, or all row-group streams of
/// a parquet scan). See the [module docs](self).
#[derive(Debug)]
pub struct AdaptiveArbiter<A> {
    /// Champion epoch; bumped on every adoption. Kept outside the lock so the
    /// per-batch staleness check is a single relaxed atomic load.
    epoch: AtomicU64,
    state: Mutex<State<A>>,
}

impl<A> Default for AdaptiveArbiter<A> {
    fn default() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            state: Mutex::new(State::default()),
        }
    }
}

impl<A: Clone + PartialEq> AdaptiveArbiter<A> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The current champion epoch (0 until a first adoption). A stream that
    /// cached an older value should call [`champion`](Self::champion).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// The current champion, if any stream's trial has adopted one, together
    /// with the epoch it was read at.
    pub fn champion(&self) -> (Option<A>, u64) {
        let state = self.state.lock();
        // Read the epoch under the lock so a concurrent adoption is not
        // missed between the load and the clone.
        (state.champion.clone(), self.epoch.load(Ordering::Relaxed))
    }

    /// The most recently rejected candidate, for the consumer's
    /// "is this the same lost experiment re-proposed?" gate.
    pub fn rejected(&self) -> Option<A> {
        self.state.lock().rejected.clone()
    }

    /// Whether a trial is currently in progress.
    pub fn trial_in_progress(&self) -> bool {
        self.state.lock().trial.is_some()
    }

    /// Start a trial for `candidate`, or join the trial already in progress
    /// (its candidate stands in for the proposal: a verdict on any change
    /// beats racing experiments against each other). Returns the candidate
    /// actually under trial.
    pub fn begin_trial(&self, candidate: A) -> A {
        let mut state = self.state.lock();
        match &state.trial {
            Some(trial) => trial.candidate.clone(),
            None => {
                state.trial = Some(Trial {
                    candidate: candidate.clone(),
                    pairs: SelectivityStats::default(),
                });
                candidate
            }
        }
    }

    /// Submit one paired observation for `candidate`: `ln_ratio` is
    /// `ln(candidate cost / incumbent cost)` measured on consecutive batches
    /// of the same stream (`rows`/`nanos` describe the candidate leg). Returns
    /// what this pair did to the trial; on [`TrialUpdate::Adopted`] the
    /// submitting stream has already seen the new champion (its epoch is
    /// current), while other streams notice via [`epoch`](Self::epoch).
    pub fn submit_pair(
        &self,
        candidate: &A,
        rows: u64,
        nanos: u64,
        ln_ratio: f64,
    ) -> TrialUpdate<A> {
        let mut state = self.state.lock();
        let Some(trial) = &mut state.trial else {
            return TrialUpdate::Superseded;
        };
        if trial.candidate != *candidate {
            return TrialUpdate::Superseded;
        }
        trial.pairs.record(0, rows, nanos, ln_ratio);
        if trial.pairs.sample_count() < TRIAL_PAIRS {
            return TrialUpdate::Running;
        }
        // Verdict: adopt only if the candidate is faster with confidence —
        // the whole CI of the mean log-ratio below zero.
        let adopted = matches!(
            trial.pairs.confidence_upper_bound(CONFIDENCE_Z),
            Some(up) if up < 0.0
        );
        let trial = state.trial.take().expect("checked above");
        if adopted {
            state.champion = Some(trial.candidate.clone());
            state.rejected = None;
            self.epoch.fetch_add(1, Ordering::Relaxed);
            TrialUpdate::Adopted(trial.candidate)
        } else {
            state.rejected = Some(trial.candidate.clone());
            TrialUpdate::Rejected(trial.candidate)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_trial(arbiter: &AdaptiveArbiter<u32>, candidate: u32, ln_ratio: f64) {
        assert_eq!(arbiter.begin_trial(candidate), candidate);
        for i in 0..TRIAL_PAIRS {
            let update =
                arbiter.submit_pair(&candidate, 1000, 1000, ln_ratio + i as f64 * 0.01);
            if i + 1 < TRIAL_PAIRS {
                assert_eq!(update, TrialUpdate::Running);
            } else {
                match update {
                    TrialUpdate::Adopted(a) | TrialUpdate::Rejected(a) => {
                        assert_eq!(a, candidate)
                    }
                    other => panic!("trial must conclude, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn adoption_publishes_champion_and_bumps_epoch() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        assert_eq!(arbiter.epoch(), 0);
        run_trial(&arbiter, 7, -10.0); // decisively faster
        assert_eq!(arbiter.champion(), (Some(7), 1));
        assert_eq!(arbiter.rejected(), None);
    }

    #[test]
    fn rejection_keeps_incumbent_and_remembers_candidate() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        run_trial(&arbiter, 7, 10.0); // decisively slower
        assert_eq!(arbiter.champion(), (None, 0));
        assert_eq!(arbiter.rejected(), Some(7));
    }

    #[test]
    fn ties_favour_the_incumbent() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        // Samples straddle zero with overlapping CI: not a confident win.
        let candidate = 7;
        arbiter.begin_trial(candidate);
        for i in 0..TRIAL_PAIRS {
            let sample = if i % 2 == 0 { 0.5 } else { -0.5 };
            let update = arbiter.submit_pair(&candidate, 1000, 1000, sample);
            if i + 1 == TRIAL_PAIRS {
                assert_eq!(update, TrialUpdate::Rejected(candidate));
            }
        }
    }

    #[test]
    fn adoption_clears_the_rejected_memory() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        run_trial(&arbiter, 7, 10.0);
        assert_eq!(arbiter.rejected(), Some(7));
        run_trial(&arbiter, 8, -10.0);
        assert_eq!(arbiter.rejected(), None);
        assert_eq!(arbiter.champion(), (Some(8), 1));
    }

    #[test]
    fn concurrent_proposals_join_the_active_trial() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        assert_eq!(arbiter.begin_trial(7), 7);
        // A second proposal joins the running trial instead of replacing it.
        assert_eq!(arbiter.begin_trial(9), 7);
    }

    #[test]
    fn pairs_for_a_stale_candidate_are_superseded() {
        let arbiter = AdaptiveArbiter::<u32>::new();
        assert_eq!(
            arbiter.submit_pair(&7, 1000, 1000, 0.0),
            TrialUpdate::Superseded
        );
        arbiter.begin_trial(7);
        assert_eq!(
            arbiter.submit_pair(&9, 1000, 1000, 0.0),
            TrialUpdate::Superseded
        );
    }
}
