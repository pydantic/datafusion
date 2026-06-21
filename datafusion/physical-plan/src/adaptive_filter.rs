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

//! Runtime-adaptive evaluation of a conjunctive (`AND`) predicate for
//! [`FilterExec`](crate::filter::FilterExec).
//!
//! This is the *ordering* policy that sits on top of the shared measurement
//! substrate in
//! [`datafusion_physical_expr_common::adaptive`]. The substrate
//! ([`AdaptiveStatsRegistry`]) measures per-conjunct selectivity and cost; this
//! module decides, from those measurements, what order to evaluate the
//! conjuncts in.
//!
//! ## How it evaluates
//!
//! The conjuncts are evaluated sequentially, combining their boolean results
//! with `AND`. The working batch is physically compacted to the surviving rows
//! only once the accumulated mask becomes selective enough (the same
//! pre-selection gate [`BinaryExpr`]'s `AND` short-circuit uses) — so a run of
//! non-selective conjuncts costs only cheap bitwise `AND`s, while a selective
//! conjunct shrinks the batch the expensive conjuncts after it must decode.
//! Gating is what makes ordering matter: a conjunct that compacts early saves
//! every later conjunct work, so the cheap-and-selective ones should run first.
//! Each conjunct is timed and counted on exactly the rows it evaluated, giving
//! the *marginal* selectivity and cost on the current working population.
//!
//! ## How it reorders: the model proposes, a trial disposes
//!
//! Every conjunct accrues a per-batch *effectiveness* sample of **rows
//! discarded per second** (`(total - matched) * 1e9 / eval_nanos`). Maximising
//! discards-per-second is exactly minimising `cost_per_row / (1 - pass_rate)`,
//! the classic optimal ordering key for independent conjuncts — so an expensive
//! but very selective predicate (e.g. a `LIKE` that keeps one row) correctly
//! sorts ahead of a cheap but unselective one.
//!
//! Crucially, this ranking is only ever a *proposal*. The classic key is
//! built on assumptions real execution violates: it prices a conjunct's work
//! as `rows × cost_per_row` even though vectorised kernels do not scale down
//! without paying for compaction; and the marginal cost-per-row measured on a
//! handful of survivor rows says little about cost at the front of the order.
//! An order that looks optimal on paper can run slower than the one the query
//! author wrote. So a proposed order is never adopted on the model's word: it
//! is put through an **A/B trial** — batches alternate between the incumbent
//! and the candidate arrangement, each timed end-to-end — and the candidate
//! wins only if its measured ns/row beats the incumbent's with statistical
//! confidence. Ties favour the incumbent; a rejected candidate is remembered
//! and not re-tried. Adopted orders run via the compact-once loop (the
//! arrangement the trial actually measured), not re-fused into a chain with
//! different cost behaviour.
//!
//! Proposals themselves are debounced: the learner only settles once every
//! adjacent pair of the ranking is either statistically certain or a tie
//! whose order provably cannot matter, and a proposal that does not promise a
//! material improvement over the incumbent freezes the incumbent unchanged —
//! interchangeable conjuncts never trigger an experiment at all.
//!
//! To stay correct under distribution drift, a frozen evaluator periodically
//! *re-thaws*: it re-measures a short window and re-proposes. Each re-thaw
//! that keeps the incumbent backs the next one off exponentially, so a stable
//! filter is re-checked geometrically less often and steady-state overhead
//! decays toward zero; a candidate that wins its trial resets the interval so
//! real drift is caught quickly.
//!
//! All partition streams of a `FilterExec` share their measurements and trial
//! verdicts, so the learning cost is paid once per query, not once per
//! stream, and a stream that starts late adopts the validated champion on its
//! first batch.
//!
//! ## Known limitation
//!
//! The measured selectivity of a conjunct is *conditional* on the conjuncts
//! ordered before it (it only sees their survivors), so with strongly
//! correlated predicates the *proposal* can be a local optimum — the trial
//! protects against adopting a bad one, but a better unexplored order may be
//! missed. A proper exploration phase (measuring each conjunct's marginal
//! selectivity on a common population) is future work.
//!
//! [`BinaryExpr`]: datafusion_physical_expr::expressions::BinaryExpr

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::and;
use arrow::compute::{filter, filter_record_batch, prep_null_mask_filter};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr_common::adaptive::{
    AdaptiveArbiter, AdaptiveStatsRegistry, SelectivityStats, TrialUpdate,
};
use datafusion_physical_expr_common::physical_expr::is_volatile;
use log::debug;

/// Confidence multiplier for the one-sided interval on effectiveness
/// (~97.5% one-sided at 2.0).
const CONFIDENCE_Z: f64 = 2.0;
/// Minimum per-conjunct samples before the confidence intervals are trusted
/// enough to make a freeze decision.
const MIN_SAMPLES_FOR_CI: u64 = 4;
/// Freeze after this many learning samples even if the order's confidence
/// intervals still overlap: if the conjuncts cannot be told apart by now, their
/// relative order does not matter, so stop paying to measure it.
const MAX_LEARNING_SAMPLES: u64 = 64;
/// Batches the fast frozen path runs before the first re-thaw check.
const INITIAL_THAW_INTERVAL: u64 = 64;
/// Each re-thaw that confirms the order is unchanged multiplies the next
/// interval by this factor (exponential backoff), so a stable filter is
/// re-checked geometrically less often and steady-state overhead decays to ~0.
const THAW_BACKOFF: u64 = 4;
/// Upper bound on the re-thaw interval.
const MAX_THAW_INTERVAL: u64 = 16_384;
/// Batches measured during a re-thaw before re-deciding the order.
const REMEASURE_WINDOW: u64 = 16;
/// Measured batches at the start of each measuring phase (learning and
/// re-measure windows) that run back-to-back, so freeze decisions are
/// reachable quickly even on short streams.
const MEASURE_WARMUP: u64 = 8;
/// After the warmup, measure one batch in this many; the rest evaluate the
/// fused predicate in the current order with no instrumentation. Bounds the
/// steady measurement overhead even when the order never resolves.
const MEASURE_STRIDE: u64 = 8;
/// Fraction of the conjunction's expected total cost below which a difference
/// is immaterial. Adjacent conjuncts whose swap could not change the expected
/// cost by more than this are a *tie*: their relative order does not matter,
/// so an unresolved tie neither delays freezing nor counts as drift when a
/// re-thaw re-ranks them.
const TIE_COST_FRACTION: f64 = 0.05;
/// Physically compact the working batch to the surviving rows only when the
/// accumulated mask keeps at most this fraction of them. Above this, the cost
/// of materializing a barely-smaller batch is not repaid, so we keep evaluating
/// against the full working batch and just AND the boolean masks — mirroring
/// the pre-selection gate in `BinaryExpr`'s `AND` short-circuit.
const COMPACTION_SELECTIVITY_THRESHOLD: f64 = 0.2;
/// Paired samples before a trial verdict. The challenger must beat the
/// incumbent with confidence within this many pairs or it is rejected (ties
/// favour the incumbent).
/// How an arrangement evaluates its conjuncts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Strategy {
    /// A single left-deep `AND` of [`BinaryExpr`]s — byte-for-byte what
    /// `FilterExec` runs when the feature is off. The incumbent starts here so
    /// that doing nothing costs nothing.
    Fused,
    /// The compact-once loop ([`eval_conjuncts`]): masks are `AND`-combined
    /// and the working batch is physically compacted at most when the
    /// survivors drop below the pre-selection threshold, instead of
    /// re-filtering and re-scattering at every `AND` level like the fused
    /// chain does. Orders adopted by a trial run this way, since this is the
    /// strategy the trial measured.
    CompactOnce,
}

/// The executable arrangement an adaptive `FilterExec` decides between: an
/// evaluation order plus the strategy that runs it. This is the type the
/// shared [`AdaptiveArbiter`] coordinates trials over; the arbiter itself is
/// arrangement-agnostic (a parquet scan would put filter *placement* here
/// instead).
#[derive(Debug, Clone, PartialEq)]
struct Arrangement {
    /// Evaluation order: indices into the conjunct list.
    order: Vec<usize>,
    /// How the order is executed.
    strategy: Strategy,
}

/// Lifecycle of the adaptive evaluator.
#[derive(Debug)]
enum Phase {
    /// Measuring batches (warmup, then strided), building confidence in the
    /// per-conjunct statistics that *propose* a candidate order.
    Learning,
    /// Participating in the shared A/B trial (see [`AdaptiveArbiter`]):
    /// alternating arms on consecutive batches and submitting paired
    /// end-to-end timings. Only a measured win changes the order — the
    /// per-conjunct cost model proposes, but never decides.
    Trial {
        /// The candidate arrangement, copied out of the shared trial so the
        /// hot path does not lock to learn what to evaluate.
        candidate: Arrangement,
        /// ns/row of the incumbent leg of the current pair, once run.
        pending_incumbent: Option<f64>,
        /// Freeze interval to use if the candidate is rejected (carries this
        /// stream's re-thaw backoff); an adopted candidate resets it.
        interval_if_rejected: u64,
    },
    /// Order settled; the incumbent arrangement is evaluated with no
    /// measurement until `thaw_at` batches have been processed.
    Frozen {
        /// Batch count at which to re-measure.
        thaw_at: u64,
        /// Interval that produced `thaw_at`; grows on each confirmation.
        interval: u64,
    },
    /// Briefly measuring again after a thaw, to detect distribution drift.
    Remeasuring {
        /// Measured batches left in the re-measurement window.
        window_left: u64,
        /// The frozen interval before this thaw (for backoff bookkeeping).
        interval: u64,
    },
}

/// State shared by every partition stream of one `FilterExec`: the
/// per-conjunct measurement registry plus the arrangement arbiter, so the
/// streams learn as one and one stream's completed trial serves all of them.
#[derive(Debug, Default)]
pub(crate) struct AdaptiveFilterShared {
    /// Per-conjunct selectivity/cost stats, keyed by conjunct index.
    stats: AdaptiveStatsRegistry,
    /// Champion/challenger coordination over [`Arrangement`]s.
    arbiter: AdaptiveArbiter<Arrangement>,
}

impl AdaptiveFilterShared {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// Adaptive evaluator for a single conjunctive predicate.
///
/// Owned per partition stream (single-threaded), so all state — order, stats,
/// and lifecycle [`Phase`] — is held directly and mutated through `&mut self`
/// with no locking. The stats are a plain `Vec` indexed by conjunct id (ids are
/// dense `0..n`), so the per-batch hot path is a direct index, not a locked map
/// lookup.
///
/// What *is* shared is the measurements and the trial verdicts: per-batch
/// observations accumulate in the stream-local `stats` (lock-free) and are
/// folded into the [`AdaptiveFilterShared`] registry whenever this stream
/// makes a decision, so N streams converge ~N× faster; and the first stream
/// whose trial validates (or rejects) a candidate publishes the verdict so the
/// others adopt it without re-running the experiment. Drift decisions
/// (re-thaw windows) deliberately use only the local fresh window — the shared
/// accumulator is a long-run prior and would dilute a distribution change.
#[derive(Debug)]
pub(crate) struct AdaptiveConjunction {
    /// The conjuncts. Stats/order indices refer to positions in this `Vec`.
    conjuncts: Vec<Arc<dyn PhysicalExpr>>,
    /// Per-conjunct observations accumulated locally since the last flush to
    /// [`shared`](Self::shared), indexed by conjunct id.
    stats: Vec<SelectivityStats>,
    /// Measurements and verdicts shared by every partition stream of the same
    /// `FilterExec`.
    shared: Arc<AdaptiveFilterShared>,
    /// Champion epoch this stream has caught up to (see
    /// [`AdaptiveArbiter::epoch`]).
    epoch_seen: u64,
    /// Incumbent evaluation order: indices into [`conjuncts`](Self::conjuncts).
    order: Vec<usize>,
    /// Incumbent evaluation strategy. Starts as [`Strategy::Fused`] over the
    /// written order (identical to the feature being off); becomes
    /// [`Strategy::CompactOnce`] when a trial adopts a new arrangement.
    strategy: Strategy,
    /// The conjuncts fused into a left-deep `AND` in [`order`](Self::order),
    /// used by [`Strategy::Fused`] and by unmeasured learning batches.
    /// Rebuilt when the order changes.
    fused: Arc<dyn PhysicalExpr>,
    /// Total batches processed; drives the re-thaw schedule.
    batches: u64,
    /// Measured batches in the current measuring phase; drives the
    /// warmup-then-stride measurement schedule.
    measured: u64,
    /// Current lifecycle phase.
    phase: Phase,
}

impl AdaptiveConjunction {
    /// Build an adaptive evaluator for `predicate`, or `None` if adaptive
    /// reordering does not apply:
    ///
    /// - `enabled` is false (the config flag is off);
    /// - the predicate has fewer than two `AND` conjuncts (nothing to reorder);
    /// - any conjunct is volatile (reordering could change results).
    ///
    /// `shared` is the state common to all partition streams of the owning
    /// `FilterExec`; the conjuncts are registered in it under their indices.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        enabled: bool,
        shared: Arc<AdaptiveFilterShared>,
    ) -> Option<Self> {
        if !enabled {
            return None;
        }
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(predicate)
            .into_iter()
            .map(Arc::clone)
            .collect();
        if conjuncts.len() < 2 {
            return None;
        }
        if conjuncts.iter().any(is_volatile) {
            return None;
        }

        shared
            .stats
            .register_all((0..conjuncts.len()).map(|id| (id, false)));
        let stats = vec![SelectivityStats::new(false); conjuncts.len()];
        let order: Vec<usize> = (0..conjuncts.len()).collect();
        let fused = fuse(&conjuncts, &order);

        Some(Self {
            conjuncts,
            stats,
            shared,
            epoch_seen: 0,
            order,
            strategy: Strategy::Fused,
            fused,
            batches: 0,
            measured: 0,
            phase: Phase::Learning,
        })
    }

    /// Evaluate the conjunction against `batch`, returning the boolean mask
    /// (over the batch's original rows) of rows that passed every conjunct.
    ///
    /// While [`Learning`](Phase::Learning) or [`Remeasuring`](Phase::Remeasuring),
    /// a warmup-then-strided subset of batches is evaluated and measured
    /// per-conjunct (see [`evaluate_measured`](Self::evaluate_measured));
    /// [`Trial`](Phase::Trial) batches alternate between the incumbent and the
    /// candidate arrangement, timed end-to-end; all other batches — including
    /// the whole [`Frozen`](Phase::Frozen) phase — evaluate the incumbent with
    /// no instrumentation.
    pub(crate) fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.batches += 1;

        // Adopt a champion another stream's trial has validated since we last
        // looked: a relaxed atomic load per batch, a lock only on change.
        if self.shared.arbiter.epoch() != self.epoch_seen {
            self.adopt_shared_champion();
        }

        // Frozen fast path; when the interval elapses, drop into a fresh
        // measurement window to check whether the data has drifted.
        if let Phase::Frozen { thaw_at, interval } = self.phase {
            if self.batches < thaw_at {
                return self.evaluate_incumbent(batch);
            }
            self.stats.iter_mut().for_each(SelectivityStats::reset);
            self.measured = 0;
            self.phase = Phase::Remeasuring {
                window_left: REMEASURE_WINDOW,
                interval,
            };
        }

        if matches!(self.phase, Phase::Trial { .. }) {
            return self.evaluate_trial(batch);
        }

        // Learning or re-measuring: measure a warmup of consecutive batches,
        // then only one batch per stride, so an order that stays unresolved
        // costs a bounded fraction of the stream rather than all of it.
        if self.measured >= MEASURE_WARMUP && !self.batches.is_multiple_of(MEASURE_STRIDE)
        {
            return self.evaluate_incumbent(batch);
        }

        self.measured += 1;
        let result = self.evaluate_measured(batch)?;
        self.update_phase();
        Ok(result)
    }

    /// Evaluate the incumbent arrangement with no instrumentation.
    fn evaluate_incumbent(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        match self.strategy {
            Strategy::Fused => self.fused.evaluate(batch)?.into_array(batch.num_rows()),
            Strategy::CompactOnce => {
                eval_conjuncts(&self.conjuncts, &self.order, batch, None)
            }
        }
    }

    /// Install the trial-validated champion published by another stream and
    /// freeze on it: the experiment has already been run, re-running it here
    /// would only repeat the cost.
    fn adopt_shared_champion(&mut self) {
        let (champion, epoch) = self.shared.arbiter.champion();
        self.epoch_seen = epoch;
        if let Some(champion) = champion {
            self.set_order(champion.order);
            self.strategy = champion.strategy;
            self.freeze(INITIAL_THAW_INTERVAL);
        }
    }

    /// Contribute one batch to the shared A/B trial. Each stream runs the
    /// incumbent and the candidate on consecutive batches, timing both, and
    /// submits the pair's `ln(candidate / incumbent)` ns/row ratio; once
    /// enough pairs accumulate (across all streams) the trial concludes. The
    /// candidate is adopted only if the mean log-ratio is below zero with
    /// confidence; ties favour the incumbent.
    fn evaluate_trial(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let Phase::Trial {
            candidate,
            pending_incumbent,
            interval_if_rejected,
        } = &mut self.phase
        else {
            unreachable!("caller checked the phase");
        };
        let interval_if_rejected = *interval_if_rejected;
        let rows = batch.num_rows();

        // Incumbent leg first; no locking on either leg.
        let Some(incumbent_ns) = *pending_incumbent else {
            let timer = Instant::now();
            let result = match self.strategy {
                Strategy::Fused => {
                    self.fused.evaluate(batch)?.into_array(batch.num_rows())
                }
                Strategy::CompactOnce => {
                    eval_conjuncts(&self.conjuncts, &self.order, batch, None)
                }
            }?;
            let nanos = timer.elapsed().as_nanos() as u64;
            if rows > 0
                && nanos > 0
                && let Phase::Trial {
                    pending_incumbent, ..
                } = &mut self.phase
            {
                *pending_incumbent = Some(nanos as f64 / rows as f64);
            }
            return Ok(result);
        };

        // Candidate leg; completes the pair.
        let candidate = std::mem::replace(
            candidate,
            Arrangement {
                order: Vec::new(),
                strategy: Strategy::CompactOnce,
            },
        );
        let timer = Instant::now();
        let result = eval_conjuncts(&self.conjuncts, &candidate.order, batch, None)?;
        let nanos = timer.elapsed().as_nanos() as u64;
        let sample = (rows > 0 && nanos > 0)
            .then(|| (nanos as f64 / rows as f64 / incumbent_ns).ln())
            .filter(|s| s.is_finite());

        let Some(sample) = sample else {
            // Not a usable pair; start the next one.
            self.phase = Phase::Trial {
                candidate,
                pending_incumbent: None,
                interval_if_rejected,
            };
            return Ok(result);
        };
        match self
            .shared
            .arbiter
            .submit_pair(&candidate, rows as u64, nanos, sample)
        {
            TrialUpdate::Running => {
                self.phase = Phase::Trial {
                    candidate,
                    pending_incumbent: None,
                    interval_if_rejected,
                };
            }
            TrialUpdate::Adopted(champion) => {
                debug!("adaptive filter trial adopted {champion:?}");
                self.epoch_seen = self.shared.arbiter.epoch();
                self.set_order(champion.order);
                self.strategy = champion.strategy;
                self.freeze(INITIAL_THAW_INTERVAL);
            }
            // Rejected here, or concluded on another stream (an adoption
            // there is handled by the epoch check): the incumbent stands.
            TrialUpdate::Rejected(rejected) => {
                debug!("adaptive filter trial rejected {rejected:?}");
                self.freeze(interval_if_rejected);
            }
            TrialUpdate::Superseded => self.freeze(interval_if_rejected),
        }
        Ok(result)
    }

    /// Evaluate the conjuncts in the incumbent order via the compact-once
    /// loop, recording each conjunct's marginal selectivity and cost into the
    /// stream-local stats.
    fn evaluate_measured(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        eval_conjuncts(&self.conjuncts, &self.order, batch, Some(&mut self.stats))
    }

    /// Advance the lifecycle phase after measuring a batch:
    /// - [`Learning`](Phase::Learning) folds the local observations into the
    ///   shared registry and proposes a candidate once the *shared* stats
    ///   settle the ranking (see [`settled_order`]), so all partition streams
    ///   learn as one;
    /// - [`Remeasuring`](Phase::Remeasuring) proposes from the *local* fresh
    ///   window only when its window ends (the shared accumulator is a
    ///   long-run prior that would dilute a distribution change), then folds
    ///   the window into the shared registry.
    ///
    /// Either way the proposal goes through [`decide`](Self::decide): a
    /// proposal that does not promise a material improvement freezes the
    /// incumbent unchanged, and one that does is handed to an A/B
    /// [`Trial`](Phase::Trial) rather than adopted on the model's word.
    fn update_phase(&mut self) {
        match std::mem::replace(&mut self.phase, Phase::Learning) {
            Phase::Learning => {
                self.flush_to_shared();
                let stats = self.shared_snapshot();
                if let Some(order) = settled_order(&stats) {
                    self.decide(order, &stats, INITIAL_THAW_INTERVAL);
                }
                // else: stay in `Learning` (already restored by the replace).
            }
            Phase::Remeasuring {
                window_left,
                interval,
            } => {
                let window_left = window_left - 1;
                if window_left == 0 {
                    let candidate = rank_by_effectiveness(&self.stats);
                    let stats = std::mem::take(&mut self.stats);
                    let backoff =
                        interval.saturating_mul(THAW_BACKOFF).min(MAX_THAW_INTERVAL);
                    self.decide(candidate, &stats, backoff);
                    self.stats = stats;
                    self.flush_to_shared();
                } else {
                    self.phase = Phase::Remeasuring {
                        window_left,
                        interval,
                    };
                }
            }
            // Not reachable: frozen and trial batches never take the measured
            // path. Restore the phase.
            other => self.phase = other,
        }
    }

    /// Act on a proposed `candidate` order, judged against `stats`:
    ///
    /// - the incumbent's order (or an immaterial reshuffle of it, per
    ///   [`TIE_COST_FRACTION`]) freezes the incumbent for
    ///   `interval_if_unchanged` batches — ties never trigger an experiment;
    /// - a candidate the shared verdicts already rejected does the same — the
    ///   experiment has been run and lost;
    /// - otherwise the candidate enters an A/B [`Trial`](Phase::Trial). The
    ///   per-conjunct stats only ever *propose*; adoption requires the trial's
    ///   measured end-to-end win.
    fn decide(
        &mut self,
        candidate: Vec<usize>,
        stats: &[SelectivityStats],
        interval_if_unchanged: u64,
    ) {
        let materially_better = expected_cost_per_row(stats, &candidate)
            < (1.0 - TIE_COST_FRACTION) * expected_cost_per_row(stats, &self.order);
        if candidate == self.order || !materially_better {
            self.freeze(interval_if_unchanged);
            return;
        }
        // A candidate whose modelled cost is not materially better than the
        // last rejected candidate's is the same experiment re-proposed —
        // with tied conjuncts the ranking reshuffles them freely, and an
        // exact-match memory would re-run the lost trial forever.
        if let Some(rejected) = self.shared.arbiter.rejected()
            && expected_cost_per_row(stats, &candidate)
                >= (1.0 - TIE_COST_FRACTION)
                    * expected_cost_per_row(stats, &rejected.order)
        {
            self.freeze(interval_if_unchanged);
            return;
        }
        // Start the shared trial, or join the one already in progress (its
        // candidate stands in for ours: proposals are made from the same
        // shared statistics, and a verdict on any reordering beats racing
        // experiments against each other).
        let candidate = self.shared.arbiter.begin_trial(Arrangement {
            order: candidate,
            strategy: Strategy::CompactOnce,
        });
        self.phase = Phase::Trial {
            candidate,
            pending_incumbent: None,
            interval_if_rejected: interval_if_unchanged,
        };
    }

    /// Fold the locally-accumulated observations into the shared registry and
    /// clear them.
    fn flush_to_shared(&mut self) {
        for (id, stats) in self.stats.iter_mut().enumerate() {
            if stats.sample_count() > 0 || stats.pass_rate().is_some() {
                self.shared.stats.merge(id, stats);
                stats.reset();
            }
        }
    }

    /// Copy out the shared stats for every conjunct.
    fn shared_snapshot(&self) -> Vec<SelectivityStats> {
        (0..self.conjuncts.len())
            .map(|id| self.shared.stats.snapshot(id).unwrap_or_default())
            .collect()
    }

    /// Install `order` as the incumbent order, rebuilding the fused predicate
    /// if it changed.
    fn set_order(&mut self, order: Vec<usize>) {
        if order != self.order {
            self.order = order;
            self.fused = fuse(&self.conjuncts, &self.order);
        }
    }

    /// Freeze the incumbent, due to re-measure after `interval` batches.
    fn freeze(&mut self, interval: u64) {
        self.phase = Phase::Frozen {
            thaw_at: self.batches + interval,
            interval,
        };
    }
}

/// Evaluate `conjuncts` in `order` against `batch` via the compact-once loop,
/// returning the boolean mask (over the batch's original rows) of rows that
/// passed every conjunct. With `stats`, each conjunct is additionally timed
/// and counted on exactly the rows it evaluated (its *marginal* selectivity
/// and cost on the current working population).
///
/// The working batch is physically compacted to the surviving rows only once
/// the accumulated mask becomes selective enough (see
/// [`COMPACTION_SELECTIVITY_THRESHOLD`]); until then masks are combined with a
/// cheap bitwise `AND`, so a run of non-selective conjuncts pays no
/// materialization cost. Unlike the fused `BinaryExpr` chain, survivors stay
/// compacted across the remaining conjuncts instead of being re-filtered and
/// re-scattered at every `AND` level.
///
/// The bookkeeping is all deferred so the common shapes cost what the fused
/// chain would: an all-true mask is dropped without an `AND` merge, the
/// row-index array mapping survivors back to original rows is only
/// materialized at the first compaction, and the final scatter only happens
/// if a compaction occurred (otherwise the accumulated mask already covers
/// the original rows).
fn eval_conjuncts(
    conjuncts: &[Arc<dyn PhysicalExpr>],
    order: &[usize],
    batch: &RecordBatch,
    mut stats: Option<&mut [SelectivityStats]>,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
    }

    // `working` is the batch conjuncts are evaluated against. `acc` is the
    // accumulated (`AND`-combined, null-free) result over `working`'s rows
    // since the last compaction; `None` means all of them are still live.
    // `live` maps `working`'s rows back to original row indices; `None`
    // until a compaction first drops rows.
    let mut working = batch.clone();
    let mut acc: Option<BooleanArray> = None;
    let mut live: Option<ArrayRef> = None;

    for &id in order {
        let rows_in = working.num_rows();

        let timer = stats.is_some().then(Instant::now);
        let array = conjuncts[id].evaluate(&working)?.into_array(rows_in)?;
        let mask = as_boolean_array(&array)?;
        // `matched` counts non-null trues (SQL filter semantics).
        let matched = mask.true_count() as u64;

        if let (Some(stats), Some(timer)) = (stats.as_deref_mut(), timer) {
            let eval_nanos = timer.elapsed().as_nanos() as u64;
            let discarded = rows_in as u64 - matched;
            let sample = if eval_nanos > 0 {
                discarded as f64 * 1e9 / eval_nanos as f64
            } else {
                0.0
            };
            stats[id].record(matched, rows_in as u64, eval_nanos, sample);
        }

        // An all-true mask leaves the accumulated result untouched.
        if matched == rows_in as u64 && mask.null_count() == 0 {
            continue;
        }

        // Fold this conjunct into the accumulated mask (null -> false).
        let mask = if mask.null_count() > 0 {
            prep_null_mask_filter(mask)
        } else {
            mask.clone()
        };
        let folded = match &acc {
            None => mask,
            Some(prev) => and(prev, &mask)?,
        };

        let alive = folded.true_count();
        if alive == 0 {
            // Nothing survives; the result is all-false over the original
            // rows no matter what the remaining conjuncts say.
            return Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new_unset(num_rows),
                None,
            )));
        }
        // Compact only when the survivors are a small fraction of the
        // working batch — otherwise the copy is not worth it.
        if (alive as f64) <= COMPACTION_SELECTIVITY_THRESHOLD * rows_in as f64 {
            working = filter_record_batch(&working, &folded)?;
            let indices = live.take().unwrap_or_else(|| {
                Arc::new(UInt32Array::from_iter_values(0..num_rows as u32))
            });
            live = Some(filter(&indices, &folded)?);
            acc = None;
        } else {
            acc = Some(folded);
        }
    }

    match live {
        // Never compacted: `acc` (or all-true) already covers the
        // original rows.
        None => Ok(match acc {
            Some(acc) => Arc::new(acc),
            None => Arc::new(BooleanArray::new(BooleanBuffer::new_set(num_rows), None)),
        }),
        // Compacted at least once: scatter the surviving original indices
        // (`live`, narrowed by any residual `acc`) into a full-length mask.
        Some(indices) => {
            let indices = match acc {
                Some(acc) => filter(&indices, &acc)?,
                None => indices,
            };
            let indices = indices
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("u32 live");
            let mut builder = BooleanBufferBuilder::new(num_rows);
            builder.append_n(num_rows, false);
            for &idx in indices.values() {
                builder.set_bit(idx as usize, true);
            }
            Ok(Arc::new(BooleanArray::new(builder.finish(), None)))
        }
    }
}

/// Decide whether `stats` settle the conjunct order enough to freeze.
/// Returns the order to freeze, or `None` to keep learning.
///
/// We freeze once every adjacent pair of the ranking is *resolved* — the
/// pair is either statistically certain or a tie whose order cannot matter
/// (see [`order_is_resolved`]) — or once enough samples have accrued that
/// more measurement is not worth its cost.
///
/// Conjuncts that have never received a row (everything upstream of them
/// was discarded) cannot be measured, but their position cannot matter
/// either, so they are excluded from the sample-count gates rather than
/// holding up freezing forever.
fn settled_order(stats: &[SelectivityStats]) -> Option<Vec<usize>> {
    let samples_of_measured = || {
        stats
            .iter()
            .filter(|s| s.pass_rate().is_some())
            .map(SelectivityStats::sample_count)
    };
    if samples_of_measured().min().unwrap_or(0) < MIN_SAMPLES_FOR_CI {
        return None;
    }
    let order = rank_by_effectiveness(stats);
    if order_is_resolved(stats, &order)
        || samples_of_measured().max().unwrap_or(0) >= MAX_LEARNING_SAMPLES
    {
        Some(order)
    } else {
        None
    }
}

/// Rank conjunct ids by mean effectiveness (discards-per-second) descending;
/// ids without samples sort last. Stable, so equal ids keep ascending order.
fn rank_by_effectiveness(stats: &[SelectivityStats]) -> Vec<usize> {
    let mut ids: Vec<usize> = (0..stats.len()).collect();
    ids.sort_by(
        |&a, &b| match (stats[a].effectiveness(), stats[b].effectiveness()) {
            (Some(x), Some(y)) => y.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        },
    );
    ids
}

/// Whether every adjacent pair in `order` is *resolved*: the ranking is
/// either statistically certain ([`pair_is_certain`]) or provably immaterial
/// ([`pair_is_tied`]). Only unresolved pairs — distinguishable conjuncts whose
/// order matters but whose measurements have not separated yet — justify more
/// measurement.
fn order_is_resolved(stats: &[SelectivityStats], order: &[usize]) -> bool {
    let total = expected_cost_per_row(stats, order);
    let mut weight = 1.0_f64;
    for pair in order.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if !pair_is_certain(stats, a, b) && !pair_is_tied(stats, a, b, weight, total) {
            return false;
        }
        weight *= stats[a].pass_rate().unwrap_or(1.0);
    }
    true
}

/// Whether `a` ranks above `b` with statistical certainty: their one-sided
/// effectiveness confidence intervals do not overlap.
fn pair_is_certain(stats: &[SelectivityStats], a: usize, b: usize) -> bool {
    match (
        stats[a].confidence_lower_bound(CONFIDENCE_Z),
        stats[b].confidence_upper_bound(CONFIDENCE_Z),
    ) {
        (Some(lo), Some(up)) => lo >= up,
        _ => false,
    }
}

/// Whether the order of adjacent conjuncts `a` and `b` is immaterial:
/// swapping them could not change the conjunction's expected cost by more
/// than [`TIE_COST_FRACTION`] of `total`. `weight` is the fraction of input
/// rows expected to reach the pair, `total` the conjunction's expected cost
/// per input row (see [`expected_cost_per_row`]).
///
/// Treats the conjuncts' pass rates as independent, like the optimal-order
/// ranking itself does.
fn pair_is_tied(
    stats: &[SelectivityStats],
    a: usize,
    b: usize,
    weight: f64,
    total: f64,
) -> bool {
    // A conjunct that has never received a row is unmeasurable, but rows
    // only stop reaching it when an upstream conjunct discards everything,
    // in which case its position cannot matter.
    let (Some(cost_a), Some(pass_a)) =
        (stats[a].cost_per_row_nanos(), stats[a].pass_rate())
    else {
        return true;
    };
    let (Some(cost_b), Some(pass_b)) =
        (stats[b].cost_per_row_nanos(), stats[b].pass_rate())
    else {
        return true;
    };
    // Expected cost of `a` before `b` vs `b` before `a`, on the rows that
    // reach the pair.
    let gain = weight * ((cost_a + pass_a * cost_b) - (cost_b + pass_b * cost_a)).abs();
    gain <= TIE_COST_FRACTION * total
}

/// Expected cost of evaluating the conjuncts in `order`, in nanoseconds per
/// input row: each conjunct's measured per-row cost, weighted by the
/// fraction of rows expected to reach it (the product of the pass rates of
/// the conjuncts ordered before it, treated as independent). Conjuncts with
/// no measurements contribute nothing — rows never reached them.
fn expected_cost_per_row(stats: &[SelectivityStats], order: &[usize]) -> f64 {
    let mut weight = 1.0_f64;
    let mut total = 0.0_f64;
    for &id in order {
        let (Some(cost), Some(pass)) =
            (stats[id].cost_per_row_nanos(), stats[id].pass_rate())
        else {
            continue;
        };
        total += weight * cost;
        weight *= pass;
    }
    total
}

/// Fuse `conjuncts` into a single left-deep `AND` in `order`, so unmeasured
/// batches evaluate as a normal predicate (inheriting `BinaryExpr`'s
/// short-circuit and pre-selection).
fn fuse(conjuncts: &[Arc<dyn PhysicalExpr>], order: &[usize]) -> Arc<dyn PhysicalExpr> {
    let mut it = order.iter().map(|&i| Arc::clone(&conjuncts[i]));
    let first = it.next().expect("at least two conjuncts");
    it.fold(first, |acc, e| {
        Arc::new(BinaryExpr::new(acc, Operator::And, e)) as Arc<dyn PhysicalExpr>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col, lit};
    use datafusion_physical_expr_common::adaptive::TRIAL_PAIRS;

    /// The conjunction arrangement for `order`, as trials propose it.
    fn arrangement(order: Vec<usize>) -> Arrangement {
        Arrangement {
            order,
            strategy: Strategy::CompactOnce,
        }
    }

    /// Drive a full trial for `order` through the arbiter's public protocol
    /// with decisive fake pairs (negative log-ratios adopt, positive reject).
    fn run_fake_trial(shared: &AdaptiveFilterShared, order: Vec<usize>, wins: bool) {
        let candidate = arrangement(order);
        assert_eq!(shared.arbiter.begin_trial(candidate.clone()), candidate);
        for i in 0..TRIAL_PAIRS {
            let jitter = i as f64 * 0.01;
            shared.arbiter.submit_pair(
                &candidate,
                1000,
                1000,
                if wins { -10.0 } else { 10.0 } + jitter,
            );
        }
    }

    fn test_batch(schema: &Arc<Schema>, a: Vec<i32>, b: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
        )
        .unwrap()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    /// `AdaptiveConjunction::try_new` with a fresh, unshared registry.
    fn try_new_unshared(
        predicate: &Arc<dyn PhysicalExpr>,
        enabled: bool,
    ) -> Option<AdaptiveConjunction> {
        AdaptiveConjunction::try_new(
            predicate,
            enabled,
            Arc::new(AdaptiveFilterShared::new()),
        )
    }

    /// `a > 2 AND b < 5`
    fn predicate(schema: &Arc<Schema>) -> Arc<dyn PhysicalExpr> {
        let left =
            binary(col("a", schema).unwrap(), Operator::Gt, lit(2i32), schema).unwrap();
        let right =
            binary(col("b", schema).unwrap(), Operator::Lt, lit(5i32), schema).unwrap();
        binary(left, Operator::And, right, schema).unwrap()
    }

    fn passing_rows(mask: &ArrayRef) -> Vec<usize> {
        let mask = as_boolean_array(mask).unwrap();
        (0..mask.len()).filter(|&i| mask.value(i)).collect()
    }

    #[test]
    fn single_conjunct_is_not_adaptive() {
        let schema = schema();
        let p =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        assert!(try_new_unshared(&p, true).is_none());
    }

    #[test]
    fn disabled_is_none() {
        let schema = schema();
        assert!(try_new_unshared(&predicate(&schema), false).is_none());
    }

    #[test]
    fn matches_plain_conjunction_evaluation() {
        let schema = schema();
        let p = predicate(&schema);
        let mut adaptive = try_new_unshared(&p, true).unwrap();

        let batch = test_batch(
            &schema,
            vec![1, 3, 5, 2, 4], // a
            vec![9, 4, 6, 1, 0], // b
        );
        // a > 2 AND b < 5: rows where a in {3,5,2,4} AND b in {<5}
        //   idx0 a=1 -> false
        //   idx1 a=3,b=4 -> true
        //   idx2 a=5,b=6 -> false (b)
        //   idx3 a=2 -> false (a)
        //   idx4 a=4,b=0 -> true
        let mask = adaptive.evaluate(&batch).unwrap();
        assert_eq!(passing_rows(&mask), vec![1, 4]);

        // Result is independent of the internal order: force a reordering and
        // re-check on the same data.
        adaptive.order = vec![1, 0];
        let mask = adaptive.evaluate(&batch).unwrap();
        assert_eq!(passing_rows(&mask), vec![1, 4]);
    }

    fn frozen_interval(adaptive: &AdaptiveConjunction) -> Option<u64> {
        match &adaptive.phase {
            Phase::Frozen { interval, .. } => Some(*interval),
            _ => None,
        }
    }

    /// Upper bound on the batches a measuring phase can span: a warmup, then
    /// one measured batch per stride until the sample cap.
    const MEASURE_SPAN: u64 =
        MEASURE_WARMUP + (MAX_LEARNING_SAMPLES - MEASURE_WARMUP + 1) * MEASURE_STRIDE;

    /// Run batches until frozen (panics if it never freezes within a bound).
    fn run_until_frozen(adaptive: &mut AdaptiveConjunction, batch: &RecordBatch) {
        for _ in 0..MEASURE_SPAN + 5 {
            adaptive.evaluate(batch).unwrap();
            if matches!(adaptive.phase, Phase::Frozen { .. }) {
                return;
            }
        }
        panic!("did not freeze");
    }

    /// From a frozen state, run batches through exactly one thaw → re-measure
    /// → freeze cycle and return the newly frozen interval.
    fn run_one_rethaw_cycle(
        adaptive: &mut AdaptiveConjunction,
        batch: &RecordBatch,
    ) -> u64 {
        let interval = frozen_interval(adaptive).expect("must start frozen");
        let mut thawed = false;
        for _ in 0..interval + MEASURE_SPAN + MEASURE_STRIDE + 2 {
            adaptive.evaluate(batch).unwrap();
            match (&adaptive.phase, thawed) {
                (Phase::Frozen { .. }, true) => {
                    return frozen_interval(adaptive).unwrap();
                }
                (Phase::Frozen { .. }, false) => {}
                _ => thawed = true,
            }
        }
        panic!("did not complete a re-thaw cycle");
    }

    /// b<5 discards almost everything, a>2 discards nothing, so the
    /// effectiveness CIs separate fast and we freeze on certainty.
    fn selective_b_batch(schema: &Arc<Schema>) -> RecordBatch {
        let a: Vec<i32> = (0..1000).map(|_| 100).collect();
        let b: Vec<i32> = (0..1000).map(|i| if i == 0 { 1 } else { 100 }).collect();
        test_batch(schema, a, b)
    }

    #[test]
    fn freezes_on_certainty_and_stops_measuring() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        let batch = selective_b_batch(&schema);

        // The learner proposes promoting b; whether the trial adopts it is a
        // timing question (not asserted here) — either verdict must freeze.
        run_until_frozen(&mut adaptive, &batch);

        // Once frozen, further batches (within the thaw interval) do not record.
        let before: u64 = adaptive.stats.iter().map(|s| s.sample_count()).sum();
        for _ in 0..10 {
            let mask = adaptive.evaluate(&batch).unwrap();
            assert_eq!(passing_rows(&mask), vec![0]); // ...and stay correct
        }
        let after: u64 = adaptive.stats.iter().map(|s| s.sample_count()).sum();
        assert_eq!(before, after, "frozen evaluator must not keep measuring");
    }

    #[test]
    fn rethaw_backs_off_when_order_is_stable() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        let batch = selective_b_batch(&schema);

        run_until_frozen(&mut adaptive, &batch);
        let interval1 = frozen_interval(&adaptive).unwrap();
        assert_eq!(interval1, INITIAL_THAW_INTERVAL);

        // Cross the thaw point and the re-measurement window; same data, so the
        // order is reconfirmed and the next interval backs off.
        let interval2 = run_one_rethaw_cycle(&mut adaptive, &batch);
        assert_eq!(interval2, interval1 * THAW_BACKOFF);
    }

    #[test]
    fn rethaw_proposes_trial_on_drift() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();

        // Settle on the initial distribution (b<5 selective). Whether a trial
        // adopts the promotion is timing-dependent; force the incumbent to the
        // b-first order so the drifted proposal below must differ from it.
        run_until_frozen(&mut adaptive, &selective_b_batch(&schema));
        adaptive.set_order(vec![1, 0]);

        // Drift: now a>2 is the selective one (only row 0), b<5 is always true.
        let a: Vec<i32> = (0..1000).map(|i| if i == 0 { 100 } else { 0 }).collect();
        let b: Vec<i32> = (0..1000).map(|_| 0).collect();
        let drift = test_batch(&schema, a, b);

        // Crossing the thaw point and the re-measure window must propose an
        // A/B trial for the now-better order, never adopt it outright.
        let mut proposed = None;
        for _ in 0..INITIAL_THAW_INTERVAL + MEASURE_SPAN + MEASURE_STRIDE + 2 {
            let mask = adaptive.evaluate(&drift).unwrap();
            assert_eq!(passing_rows(&mask), vec![0]); // a>2 AND b<5 -> row 0 only
            if let Phase::Trial { candidate, .. } = &adaptive.phase {
                proposed = Some(candidate.order.clone());
                break;
            }
        }
        assert_eq!(
            proposed,
            Some(vec![0, 1]),
            "drift must send the re-ranked order to a trial"
        );
        assert_eq!(
            adaptive.order,
            vec![1, 0],
            "incumbent unchanged until verdict"
        );
    }

    #[test]
    fn non_selective_conjuncts_never_compact_but_are_correct() {
        // Both conjuncts keep well over the compaction threshold, so the
        // working batch is never compacted and the result is produced purely
        // by AND-combining masks. Result must still be exact.
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        // a > 2 keeps 8/10; b < 5 keeps 7/10 — neither is <= 20%.
        let a = vec![5, 6, 7, 8, 9, 10, 11, 12, 1, 2]; // last two fail a>2
        let b = vec![0, 1, 2, 3, 4, 9, 9, 9, 0, 0]; // idx5..7 fail b<5
        let batch = test_batch(&schema, a, b);
        // a>2 AND b<5: idx0..4 pass both; idx5..7 fail b; idx8..9 fail a.
        let mask = adaptive.evaluate(&batch).unwrap();
        assert_eq!(passing_rows(&mask), vec![0, 1, 2, 3, 4]);
    }

    /// `a > 2 AND b < 5 AND b >= -5` — a selective leader plus two followers.
    fn three_conjunct_predicate(schema: &Arc<Schema>) -> Arc<dyn PhysicalExpr> {
        let leader =
            binary(col("a", schema).unwrap(), Operator::Gt, lit(2i32), schema).unwrap();
        let f1 =
            binary(col("b", schema).unwrap(), Operator::Lt, lit(5i32), schema).unwrap();
        let f2 = binary(
            col("b", schema).unwrap(),
            Operator::GtEq,
            lit(-5i32),
            schema,
        )
        .unwrap();
        let and = binary(leader, Operator::And, f1, schema).unwrap();
        binary(and, Operator::And, f2, schema).unwrap()
    }

    /// Followers that are statistically indistinguishable (each discards one
    /// of the leader's survivors, so their effectiveness samples are small,
    /// positive, and overlapping) must freeze as a tie within a few batches,
    /// not run to the learning sample cap.
    #[test]
    fn freezes_fast_on_tied_followers() {
        let schema = schema();
        let mut adaptive =
            try_new_unshared(&three_conjunct_predicate(&schema), true).unwrap();

        // Leader keeps 10 of 1000 rows; each follower discards exactly one of
        // those survivors (b = 10 fails b < 5, b = -10 fails b >= -5).
        let a: Vec<i32> = (0..1000)
            .map(|i| if i % 100 == 0 { 100 } else { 0 })
            .collect();
        let b: Vec<i32> = (0..1000)
            .map(|i| match i {
                100 => 10,
                200 => -10,
                _ => 0,
            })
            .collect();
        let batch = test_batch(&schema, a, b);

        let mut frozen_at = None;
        for n in 1..=MAX_LEARNING_SAMPLES {
            adaptive.evaluate(&batch).unwrap();
            if matches!(adaptive.phase, Phase::Frozen { .. }) {
                frozen_at = Some(n);
                break;
            }
        }
        let frozen_at = frozen_at.expect("must freeze");
        assert!(
            frozen_at <= MIN_SAMPLES_FOR_CI + 2,
            "tied followers should freeze right after the CI gate, froze at {frozen_at}"
        );
        // The selective leader still leads.
        assert_eq!(adaptive.order.first().copied(), Some(0));
    }

    /// A leader that discards everything starves the followers of rows; they
    /// can never be measured, but their order can never matter either, so the
    /// evaluator must still freeze (previously it measured forever).
    #[test]
    fn starved_conjuncts_do_not_block_freezing() {
        let schema = schema();
        let mut adaptive =
            try_new_unshared(&three_conjunct_predicate(&schema), true).unwrap();

        // a > 2 fails every row.
        let batch = test_batch(&schema, vec![0; 1000], vec![0; 1000]);
        for _ in 0..MIN_SAMPLES_FOR_CI + 2 {
            let mask = adaptive.evaluate(&batch).unwrap();
            assert!(passing_rows(&mask).is_empty());
        }
        assert!(
            matches!(adaptive.phase, Phase::Frozen { .. }),
            "starved followers must not hold up freezing"
        );
    }

    /// Tied followers re-rank on noise at every re-thaw; that must not be
    /// treated as drift, or the interval never backs off and the evaluator
    /// re-measures forever.
    #[test]
    fn rethaw_backs_off_despite_tied_followers() {
        let schema = schema();
        let mut adaptive =
            try_new_unshared(&three_conjunct_predicate(&schema), true).unwrap();
        let a: Vec<i32> = (0..1000)
            .map(|i| if i % 100 == 0 { 100 } else { 0 })
            .collect();
        let b: Vec<i32> = (0..1000)
            .map(|i| match i {
                100 => 10,
                200 => -10,
                _ => 0,
            })
            .collect();
        let batch = test_batch(&schema, a, b);

        run_until_frozen(&mut adaptive, &batch);
        let interval1 = frozen_interval(&adaptive).unwrap();
        // Cross the thaw point and the re-measurement window; the tied
        // followers may re-rank, but the interval must still back off.
        let interval2 = run_one_rethaw_cycle(&mut adaptive, &batch);
        assert_eq!(interval2, interval1 * THAW_BACKOFF);
    }

    /// A stream sharing state with one whose trial already validated a
    /// champion adopts it on its very first batch instead of re-running the
    /// experiment.
    #[test]
    fn streams_adopt_shared_champion_without_retrial() {
        let schema = schema();
        let shared = Arc::new(AdaptiveFilterShared::new());
        let p = predicate(&schema);
        let batch = selective_b_batch(&schema);

        // Another stream's completed, adopted trial.
        run_fake_trial(&shared, vec![1, 0], true);
        assert_eq!(shared.arbiter.epoch(), 1);

        let mut stream = AdaptiveConjunction::try_new(&p, true, shared).unwrap();
        let mask = stream.evaluate(&batch).unwrap();
        assert_eq!(passing_rows(&mask), vec![0]);
        assert!(
            matches!(stream.phase, Phase::Frozen { .. }),
            "the published champion must be adopted, not re-learned"
        );
        assert_eq!(stream.order, vec![1, 0]);
        assert_eq!(stream.strategy, Strategy::CompactOnce);
        assert_eq!(stream.epoch_seen, 1);
    }

    /// Prime the shared trial with decisive fake paired log-ratio samples,
    /// so the verdict after the remaining real pair is deterministic, and put
    /// the stream into the trial.
    fn prime_trial(
        adaptive: &mut AdaptiveConjunction,
        candidate: Vec<usize>,
        candidate_wins: bool,
    ) {
        let candidate = arrangement(candidate);
        assert_eq!(
            adaptive.shared.arbiter.begin_trial(candidate.clone()),
            candidate
        );
        for i in 0..TRIAL_PAIRS - 1 {
            // Decisive log-ratios with a little variance: candidate ~e^10
            // times faster (or slower) than the incumbent in every pair.
            let jitter = i as f64 * 0.01;
            adaptive.shared.arbiter.submit_pair(
                &candidate,
                1000,
                1000,
                if candidate_wins { -10.0 } else { 10.0 } + jitter,
            );
        }
        adaptive.phase = Phase::Trial {
            candidate,
            pending_incumbent: None,
            interval_if_rejected: 512,
        };
    }

    /// A trial whose samples show the candidate decisively faster must adopt
    /// it: switch the order and strategy, publish the champion, bump the
    /// epoch.
    #[test]
    fn trial_adopts_decisively_faster_candidate() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        let batch = selective_b_batch(&schema);

        prime_trial(&mut adaptive, vec![1, 0], true);
        // One batch per arm completes both sample counts and delivers the
        // verdict; results stay correct throughout.
        for _ in 0..2 {
            let mask = adaptive.evaluate(&batch).unwrap();
            assert_eq!(passing_rows(&mask), vec![0]);
        }
        assert!(matches!(adaptive.phase, Phase::Frozen { .. }));
        assert_eq!(adaptive.order, vec![1, 0]);
        assert_eq!(adaptive.strategy, Strategy::CompactOnce);
        let (champion, epoch) = adaptive.shared.arbiter.champion();
        assert_eq!(
            champion,
            Some(arrangement(vec![1, 0])),
            "the win must be published for the other streams"
        );
        assert_eq!(epoch, 1);
    }

    /// A trial whose samples show the candidate decisively slower must keep
    /// the incumbent untouched, freeze with the carried (backed-off) interval,
    /// and record the rejection so no stream re-trials the same order.
    #[test]
    fn trial_rejects_decisively_slower_candidate() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        let batch = selective_b_batch(&schema);

        prime_trial(&mut adaptive, vec![1, 0], false);
        for _ in 0..2 {
            let mask = adaptive.evaluate(&batch).unwrap();
            assert_eq!(passing_rows(&mask), vec![0]);
        }
        assert_eq!(adaptive.order, vec![0, 1], "incumbent must be kept");
        assert_eq!(adaptive.strategy, Strategy::Fused);
        assert_eq!(frozen_interval(&adaptive), Some(512));
        let (champion, epoch) = adaptive.shared.arbiter.champion();
        assert_eq!(champion, None);
        assert_eq!(epoch, 0);
        assert_eq!(
            adaptive.shared.arbiter.rejected(),
            Some(arrangement(vec![1, 0]))
        );
    }

    /// Streams contribute samples to the same shared trial, so it concludes
    /// across them: a stream shorter than a whole trial still gets a verdict.
    #[test]
    fn trial_is_completed_across_streams() {
        let schema = schema();
        let shared = Arc::new(AdaptiveFilterShared::new());
        let p = predicate(&schema);
        let batch = selective_b_batch(&schema);

        let mut stream_a =
            AdaptiveConjunction::try_new(&p, true, Arc::clone(&shared)).unwrap();
        let mut stream_b =
            AdaptiveConjunction::try_new(&p, true, Arc::clone(&shared)).unwrap();
        prime_trial(&mut stream_a, vec![1, 0], true);
        stream_b.phase = Phase::Trial {
            candidate: arrangement(vec![1, 0]),
            pending_incumbent: None,
            interval_if_rejected: 512,
        };

        // One pair is missing; stream B (which did not start the trial)
        // contributes it over two batches and concludes the experiment.
        stream_b.evaluate(&batch).unwrap();
        stream_b.evaluate(&batch).unwrap();
        assert!(!shared.arbiter.trial_in_progress(), "trial must conclude");
        assert_eq!(shared.arbiter.champion().0, Some(arrangement(vec![1, 0])));
        assert_eq!(stream_b.order, vec![1, 0]);
        assert!(matches!(stream_b.phase, Phase::Frozen { .. }));
        // Stream A adopts the published champion via the epoch check.
        stream_a.evaluate(&batch).unwrap();
        assert_eq!(stream_a.order, vec![1, 0]);
        assert!(matches!(stream_a.phase, Phase::Frozen { .. }));
    }

    /// A proposal that matches an already-rejected order freezes the incumbent
    /// instead of starting another trial: the experiment was run and lost.
    #[test]
    fn decide_skips_rejected_candidate() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();

        // Fabricated stats under which [1, 0] is materially better: conjunct 1
        // discards almost everything at equal per-row cost.
        let mut stats = vec![SelectivityStats::default(); 2];
        stats[0].record(1000, 1000, 1000, 0.0);
        stats[1].record(10, 1000, 1000, 990.0 * 1e9 / 1000.0);

        // Record a lost trial for [1, 0] through the arbiter.
        run_fake_trial(&adaptive.shared, vec![1, 0], false);
        adaptive.decide(vec![1, 0], &stats, 256);
        assert_eq!(frozen_interval(&adaptive), Some(256));
        assert_eq!(adaptive.order, vec![0, 1]);

        // Without the recorded rejection the same proposal goes to trial.
        let mut fresh = try_new_unshared(&predicate(&schema), true).unwrap();
        fresh.decide(vec![1, 0], &stats, 256);
        assert!(matches!(fresh.phase, Phase::Trial { .. }));
    }

    /// While the order stays unresolved, only the warmup is measured
    /// back-to-back; after that measurement drops to one batch per stride, so
    /// unresolved learning costs a bounded fraction of the stream.
    #[test]
    fn learning_measures_warmup_then_strided() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        // Empty batches never record a sample, so the evaluator stays in
        // `Learning` and the measurement schedule is observable in isolation.
        let batch = test_batch(&schema, vec![], vec![]);
        let n = 10 * MEASURE_STRIDE;
        for _ in 0..n {
            adaptive.evaluate(&batch).unwrap();
        }
        assert!(matches!(adaptive.phase, Phase::Learning));
        assert!(
            adaptive.measured >= MEASURE_WARMUP,
            "warmup batches must all be measured"
        );
        assert!(
            adaptive.measured <= MEASURE_WARMUP + n / MEASURE_STRIDE,
            "post-warmup measurement must be strided, measured {} of {n}",
            adaptive.measured
        );
    }

    #[test]
    fn all_true_conjuncts_yield_all_true_mask() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        // Every row passes both conjuncts.
        let batch = test_batch(&schema, vec![10; 5], vec![0; 5]);
        let mask = adaptive.evaluate(&batch).unwrap();
        assert_eq!(passing_rows(&mask), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn fully_discarding_conjunct_yields_all_false_mask() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        // No row passes a > 2; the remaining conjunct must not resurrect rows.
        let batch = test_batch(&schema, vec![0; 5], vec![0; 5]);
        let mask = adaptive.evaluate(&batch).unwrap();
        assert!(passing_rows(&mask).is_empty());
        assert_eq!(as_boolean_array(&mask).unwrap().len(), 5);
    }

    #[test]
    fn empty_batch() {
        let schema = schema();
        let mut adaptive = try_new_unshared(&predicate(&schema), true).unwrap();
        let batch = test_batch(&schema, vec![], vec![]);
        let mask = adaptive.evaluate(&batch).unwrap();
        assert_eq!(as_boolean_array(&mask).unwrap().len(), 0);
    }

    /// Manual perf harness comparing the adaptive evaluator against the plain
    /// fused predicate (what `FilterExec` runs when the flag is off), on a
    /// ClickBench-Q41-shaped filter: several trivially cheap conjuncts, the
    /// most selective one already written first. This is the adversarial case
    /// for the adaptive path (nothing to learn, overhead only).
    ///
    /// Run with:
    /// ```sh
    /// cargo test --release -p datafusion-physical-plan --lib \
    ///   adaptive_filter::tests::perf_overhead_vs_fused -- --ignored --nocapture
    /// ```
    /// For a profile: find the test binary via `cargo test --no-run` and run it
    /// under `samply record` with `ADAPTIVE_PERF_ITERS=200000`.
    /// Manual experiment replicating predicate_eval `cardinality_q30`:
    /// `c0<90 AND c1<5` over uniform ints, replayed in fresh short streams the
    /// way a criterion iteration would, to expose per-query (re-learning and
    /// re-trial) costs.
    #[test]
    #[ignore = "manual perf experiment, run with --ignored --nocapture"]
    fn perf_q30_per_query_cost() {
        use datafusion_physical_expr::expressions::{binary, col, lit};

        const ROWS: usize = 8192;
        const BATCHES_PER_QUERY: u64 = 122;
        // K-1 ~90% conjuncts followed by one ~5% conjunct (the qXX sweep).
        let k: usize = std::env::var("ADAPTIVE_PERF_K")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);
        let multipliers = [
            1i64, 3, 7, 9, 11, 13, 17, 19, 21, 23, 27, 29, 31, 33, 37, 39,
        ];
        let schema = Arc::new(Schema::new(
            (0..k)
                .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        let cols: Vec<ArrayRef> = multipliers[..k]
            .iter()
            .map(|m| {
                Arc::new(Int32Array::from_iter_values(
                    (0..ROWS as i64).map(move |v| ((v * m) % 100) as i32),
                )) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = (0..k)
            .map(|i| {
                let threshold = if i == k - 1 { 5i32 } else { 90i32 };
                binary(c(&format!("c{i}")), Operator::Lt, lit(threshold), &schema)
                    .unwrap()
            })
            .collect();
        let written = fuse(&conjuncts, &(0..k).collect::<Vec<_>>());

        let queries: u64 = 200;
        // Flag-off equivalent: the fused written order.
        let start = Instant::now();
        for _ in 0..queries {
            for _ in 0..BATCHES_PER_QUERY {
                let v = written.evaluate(&batch).unwrap();
                std::hint::black_box(v.into_array(ROWS).unwrap());
            }
        }
        let off = start.elapsed().as_nanos() as u64 / (queries * BATCHES_PER_QUERY);

        // Flag-on: a fresh evaluator per query (criterion-iteration shape).
        let start = Instant::now();
        for _ in 0..queries {
            let mut adaptive = try_new_unshared(&written, true).unwrap();
            for _ in 0..BATCHES_PER_QUERY {
                std::hint::black_box(adaptive.evaluate(&batch).unwrap());
            }
        }
        let on = start.elapsed().as_nanos() as u64 / (queries * BATCHES_PER_QUERY);
        println!(
            "fused written: {off} ns/batch; adaptive fresh-per-query: {on} ns/batch ({:.2}x)",
            on as f64 / off as f64
        );

        // Phase census for one query.
        let mut adaptive = try_new_unshared(&written, true).unwrap();
        let mut census = std::collections::BTreeMap::new();
        for _ in 0..BATCHES_PER_QUERY {
            adaptive.evaluate(&batch).unwrap();
            let phase = match adaptive.phase {
                Phase::Learning => "learning",
                Phase::Trial { .. } => "trial",
                Phase::Frozen { .. } => "frozen",
                Phase::Remeasuring { .. } => "remeasuring",
            };
            *census.entry(phase).or_insert(0u64) += 1;
        }
        println!("phase census over one {BATCHES_PER_QUERY}-batch query: {census:?}");
        println!(
            "final order {:?} strategy {:?}",
            adaptive.order, adaptive.strategy
        );
    }

    /// Manual experiment replicating predicate_eval `cardinality_q31`:
    /// `c0<90 AND c1<90 AND c2<90 AND c3<5` over uniform ints. Compares the
    /// written order, the optimal order, and the adaptive evaluator under
    /// each evaluation strategy, to attribute regressions to overhead vs a
    /// bad order choice vs the fused chain's evaluation strategy.
    #[test]
    #[ignore = "manual perf experiment, run with --ignored --nocapture"]
    fn perf_q31_order_strategies() {
        use datafusion_physical_expr::expressions::{binary, col, lit};

        const ROWS: usize = 8192;
        let schema = Arc::new(Schema::new(
            (0..4)
                .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        // Mirror the bench's decorrelated uniform columns.
        let cols: Vec<ArrayRef> = [1i64, 3, 7, 9]
            .iter()
            .map(|m| {
                Arc::new(Int32Array::from_iter_values(
                    (0..ROWS as i64).map(move |v| ((v * m) % 100) as i32),
                )) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();

        let c = |name: &str| col(name, &schema).unwrap();
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = vec![
            binary(c("c0"), Operator::Lt, lit(90i32), &schema).unwrap(),
            binary(c("c1"), Operator::Lt, lit(90i32), &schema).unwrap(),
            binary(c("c2"), Operator::Lt, lit(90i32), &schema).unwrap(),
            binary(c("c3"), Operator::Lt, lit(5i32), &schema).unwrap(),
        ];
        let written = fuse(&conjuncts, &[0, 1, 2, 3]);
        let optimal = fuse(&conjuncts, &[3, 0, 1, 2]);

        let iters: u64 = 20_000;
        let time = |expr: &Arc<dyn PhysicalExpr>| {
            let start = Instant::now();
            for _ in 0..iters {
                let v = expr.evaluate(&batch).unwrap();
                std::hint::black_box(v.into_array(ROWS).unwrap());
            }
            start.elapsed().as_nanos() as u64 / iters
        };

        // Warm up.
        time(&written);
        let w = time(&written);
        let o = time(&optimal);
        println!("fused written  [c0,c1,c2,c3]: {w:>8} ns/batch");
        println!(
            "fused optimal  [c3,c0,c1,c2]: {o:>8} ns/batch ({:+.1}%)",
            (o as f64 / w as f64 - 1.0) * 100.0
        );

        // Adaptive evaluator: a long stream (mostly frozen in learned order)
        // and a short stream (mostly measured path).
        for n in [16u64, 122, 20_000] {
            let mut adaptive = try_new_unshared(&written, true).unwrap();
            let start = Instant::now();
            for _ in 0..n {
                std::hint::black_box(adaptive.evaluate(&batch).unwrap());
            }
            let ns = start.elapsed().as_nanos() as u64 / n;
            let phase = match adaptive.phase {
                Phase::Learning => "learning",
                Phase::Trial { .. } => "trial",
                Phase::Frozen { .. } => "frozen",
                Phase::Remeasuring { .. } => "remeasuring",
            };
            println!(
                "adaptive {n:>6}-batch stream:   {ns:>8} ns/batch ({:+.1}% vs written, order {:?}, ends {phase})",
                (ns as f64 / w as f64 - 1.0) * 100.0,
                adaptive.order,
            );
        }

        // The measured path itself, pinned to the optimal order (no learning
        // decisions): what a compact-once strategy costs at steady state.
        let mut pinned = try_new_unshared(&written, true).unwrap();
        pinned.set_order(vec![3, 0, 1, 2]);
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(pinned.evaluate_measured(&batch).unwrap());
        }
        let ns = start.elapsed().as_nanos() as u64 / iters;
        println!(
            "measured path, optimal order:  {ns:>8} ns/batch ({:+.1}% vs written)",
            (ns as f64 / w as f64 - 1.0) * 100.0
        );
    }

    #[test]
    #[ignore = "manual perf harness, run with --ignored --nocapture"]
    fn perf_overhead_vs_fused() {
        use datafusion_physical_expr::expressions::{binary, col, lit};

        const ROWS: usize = 8192;
        let schema = Arc::new(Schema::new(vec![
            Field::new("counter_id", DataType::Int32, false),
            Field::new("event_date", DataType::Int32, false),
            Field::new("is_refresh", DataType::Int32, false),
            Field::new("dont_count", DataType::Int32, false),
            Field::new("url_hash", DataType::Int32, false),
        ]));
        // ~0.1% of rows match counter_id = 62; the follower conjuncts each
        // discard exactly one of those survivors, mirroring Q41's shape: their
        // effectiveness is small, positive, and statistically
        // indistinguishable from each other (overlapping CIs), so certainty
        // freezing cannot resolve their order.
        let counter: Vec<i32> = (0..ROWS)
            .map(|i| if i % 1000 == 0 { 62 } else { 1 })
            .collect();
        let is_refresh: Vec<i32> =
            (0..ROWS).map(|i| if i == 1000 { 1 } else { 0 }).collect();
        let dont_count: Vec<i32> =
            (0..ROWS).map(|i| if i == 2000 { 1 } else { 0 }).collect();
        let url_hash: Vec<i32> =
            (0..ROWS).map(|i| if i == 3000 { 999 } else { 7 }).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(counter)),
                Arc::new(Int32Array::from(vec![100; ROWS])),
                Arc::new(Int32Array::from(is_refresh)),
                Arc::new(Int32Array::from(dont_count)),
                Arc::new(Int32Array::from(url_hash)),
            ],
        )
        .unwrap();

        let c = |name: &str| col(name, &schema).unwrap();
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = vec![
            binary(c("counter_id"), Operator::Eq, lit(62i32), &schema).unwrap(),
            binary(c("event_date"), Operator::GtEq, lit(50i32), &schema).unwrap(),
            binary(c("event_date"), Operator::LtEq, lit(150i32), &schema).unwrap(),
            binary(c("is_refresh"), Operator::Eq, lit(0i32), &schema).unwrap(),
            binary(c("dont_count"), Operator::Eq, lit(0i32), &schema).unwrap(),
            binary(c("url_hash"), Operator::Eq, lit(7i32), &schema).unwrap(),
        ];
        let fused = conjuncts
            .clone()
            .into_iter()
            .reduce(|acc, e| {
                Arc::new(BinaryExpr::new(acc, Operator::And, e)) as Arc<dyn PhysicalExpr>
            })
            .unwrap();

        let iters: u64 = std::env::var("ADAPTIVE_PERF_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);

        let time_fused = |n: u64| {
            let start = Instant::now();
            for _ in 0..n {
                let v = fused.evaluate(&batch).unwrap();
                std::hint::black_box(v.into_array(ROWS).unwrap());
            }
            start.elapsed().as_nanos() as u64 / n
        };
        let time_adaptive = |n: u64| {
            let mut adaptive = try_new_unshared(&fused, true).unwrap();
            let start = Instant::now();
            for _ in 0..n {
                std::hint::black_box(adaptive.evaluate(&batch).unwrap());
            }
            (start.elapsed().as_nanos() as u64 / n, adaptive)
        };

        // Profiling mode: spend all the time in the measured (learning) path by
        // replaying short streams on fresh evaluators, then exit.
        if std::env::var("ADAPTIVE_PERF_PROFILE").is_ok() {
            for _ in 0..iters {
                std::hint::black_box(time_adaptive(16));
            }
            return;
        }

        // Warm up.
        time_fused(100);
        time_adaptive(100);

        let fused_ns = time_fused(iters);
        println!("fused (flag off equivalent):      {fused_ns:>8} ns/batch");
        // Short streams never amortize learning; long streams should.
        for n in [16u64, 64, 256, iters] {
            let (ns, adaptive) = time_adaptive(n);
            let phase = match adaptive.phase {
                Phase::Learning => "learning",
                Phase::Trial { .. } => "trial",
                Phase::Frozen { .. } => "frozen",
                Phase::Remeasuring { .. } => "remeasuring",
            };
            println!(
                "adaptive, {n:>6}-batch stream:    {ns:>8} ns/batch ({:.2}x fused, ends {phase})",
                ns as f64 / fused_ns as f64,
            );
        }
    }

    #[test]
    fn reorders_selective_conjunct_first() {
        let schema = schema();
        let p = predicate(&schema); // [a>2, b<5]
        let mut adaptive = try_new_unshared(&p, true).unwrap();

        // Conjunct 1 (b < 5) is far more selective than conjunct 0 (a > 2):
        // a is always > 2 (never discards), b is almost always >= 5 (discards
        // ~everything). Discards-per-second is highest for conjunct 1, so the
        // learner must propose promoting it — into a trial, never directly.
        let batch = selective_b_batch(&schema);
        let mut proposed = None;
        for _ in 0..MEASURE_SPAN + 5 {
            let mask = adaptive.evaluate(&batch).unwrap();
            assert_eq!(passing_rows(&mask), vec![0]); // correct in every phase
            if let Phase::Trial { candidate, .. } = &adaptive.phase {
                proposed = Some(candidate.order.clone());
                break;
            }
        }
        assert_eq!(proposed, Some(vec![1, 0]));
        assert_eq!(
            adaptive.order,
            vec![0, 1],
            "incumbent unchanged until verdict"
        );
    }
}
