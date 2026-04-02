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

use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::PartitionedFile;
use crate::morsel::{Morsel, MorselPlanner, Morselizer, PendingMorselPlanner};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_plan::metrics::ScopedTimerGuard;
use futures::stream::BoxStream;
use futures::{FutureExt as _, StreamExt as _};

use super::{FileStreamMetrics, OnError};

/// State [`FileStreamState::Scan`]
///
/// Groups together ready planners, ready morsels, the active reader,
/// pending planner I/O, the remaining files and limit, and the metrics
/// associated with processing that work.
///
/// [`FileStreamState::Scan`]: super::FileStreamState::Scan
pub(super) struct ScanState {
    /// Files that still need to be planned.
    file_iter: VecDeque<PartitionedFile>,
    /// Remaining record limit, if any.
    remain: Option<usize>,
    /// The morselizer used to plan files
    morselizer: Box<dyn Morselizer>,
    /// Behavior if opening or scanning a file fails.
    on_error: OnError,
    /// CPU-ready planners for the current file.
    ready_planners: VecDeque<Box<dyn MorselPlanner>>,
    /// Ready morsels for the current file.
    ready_morsels: VecDeque<Box<dyn Morsel>>,
    /// The active reader, if any.
    reader: Option<BoxStream<'static, Result<RecordBatch>>>,
    /// Planner currently doing I/O
    pending_planner: Option<PendingMorselPlanner>,
    /// Metrics for the active scan queues.
    metrics: FileStreamMetrics,
}

impl ScanState {
    pub(super) fn new(
        file_iter: impl Into<VecDeque<PartitionedFile>>,
        remain: Option<usize>,
        morselizer: Box<dyn Morselizer>,
        on_error: OnError,
        metrics: FileStreamMetrics,
    ) -> Self {
        let file_iter = file_iter.into();
        Self {
            file_iter,
            remain,
            morselizer,
            on_error,
            ready_planners: Default::default(),
            ready_morsels: Default::default(),
            reader: None,
            pending_planner: None,
            metrics,
        }
    }

    /// Updates how scan errors are handled while the stream is still active.
    pub(super) fn set_on_error(&mut self, on_error: OnError) {
        self.on_error = on_error;
    }

    /// Drives one iteration of the active scan state, reading from morsels,
    /// planners, pending planner I/O, or unopened files from `self`.
    ///
    /// The return [`ScanAndReturn`] tells `poll_inner` how to update the
    /// outer `FileStreamState`.
    pub(super) fn poll_scan(&mut self, cx: &mut Context<'_>) -> ScanAndReturn {
        let _processing_timer: ScopedTimerGuard<'_> =
            self.metrics.time_processing.timer();

        // Try and resolve outstanding IO first
        if let Some(mut pending_planner) = self.pending_planner.take() {
            match pending_planner.poll_unpin(cx) {
                // IO is still pending
                Poll::Pending => {
                    self.pending_planner = Some(pending_planner);
                    return ScanAndReturn::Return(Poll::Pending);
                }
                // IO resolved, and the planner is ready for CPU work
                Poll::Ready(Ok(planner)) => {
                    self.ready_planners.push_back(planner);
                    return ScanAndReturn::Continue;
                }
                // IO Error
                Poll::Ready(Err(err)) => {
                    self.metrics.file_open_errors.add(1);
                    self.metrics.time_opening.stop();
                    return match self.on_error {
                        OnError::Skip => {
                            self.metrics.files_processed.add(1);
                            ScanAndReturn::Continue
                        }
                        OnError::Fail => ScanAndReturn::Error(err),
                    };
                }
            }
        }

        // Next try and get the net batch from the active reader, if any
        if let Some(reader) = self.reader.as_mut() {
            match reader.poll_next_unpin(cx) {
                // According to the API contract, readers should always be ready
                // but in practice they may actually be waiting on IO, and if
                // that happens wait for it here.
                Poll::Pending => return ScanAndReturn::Return(Poll::Pending),
                Poll::Ready(Some(Ok(batch))) => {
                    self.metrics.time_scanning_until_data.stop();
                    self.metrics.time_scanning_total.stop();
                    // check limit
                    let (batch, finished) = match &mut self.remain {
                        Some(remain) => {
                            if *remain > batch.num_rows() {
                                *remain -= batch.num_rows();
                                self.metrics.time_scanning_total.start();
                                (batch, false)
                            } else {
                                let batch = batch.slice(0, *remain);
                                let done = 1 + self.file_iter.len();
                                self.metrics.files_processed.add(done);
                                *remain = 0;
                                (batch, true)
                            }
                        }
                        None => {
                            self.metrics.time_scanning_total.start();
                            (batch, false)
                        }
                    };
                    return if finished {
                        ScanAndReturn::Done(Some(Ok(batch)))
                    } else {
                        ScanAndReturn::Return(Poll::Ready(Some(Ok(batch))))
                    };
                }
                Poll::Ready(Some(Err(err))) => {
                    self.reader = None;
                    self.metrics.file_scan_errors.add(1);
                    self.metrics.time_scanning_until_data.stop();
                    self.metrics.time_scanning_total.stop();
                    return match self.on_error {
                        OnError::Skip => {
                            self.metrics.files_processed.add(1);
                            ScanAndReturn::Continue
                        }
                        OnError::Fail => ScanAndReturn::Error(err),
                    };
                }
                Poll::Ready(None) => {
                    self.reader = None;
                    self.metrics.files_processed.add(1);
                    self.metrics.time_scanning_until_data.stop();
                    self.metrics.time_scanning_total.stop();
                    return ScanAndReturn::Continue;
                }
            }
        }

        // Don't have a reader but have morsels ready to turn into a reader, so do that.
        if let Some(morsel) = self.ready_morsels.pop_front() {
            self.metrics.files_opened.add(1);
            self.metrics.time_opening.stop();
            self.metrics.time_scanning_until_data.start();
            self.metrics.time_scanning_total.start();
            self.reader = Some(morsel.into_stream());
            return ScanAndReturn::Continue;
        }

        // Don't have a morsel or stream, so try and plan some more morsels
        if let Some(planner) = self.ready_planners.pop_front() {
            return match planner.plan() {
                Ok(Some(mut plan)) => {
                    // Get all morsels and planners and try again
                    self.ready_morsels.extend(plan.take_morsels());
                    self.ready_planners.extend(plan.take_ready_planners());
                    if let Some(pending_planner) = plan.take_pending_planner() {
                        self.pending_planner = Some(pending_planner);
                    }
                    ScanAndReturn::Continue
                }
                Ok(None) => {
                    self.metrics.files_processed.add(1);
                    self.metrics.time_opening.stop();
                    ScanAndReturn::Continue
                }
                Err(err) => {
                    self.metrics.file_open_errors.add(1);
                    self.metrics.time_opening.stop();
                    match self.on_error {
                        OnError::Skip => {
                            self.metrics.files_processed.add(1);
                            ScanAndReturn::Continue
                        }
                        OnError::Fail => ScanAndReturn::Error(err),
                    }
                }
            };
        }

        // No planners, morsels, or active reader, so try and open the next file and plan it.
        let part_file = match self.file_iter.pop_front() {
            Some(part_file) => part_file,
            None => return ScanAndReturn::Done(None),
        };

        self.metrics.time_opening.start();
        match self.morselizer.plan_file(part_file) {
            Ok(planner) => {
                self.ready_planners.push_back(planner);
                ScanAndReturn::Continue
            }
            Err(err) => match self.on_error {
                OnError::Skip => {
                    self.metrics.file_open_errors.add(1);
                    self.metrics.time_opening.stop();
                    self.metrics.files_processed.add(1);
                    ScanAndReturn::Continue
                }
                OnError::Fail => ScanAndReturn::Error(err),
            },
        }
    }
}

/// What should be done on the next iteration of [`ScanState::poll_scan`]?
pub(super) enum ScanAndReturn {
    /// Poll again
    Continue,
    /// Return the provided result without changing the outer state.
    Return(Poll<Option<Result<RecordBatch>>>),
    /// Update the outer `FileStreamState` to `Done` and return the provided result.
    Done(Option<Result<RecordBatch>>),
    /// Update the outer `FileStreamState` to `Error` and return the provided error.
    Error(DataFusionError),
}
