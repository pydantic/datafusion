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

//! Shared morsel-driven pipeline with concurrent I/O.
//!
//! Replaces per-partition morselization with a single shared pipeline:
//! ```text
//! files → buffer_unordered(M) morselize → flatten
//!       → buffer_unordered(N) open → flatten → MPMC channel ← partitions
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_channel::Receiver;
use datafusion_common::Result;
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::RecordBatchStream;
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use futures::stream::{self, StreamExt};
use futures::Stream;

use crate::file_stream::FileOpener;
use crate::PartitionedFile;

/// A shared pipeline that produces `RecordBatch`es via an MPMC channel.
/// All partitions pull from the same channel for natural load balancing.
pub struct SharedPipeline {
    /// MPMC receiver — cloned for each partition.
    rx: Receiver<Result<RecordBatch>>,
    /// Output schema.
    schema: SchemaRef,
    /// Pipeline task handle (aborted on drop via SpawnedTask).
    _task: SpawnedTask<()>,
}

impl SharedPipeline {
    /// Spawn the shared pipeline.
    pub fn spawn(
        opener: Arc<dyn FileOpener>,
        files: Vec<PartitionedFile>,
        schema: SchemaRef,
        limit: Option<usize>,
        morselize_concurrency: usize,
        open_concurrency: usize,
    ) -> Self {
        // Channel bound of 1: near-zero buffering, backpressure propagates directly.
        let (tx, rx) = async_channel::bounded(1);

        let task = SpawnedTask::spawn(async move {
            run_pipeline(
                opener,
                files,
                limit,
                morselize_concurrency,
                open_concurrency,
                tx,
            )
            .await;
        });

        Self {
            rx,
            schema,
            _task: task,
        }
    }

    /// Create a per-partition stream that pulls from the shared channel.
    pub fn partition_stream(
        &self,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> SharedPipelineStream {
        SharedPipelineStream {
            rx: Box::pin(self.rx.clone()),
            schema: Arc::clone(&self.schema),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        }
    }
}

/// Per-partition stream wrapping the MPMC receiver.
pub struct SharedPipelineStream {
    rx: Pin<Box<Receiver<Result<RecordBatch>>>>,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl Stream for SharedPipelineStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.rx.as_mut().poll_next(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for SharedPipelineStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// The core pipeline logic, runs as a spawned task.
///
/// Uses sequential processing with a single pipeline task.
/// Concurrency comes from the MPMC channel allowing multiple
/// partitions to consume batches in parallel.
async fn run_pipeline(
    opener: Arc<dyn FileOpener>,
    files: Vec<PartitionedFile>,
    limit: Option<usize>,
    morselize_concurrency: usize,
    open_concurrency: usize,
    tx: async_channel::Sender<Result<RecordBatch>>,
) {
    // Use buffer_unordered for I/O concurrency within the pipeline task.
    // This is poll-based (no task spawning) so it works on any runtime.
    log::debug!(
        "SharedPipeline: starting with {} files, M={}, N={}",
        files.len(),
        morselize_concurrency,
        open_concurrency,
    );

    let file_stream = stream::iter(files);

    let opener_m = Arc::clone(&opener);
    let morsel_stream = file_stream
        .map(move |file| {
            let opener = Arc::clone(&opener_m);
            async move {
                log::debug!("SharedPipeline: morselizing file");
                let result = opener.morselize(file).await;
                log::debug!(
                    "SharedPipeline: morselize done, {} morsels",
                    result.as_ref().map(|m| m.len()).unwrap_or(0)
                );
                result
            }
        })
        .buffer_unordered(morselize_concurrency)
        .flat_map(|result| match result {
            Ok(morsels) => stream::iter(morsels.into_iter().map(Ok)).left_stream(),
            Err(e) => stream::once(futures::future::ready(Err(e))).right_stream(),
        });

    // Open morsels with buffer_unordered for prefetching.
    let mut reader_stream = Box::pin(
        morsel_stream
            .map(move |morsel_result| {
                let opener = Arc::clone(&opener);
                async move {
                    let morsel = morsel_result?;
                    log::debug!("SharedPipeline: opening morsel");
                    let result = opener.open(morsel)?.await;
                    log::debug!("SharedPipeline: open done");
                    result
                }
            })
            .buffer_unordered(open_concurrency),
    );

    let mut remaining = limit;
    log::debug!("SharedPipeline: entering main loop");
    while let Some(reader_result) = reader_stream.next().await {
        match reader_result {
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            Ok(mut batch_stream) => {
                while let Some(batch_result) = batch_stream.next().await {
                    match batch_result {
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                        Ok(batch) => {
                            if let Some(ref mut rem) = remaining {
                                let rows = batch.num_rows();
                                if rows >= *rem {
                                    let last = batch.slice(0, *rem);
                                    let _ = tx.send(Ok(last)).await;
                                    return;
                                }
                                *rem -= rows;
                            }
                            if tx.send(Ok(batch)).await.is_err() {
                                return; // all receivers dropped
                            }
                        }
                    }
                }
            }
        }
    }
    // tx dropped here → all receivers get None
}
