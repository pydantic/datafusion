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

//! [`MorselPool`] and [`MorselStream`] for morsel-driven file scanning.
//!
//! This module implements the core morsel execution engine that replaces
//! static file-to-partition assignment with dynamic work stealing.
//!
//! # Architecture
//!
//! ```text
//! Stage 1: File Opening     Stage 2: Morsel Distribution    Stage 3: Execution
//! (IO-heavy)                (shared pool)                    (per-partition)
//!
//! [File1] ──open──► [M1a, M1b] ──► MorselPool ──► Partition 0: MorselStream
//! [File2] ──open──► [M2a]      ──►            ──► Partition 1: MorselStream
//! [File3] ──open──► [M3a, M3b] ──►            ──► Partition 2: MorselStream
//! ```
//!
//! In **unordered mode** (default for analytics), all partitions share a
//! single morsel queue — fast partitions naturally "steal" work from slow
//! ones by grabbing morsels first.
//!
//! In **ordered mode** (`preserve_order == true`), each partition gets its
//! own dedicated morsel queue to maintain file ordering guarantees.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::file_scan_config::FileScanConfig;
use crate::file_stream::{FileMorsel, FileOpener};
use crate::PartitionedFile;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::error::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder,
};

use futures::{Stream, StreamExt as _, ready};
use log::debug;
use tokio::sync::mpsc;

/// Configuration for morsel-driven scanning.
///
/// Controls the 3-stage pipeline:
/// - **Stage 1 (File Opening)**: Opens files and splits them into morsels.
///   Controlled by `max_concurrent_opens` and `open_prefetch_depth`.
/// - **Stage 2 (Morsel Execution)**: Executes morsels to produce RecordBatches.
///   Controlled by `max_concurrent_executions`.
/// - **Stage 3 (Output Buffering)**: Buffers ready RecordBatches for consumers.
///   Controlled by `morsel_buffer_size`.
#[derive(Debug, Clone)]
pub struct MorselConfig {
    /// Maximum number of files to open concurrently (Stage 1).
    pub max_concurrent_opens: usize,
    /// How many opened-but-not-yet-executing files to buffer between Stage 1 and Stage 2.
    pub open_prefetch_depth: usize,
    /// Maximum number of morsels to execute concurrently per partition (Stage 2).
    pub max_concurrent_executions: usize,
    /// How many ready RecordBatches to buffer per partition (Stage 3).
    pub morsel_buffer_size: usize,
}

impl Default for MorselConfig {
    fn default() -> Self {
        Self {
            max_concurrent_opens: 4,
            open_prefetch_depth: 4,
            max_concurrent_executions: 1,
            morsel_buffer_size: 2,
        }
    }
}

impl MorselConfig {
    /// Create a `MorselConfig` from DataFusion's `ExecutionOptions`.
    pub fn from_execution_options(
        options: &datafusion_common::config::ExecutionOptions,
    ) -> Self {
        Self {
            max_concurrent_opens: options.morsel_max_concurrent_opens,
            open_prefetch_depth: options.morsel_open_prefetch_depth,
            max_concurrent_executions: options.morsel_max_concurrent_executions,
            morsel_buffer_size: options.morsel_buffer_size,
        }
    }
}

/// Shared morsel pool that distributes work across partition streams.
///
/// The pool manages file opening and morsel distribution. In unordered mode,
/// all partitions share a single channel. In ordered mode, each partition
/// gets its own channel to preserve file ordering.
pub struct MorselPool {
    /// Receivers for each partition. In unordered mode, all partitions share
    /// the same underlying sender (but each has its own receiver from a
    /// broadcast-like fanout). In ordered mode, each partition has a dedicated
    /// channel.
    receivers: Vec<tokio::sync::Mutex<mpsc::Receiver<Box<dyn FileMorsel>>>>,

    /// Schema of the output stream.
    projected_schema: SchemaRef,
}

impl MorselPool {
    /// Create a new morsel pool and start the background file opening task.
    ///
    /// # Arguments
    /// * `config` - The file scan configuration
    /// * `file_opener` - The opener used to create morsels from files
    /// * `metrics` - Metrics for tracking pool operations
    /// * `morsel_config` - Configuration for concurrency and buffering
    pub fn new(
        config: &FileScanConfig,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
        morsel_config: MorselConfig,
    ) -> Result<Self> {
        let projected_schema = config.projected_schema()?;
        let num_partitions = config.file_groups.len();
        let ordered = config.preserve_order;

        if ordered {
            Self::new_ordered(
                config,
                file_opener,
                metrics,
                morsel_config,
                projected_schema,
            )
        } else {
            Self::new_unordered(
                config,
                file_opener,
                metrics,
                morsel_config,
                projected_schema,
                num_partitions,
            )
        }
    }

    /// Create unordered pool — all partitions pull from the same morsel source.
    fn new_unordered(
        config: &FileScanConfig,
        file_opener: Arc<dyn FileOpener>,
        _metrics: &ExecutionPlanMetricsSet,
        morsel_config: MorselConfig,
        projected_schema: SchemaRef,
        num_partitions: usize,
    ) -> Result<Self> {
        // Collect all files from all partitions into a single list
        let all_files: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(|group| group.iter().cloned())
            .collect();

        // Create per-partition channels
        let mut receivers = Vec::with_capacity(num_partitions);
        let mut senders = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            let (tx, rx) = mpsc::channel(morsel_config.morsel_buffer_size);
            senders.push(tx);
            receivers.push(tokio::sync::Mutex::new(rx));
        }

        // Spawn background task that opens files and round-robin distributes morsels
        let max_concurrent = morsel_config.max_concurrent_opens;
        let limit = config.limit;
        tokio::spawn(async move {
            Self::open_and_distribute_unordered(
                all_files,
                file_opener,
                senders,
                max_concurrent,
                limit,
            )
            .await;
        });

        Ok(Self {
            receivers,
            projected_schema,
        })
    }

    /// Background task for unordered mode: opens files, splits into morsels,
    /// and round-robin distributes to partition channels.
    async fn open_and_distribute_unordered(
        files: Vec<PartitionedFile>,
        opener: Arc<dyn FileOpener>,
        senders: Vec<mpsc::Sender<Box<dyn FileMorsel>>>,
        max_concurrent: usize,
        _limit: Option<usize>,
    ) {
        use futures::stream::FuturesOrdered;

        let mut file_iter = files.into_iter();
        let mut pending_opens = FuturesOrdered::new();
        let mut sender_idx = 0;
        let num_senders = senders.len();

        // Seed the initial batch of concurrent opens
        for _ in 0..max_concurrent {
            if let Some(file) = file_iter.next() {
                match opener.open_morsels(file) {
                    Ok(future) => pending_opens.push_back(future),
                    Err(e) => {
                        debug!("Error starting morsel open: {e}");
                        continue;
                    }
                }
            }
        }

        loop {
            let morsels = match pending_opens.next().await {
                Some(Ok(morsels)) => morsels,
                Some(Err(e)) => {
                    debug!("Error opening file for morsels: {e}");
                    // Start next file to replace the failed one
                    if let Some(file) = file_iter.next() {
                        match opener.open_morsels(file) {
                            Ok(future) => pending_opens.push_back(future),
                            Err(e) => debug!("Error starting morsel open: {e}"),
                        }
                    }
                    continue;
                }
                None => break, // All files processed
            };

            // Start opening next file to maintain concurrency
            if let Some(file) = file_iter.next() {
                match opener.open_morsels(file) {
                    Ok(future) => pending_opens.push_back(future),
                    Err(e) => debug!("Error starting morsel open: {e}"),
                }
            }

            // Distribute morsels round-robin across partitions
            for morsel in morsels {
                let idx = sender_idx % num_senders;
                sender_idx += 1;
                if senders[idx].send(morsel).await.is_err() {
                    // Receiver dropped — check if all are gone
                    let all_closed = senders.iter().all(|s| s.is_closed());
                    if all_closed {
                        return;
                    }
                }
            }
        }
    }

    /// Create ordered pool — each partition gets morsels only from its own file group.
    fn new_ordered(
        config: &FileScanConfig,
        file_opener: Arc<dyn FileOpener>,
        _metrics: &ExecutionPlanMetricsSet,
        morsel_config: MorselConfig,
        projected_schema: SchemaRef,
    ) -> Result<Self> {
        let mut receivers = Vec::with_capacity(config.file_groups.len());

        for group in &config.file_groups {
            let (tx, rx) = mpsc::channel(morsel_config.morsel_buffer_size);
            receivers.push(tokio::sync::Mutex::new(rx));

            let files: Vec<PartitionedFile> = group.iter().cloned().collect();
            let opener = Arc::clone(&file_opener);
            let max_concurrent = morsel_config.max_concurrent_opens;

            // For ordered mode, each partition has its own background task
            tokio::spawn(async move {
                Self::open_and_distribute_ordered(files, opener, tx, max_concurrent)
                    .await;
            });
        }

        Ok(Self {
            receivers,
            projected_schema,
        })
    }

    /// Background task for ordered mode: opens files sequentially within a
    /// partition and sends morsels in order.
    async fn open_and_distribute_ordered(
        files: Vec<PartitionedFile>,
        opener: Arc<dyn FileOpener>,
        sender: mpsc::Sender<Box<dyn FileMorsel>>,
        _max_concurrent: usize,
    ) {
        // In ordered mode, files must be processed sequentially to maintain order
        for file in files {
            let morsels = match opener.open_morsels(file) {
                Ok(future) => match future.await {
                    Ok(morsels) => morsels,
                    Err(e) => {
                        debug!("Error opening file for ordered morsels: {e}");
                        continue;
                    }
                },
                Err(e) => {
                    debug!("Error starting ordered morsel open: {e}");
                    continue;
                }
            };

            // Send morsels in order
            for morsel in morsels {
                if sender.send(morsel).await.is_err() {
                    return; // Receiver dropped
                }
            }
        }
    }

    /// Get the next morsel for the given partition.
    ///
    /// Returns `None` when all morsels have been consumed (no more work).
    pub async fn next_morsel(
        &self,
        partition: usize,
    ) -> Option<Box<dyn FileMorsel>> {
        self.receivers[partition].lock().await.recv().await
    }

    /// Get the projected schema for streams produced by this pool.
    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }
}

/// Per-partition stream backed by a shared [`MorselPool`].
///
/// Each `MorselStream` pulls morsels from the pool on demand and streams
/// their `RecordBatch`es to the consumer. When one morsel is exhausted,
/// the next is automatically fetched from the pool.
pub struct MorselStream {
    /// The shared morsel pool
    pool: Arc<MorselPool>,
    /// This stream's partition ID
    partition: usize,
    /// Current state of the stream
    state: MorselStreamState,
    /// Remaining row limit
    remain: Option<usize>,
    /// Metrics
    baseline_metrics: BaselineMetrics,
    /// Count of morsels executed
    morsels_executed: Count,
}

enum MorselStreamState {
    /// Need to fetch the next morsel from the pool
    FetchMorsel,
    /// Currently executing a morsel's stream
    Executing {
        stream: SendableRecordBatchStream,
    },
    /// Waiting for a morsel from the pool
    WaitingForMorsel {
        future: Pin<Box<dyn Future<Output = Option<Box<dyn FileMorsel>>> + Send>>,
    },
    /// Stream is done
    Done,
}

impl MorselStream {
    /// Create a new `MorselStream` for the given partition.
    pub fn new(
        pool: Arc<MorselPool>,
        partition: usize,
        limit: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            pool,
            partition,
            state: MorselStreamState::FetchMorsel,
            remain: limit,
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            morsels_executed: MetricBuilder::new(metrics)
                .counter("morsels_executed", partition),
        }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                MorselStreamState::FetchMorsel => {
                    let pool = Arc::clone(&self.pool);
                    let partition = self.partition;
                    let future = Box::pin(async move {
                        pool.next_morsel(partition).await
                    });
                    self.state = MorselStreamState::WaitingForMorsel { future };
                }
                MorselStreamState::WaitingForMorsel { future } => {
                    match ready!(future.as_mut().poll(cx)) {
                        Some(morsel) => {
                            self.morsels_executed.add(1);
                            match morsel.execute() {
                                Ok(stream) => {
                                    self.state =
                                        MorselStreamState::Executing { stream };
                                }
                                Err(e) => {
                                    self.state = MorselStreamState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                        None => {
                            self.state = MorselStreamState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                MorselStreamState::Executing { stream } => {
                    match ready!(stream.as_mut().poll_next(cx)) {
                        Some(Ok(batch)) => {
                            let batch = match &mut self.remain {
                                Some(remain) => {
                                    if *remain > batch.num_rows() {
                                        *remain -= batch.num_rows();
                                        batch
                                    } else {
                                        let batch = batch.slice(0, *remain);
                                        *remain = 0;
                                        self.state = MorselStreamState::Done;
                                        batch
                                    }
                                }
                                None => batch,
                            };
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Some(Err(e)) => {
                            self.state = MorselStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // Current morsel exhausted, fetch next
                            self.state = MorselStreamState::FetchMorsel;
                        }
                    }
                }
                MorselStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Stream for MorselStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.poll_inner(cx);
        self.baseline_metrics.record_poll(result)
    }
}

impl RecordBatchStream for MorselStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.pool.projected_schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_scan_config::FileScanConfigBuilder;
    use crate::file_stream::FileOpenFuture;
    use crate::test_util::MockSource;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::assert_batches_eq;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_batch(start: i32, count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let values: Vec<i32> = (start..start + count as i32).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    struct TestMorselOpener {
        records: Vec<RecordBatch>,
        call_count: AtomicUsize,
    }

    impl TestMorselOpener {
        fn new(records: Vec<RecordBatch>) -> Self {
            Self {
                records,
                call_count: AtomicUsize::new(0),
            }
        }
    }

    impl FileOpener for TestMorselOpener {
        fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let iterator = self.records.clone().into_iter().map(Ok);
            let stream = futures::stream::iter(iterator).boxed();
            Ok(futures::future::ready(Ok(stream)).boxed())
        }
    }

    fn make_test_config(
        schema: SchemaRef,
        num_partitions: usize,
        files_per_partition: usize,
        preserve_order: bool,
    ) -> FileScanConfig {
        let table_schema =
            crate::table_schema::TableSchema::new(Arc::clone(&schema), vec![]);
        let file_source = Arc::new(MockSource::new(table_schema));

        let mut builder =
            FileScanConfigBuilder::new(ObjectStoreUrl::parse("test:///").unwrap(), file_source)
                .with_preserve_order(preserve_order);

        for partition in 0..num_partitions {
            let files: Vec<PartitionedFile> = (0..files_per_partition)
                .map(|f| {
                    PartitionedFile::new(
                        format!("file_p{partition}_f{f}"),
                        100,
                    )
                })
                .collect();
            builder = builder.with_file_group(crate::file_groups::FileGroup::new(files));
        }

        builder.build()
    }

    #[tokio::test]
    async fn test_morsel_stream_basic() {
        let batch = make_batch(0, 3);
        let schema = batch.schema();

        let config = make_test_config(schema, 1, 2, false);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let metrics = ExecutionPlanMetricsSet::new();

        let pool = Arc::new(
            MorselPool::new(&config, opener, &metrics, MorselConfig::default()).unwrap(),
        );

        let stream = MorselStream::new(Arc::clone(&pool), 0, None, &metrics);
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "+---+",
        ], &batches);
    }

    #[tokio::test]
    async fn test_morsel_stream_with_limit() {
        let batch = make_batch(0, 3);
        let schema = batch.schema();

        let config = make_test_config(schema, 1, 2, false);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let metrics = ExecutionPlanMetricsSet::new();

        let pool = Arc::new(
            MorselPool::new(&config, opener, &metrics, MorselConfig::default()).unwrap(),
        );

        let stream = MorselStream::new(Arc::clone(&pool), 0, Some(4), &metrics);
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "+---+",
        ], &batches);
    }

    #[tokio::test]
    async fn test_morsel_pool_ordered() {
        let batch = make_batch(0, 2);
        let schema = batch.schema();

        // 2 partitions, 1 file each, ordered mode
        let config = make_test_config(schema, 2, 1, true);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let metrics = ExecutionPlanMetricsSet::new();

        let pool = Arc::new(
            MorselPool::new(&config, opener, &metrics, MorselConfig::default()).unwrap(),
        );

        // Each partition should get exactly its own file's morsels
        let stream0 = MorselStream::new(Arc::clone(&pool), 0, None, &metrics);
        let batches0: Vec<RecordBatch> = stream0
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(batches0.len(), 1);
        assert_eq!(batches0[0].num_rows(), 2);

        let stream1 = MorselStream::new(Arc::clone(&pool), 1, None, &metrics);
        let batches1: Vec<RecordBatch> = stream1
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(batches1.len(), 1);
        assert_eq!(batches1[0].num_rows(), 2);
    }
}
