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

//! [`MorselSource`] and [`MorselStream`] for morsel-driven file scanning.
//!
//! This module implements the core morsel execution engine using Stream
//! composition with work-stealing deques for cache locality.
//!
//! # Architecture
//!
//! ```text
//!                    ┌─────────────────────────────┐
//!                    │  Feeder Stream (shared)      │
//!                    │  files.map(open_morsels)     │
//!                    │      .buffered(N)            │
//!                    │  produces Vec<FileMorsel>    │
//!                    └──────────┬──────────────────┘
//!                               │
//!         partition K pulls a batch, pushes surplus to own queue
//!                               │
//!    ┌──────────────┬───────────┼───────────────┐
//!    │              │           │               │
//!  Queue[0]      Queue[1]   Queue[2]        Queue[3]
//!    │              │           │               │
//!  MorselStream  MorselStream MorselStream  MorselStream
//!  1. pop local  1. pop local 1. pop local  1. pop local
//!  2. steal      2. steal     2. steal      2. steal
//!  3. pull feeder 3. pull     3. pull       3. pull
//! ```
//!
//! In **unordered mode** (default), all partitions share a single
//! `MorselSource`. Each partition preferentially pops from its own
//! local queue, then steals from others, then pulls from the feeder.
//! This provides both work stealing and cache locality.
//!
//! In **ordered mode** (`preserve_order == true`), each partition gets
//! its own `MorselSource` with a dedicated feeder stream over its own
//! file group.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::PartitionedFile;
use crate::file_stream::{FileMorsel, FileOpener};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use crossbeam_deque::{Injector, Steal};
use datafusion_common::error::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder,
};

use futures::{Stream, StreamExt as _, ready};

/// Configuration for morsel-driven scanning.
///
/// Controls the pipeline:
/// - **File Opening**: Opens files and splits them into morsels.
///   Controlled by `max_concurrent_opens`.
/// - **Morsel Execution**: Executes morsels to produce RecordBatches.
///   `max_concurrent_executions` reserved for future use.
/// - **Output Buffering**: `morsel_buffer_size` reserved for future use
///   (demand-driven model provides natural backpressure).
#[derive(Debug, Clone)]
pub struct MorselConfig {
    /// Maximum number of files to open concurrently.
    pub max_concurrent_opens: usize,
    /// How many opened-but-not-yet-executing files to buffer.
    /// Currently merged with `max_concurrent_opens`.
    pub open_prefetch_depth: usize,
    /// Maximum number of morsels to execute concurrently per partition.
    /// Reserved for future use.
    pub max_concurrent_executions: usize,
    /// Reserved for future use (demand-driven model provides backpressure).
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

/// Shared morsel source with work-stealing deques for cache locality.
///
/// Each partition has a local queue (crossbeam `Injector`). When a partition
/// needs work it: (1) pops from its own queue, (2) steals from other
/// partitions' queues, (3) pulls from the shared feeder stream.
///
/// When pulling from the feeder, the partition takes the first morsel and
/// pushes surplus morsels (from the same file) to its own queue. This keeps
/// same-file morsels together on one partition for cache locality.
pub struct MorselSource {
    /// Per-partition lock-free work-stealing queues.
    local_queues: Vec<Injector<Box<dyn FileMorsel>>>,

    /// Shared feeder stream producing `Vec<FileMorsel>` per file.
    /// Protected by a Mutex so only one partition pulls at a time.
    feeder: tokio::sync::Mutex<
        Pin<Box<dyn Stream<Item = Result<Vec<Box<dyn FileMorsel>>>> + Send>>,
    >,

    /// Schema of the output stream.
    projected_schema: SchemaRef,
}

impl MorselSource {
    /// Create a new `MorselSource`.
    ///
    /// # Arguments
    /// * `files` - Files to open and split into morsels
    /// * `opener` - The opener used to create morsels from files
    /// * `morsel_config` - Configuration for concurrency
    /// * `num_partitions` - Number of local queues to create
    /// * `projected_schema` - Schema of the output stream
    pub fn new(
        files: Vec<PartitionedFile>,
        opener: Arc<dyn FileOpener>,
        morsel_config: &MorselConfig,
        num_partitions: usize,
        projected_schema: SchemaRef,
    ) -> Self {
        let local_queues = (0..num_partitions).map(|_| Injector::new()).collect();
        let feeder = build_morsel_stream(files, opener, morsel_config.max_concurrent_opens);

        Self {
            local_queues,
            feeder: tokio::sync::Mutex::new(feeder),
            projected_schema,
        }
    }

    /// Get the next morsel for the given partition.
    ///
    /// Three-tier lookup:
    /// 1. Own local queue (cache-hot, same-file morsels)
    /// 2. Steal from other partitions' queues
    /// 3. Pull from feeder stream (may need to wait for IO)
    pub async fn next_morsel(
        &self,
        partition: usize,
    ) -> Option<Result<Box<dyn FileMorsel>>> {
        // 1. Check own local queue
        if let Steal::Success(morsel) = self.local_queues[partition].steal() {
            return Some(Ok(morsel));
        }

        // 2. Steal from other partitions' queues
        let n = self.local_queues.len();
        for i in 1..n {
            let other = (partition + i) % n;
            if let Steal::Success(morsel) = self.local_queues[other].steal() {
                return Some(Ok(morsel));
            }
        }

        // 3. Pull from feeder stream (may need to wait for IO)
        let mut feeder = self.feeder.lock().await;

        // Double-check local queue after acquiring lock
        // (another partition may have fed us while we waited)
        if let Steal::Success(morsel) = self.local_queues[partition].steal() {
            return Some(Ok(morsel));
        }

        match feeder.next().await? {
            Ok(mut morsels) => {
                let first = morsels.remove(0);
                // Push surplus to own local queue
                // (locality: same-file morsels stay together)
                for morsel in morsels {
                    self.local_queues[partition].push(morsel);
                }
                Some(Ok(first))
            }
            Err(e) => Some(Err(e)),
        }
    }

    /// Get the projected schema for streams produced by this source.
    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }
}

/// Build the feeder stream that produces `Vec<FileMorsel>` per file.
///
/// Files are opened concurrently (up to `max_concurrent_opens`) but each
/// file produces a `Vec` of morsels that is NOT flattened — this allows
/// the consumer to keep same-file morsels together for locality.
fn build_morsel_stream(
    files: Vec<PartitionedFile>,
    opener: Arc<dyn FileOpener>,
    max_concurrent_opens: usize,
) -> Pin<Box<dyn Stream<Item = Result<Vec<Box<dyn FileMorsel>>>> + Send>> {
    Box::pin(
        futures::stream::iter(files)
            .map(move |file| {
                let opener = Arc::clone(&opener);
                async move { opener.open_morsels(file)?.await }
            })
            .buffered(max_concurrent_opens),
    )
}

/// Per-partition stream backed by a shared [`MorselSource`].
///
/// Each `MorselStream` pulls morsels from the source on demand and streams
/// their `RecordBatch`es to the consumer. When one morsel is exhausted,
/// the next is automatically fetched from the source.
pub struct MorselStream {
    /// The shared morsel source
    source: Arc<MorselSource>,
    /// This stream's partition ID within the source
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
    /// Need to fetch the next morsel from the source
    FetchMorsel,
    /// Currently executing a morsel's stream
    Executing { stream: SendableRecordBatchStream },
    /// Waiting for a morsel from the source
    WaitingForMorsel {
        future:
            Pin<Box<dyn Future<Output = Option<Result<Box<dyn FileMorsel>>>> + Send>>,
    },
    /// Stream is done
    Done,
}

impl MorselStream {
    /// Create a new `MorselStream` for the given partition.
    pub fn new(
        source: Arc<MorselSource>,
        partition: usize,
        limit: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            source,
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
                    let source = Arc::clone(&self.source);
                    let partition = self.partition;
                    let future =
                        Box::pin(async move { source.next_morsel(partition).await });
                    self.state = MorselStreamState::WaitingForMorsel { future };
                }
                MorselStreamState::WaitingForMorsel { future } => {
                    match ready!(future.as_mut().poll(cx)) {
                        Some(Ok(morsel)) => {
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
                        Some(Err(e)) => {
                            self.state = MorselStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
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
        Arc::clone(self.source.projected_schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
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

        let mut builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            file_source,
        )
        .with_preserve_order(preserve_order);

        for partition in 0..num_partitions {
            let files: Vec<PartitionedFile> = (0..files_per_partition)
                .map(|f| PartitionedFile::new(format!("file_p{partition}_f{f}"), 100))
                .collect();
            builder = builder.with_file_group(crate::file_groups::FileGroup::new(files));
        }

        builder.build()
    }

    fn make_source(
        config: &FileScanConfig,
        opener: Arc<dyn FileOpener>,
        morsel_config: &MorselConfig,
    ) -> Arc<MorselSource> {
        let projected_schema = config.projected_schema().unwrap();
        let num_partitions = config.file_groups.len();

        let all_files: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(|group: &crate::file_groups::FileGroup| group.iter().cloned())
            .collect();

        Arc::new(MorselSource::new(
            all_files,
            opener,
            morsel_config,
            num_partitions,
            projected_schema,
        ))
    }

    #[tokio::test]
    async fn test_morsel_stream_basic() {
        let batch = make_batch(0, 3);
        let schema = batch.schema();

        let config = make_test_config(schema, 1, 2, false);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let metrics = ExecutionPlanMetricsSet::new();
        let morsel_config = MorselConfig::default();

        let source = make_source(&config, opener, &morsel_config);
        let stream = MorselStream::new(Arc::clone(&source), 0, None, &metrics);
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
        let morsel_config = MorselConfig::default();

        let source = make_source(&config, opener, &morsel_config);
        let stream = MorselStream::new(Arc::clone(&source), 0, Some(4), &metrics);
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
    async fn test_morsel_source_ordered() {
        let batch = make_batch(0, 2);
        let schema = batch.schema();

        // 2 partitions, 1 file each, ordered mode
        let config = make_test_config(schema, 2, 1, true);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let metrics = ExecutionPlanMetricsSet::new();
        let morsel_config = MorselConfig::default();
        let projected_schema = config.projected_schema().unwrap();

        // Ordered: each partition gets its own MorselSource
        let source0 = Arc::new(MorselSource::new(
            config.file_groups[0].iter().cloned().collect(),
            Arc::clone(&opener) as Arc<dyn FileOpener>,
            &morsel_config,
            1,
            Arc::clone(&projected_schema),
        ));
        let source1 = Arc::new(MorselSource::new(
            config.file_groups[1].iter().cloned().collect(),
            opener,
            &morsel_config,
            1,
            projected_schema,
        ));

        // Each partition should get exactly its own file's morsels
        let stream0 = MorselStream::new(Arc::clone(&source0), 0, None, &metrics);
        let batches0: Vec<RecordBatch> = stream0
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(batches0.len(), 1);
        assert_eq!(batches0[0].num_rows(), 2);

        let stream1 = MorselStream::new(Arc::clone(&source1), 0, None, &metrics);
        let batches1: Vec<RecordBatch> = stream1
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(batches1.len(), 1);
        assert_eq!(batches1[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_work_stealing() {
        // Create a source with 2 partitions and 4 files
        let batch = make_batch(0, 2);
        let schema = batch.schema();

        let config = make_test_config(schema, 2, 2, false);
        let opener = Arc::new(TestMorselOpener::new(vec![batch.clone()]));
        let morsel_config = MorselConfig::default();

        let source = make_source(&config, opener, &morsel_config);

        // Drain all morsels from partition 0 only — it should steal
        // from partition 1's queue if needed, or pull from feeder
        let mut count = 0;
        while source.next_morsel(0).await.is_some() {
            count += 1;
        }
        // 2 partitions × 2 files = 4 files total, each producing 1 morsel
        assert_eq!(count, 4);

        // Partition 1 should have nothing left
        assert!(source.next_morsel(1).await.is_none());
    }

    #[tokio::test]
    async fn test_error_propagation() {
        use datafusion_common::internal_err;

        struct ErrorOpener;
        impl FileOpener for ErrorOpener {
            fn open(
                &self,
                _partitioned_file: PartitionedFile,
            ) -> Result<FileOpenFuture> {
                Ok(futures::future::ready(internal_err!("test error")).boxed())
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let config = make_test_config(schema, 1, 1, false);
        let opener = Arc::new(ErrorOpener);
        let morsel_config = MorselConfig::default();

        let source = make_source(&config, opener, &morsel_config);
        let result = source.next_morsel(0).await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_locality() {
        use crate::file_stream::FileOpenMorselFuture;

        // Opener that returns multiple morsels per file
        struct MultiMorselOpener {
            records: Vec<RecordBatch>,
        }

        impl FileOpener for MultiMorselOpener {
            fn open(
                &self,
                _partitioned_file: PartitionedFile,
            ) -> Result<FileOpenFuture> {
                let iterator = self.records.clone().into_iter().map(Ok);
                let stream = futures::stream::iter(iterator).boxed();
                Ok(futures::future::ready(Ok(stream)).boxed())
            }

            fn open_morsels(
                &self,
                _partitioned_file: PartitionedFile,
            ) -> Result<FileOpenMorselFuture> {
                let records = self.records.clone();
                Ok(Box::pin(async move {
                    // Return 3 morsels per file
                    let mut morsels: Vec<Box<dyn FileMorsel>> = Vec::new();
                    for record in records {
                        let stream =
                            futures::stream::iter(vec![Ok(record)]).boxed();
                        morsels.push(Box::new(
                            crate::file_stream::StreamMorsel::new(stream),
                        ));
                    }
                    Ok(morsels)
                }))
            }
        }

        let batch1 = make_batch(1, 1);
        let batch2 = make_batch(2, 1);
        let batch3 = make_batch(3, 1);

        let opener = Arc::new(MultiMorselOpener {
            records: vec![batch1, batch2, batch3],
        });

        let schema = Arc::new(Schema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let projected_schema = schema;

        // 2 partitions, 1 file
        let files = vec![PartitionedFile::new("file0", 100)];
        let morsel_config = MorselConfig::default();
        let source = Arc::new(MorselSource::new(
            files,
            opener,
            &morsel_config,
            2,
            projected_schema,
        ));

        // Partition 0 pulls from feeder: gets first morsel, pushes 2 to its queue
        let m0 = source.next_morsel(0).await.unwrap().unwrap();
        // Next call should come from partition 0's local queue (locality)
        let m1 = source.next_morsel(0).await.unwrap().unwrap();
        // And the third
        let m2 = source.next_morsel(0).await.unwrap().unwrap();

        // Execute all three and verify we got all the data
        let b0 = m0.execute().unwrap();
        let b1 = m1.execute().unwrap();
        let b2 = m2.execute().unwrap();

        let batches: Vec<RecordBatch> = futures::stream::select_all(vec![b0, b1, b2])
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(batches.len(), 3);
    }
}
