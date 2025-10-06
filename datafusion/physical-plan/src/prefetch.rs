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

//! Execution plan for prefetching RecordBatches

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_datafusion_err, internal_err, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::stream::{Stream, StreamExt};

use crate::execution_plan::ExecutionPlanProperties;
use crate::filter_pushdown::{FilterDescription, FilterPushdownPropagation};
use crate::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, SpillMetrics, Time,
};
use crate::spill::get_record_batch_memory_size;
use crate::spill::spill_manager::SpillManager;
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics};

/// Represents a prefetched batch that may be in memory or spilled to disk
enum PrefetchedBatch {
    /// Batch stored in memory
    InMemory(RecordBatch),
    /// Batch spilled to disk
    Spilled(RefCountedTempFile),
}

/// Execution plan that prefetches RecordBatches from its child with memory-aware spilling
///
/// `PrefetchExec` asynchronously pulls up to `PREFETCH_BUFFER_SIZE` batches from its child
/// ExecutionPlan and buffers them. When memory pressure is detected (via `MemoryReservation`),
/// batches are spilled to disk using Arrow IPC format.
///
/// This is designed to be a transparent wrapper - it preserves ordering, partitioning,
/// and equivalence properties from its child.
#[derive(Debug, Clone)]
pub struct PrefetchExec {
    /// The child execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Cached plan properties (schema, ordering, partitioning, etc.)
    cache: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl PrefetchExec {
    /// Create a new PrefetchExec
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        // Pass through properties from child (transparent wrapper)
        let cache = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );

        let metrics = ExecutionPlanMetricsSet::new();

        Ok(Self {
            input,
            cache,
            metrics,
        })
    }

    /// Get the input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for PrefetchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PrefetchExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PrefetchExec")
            }
        }
    }
}

impl ExecutionPlan for PrefetchExec {
    fn name(&self) -> &'static str {
        "PrefetchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // We preserve input order since we process batches sequentially
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("PrefetchExec must have exactly one child");
        }
        Ok(Arc::new(PrefetchExec::try_new(Arc::clone(&children[0]))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Get input stream
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let schema = input_stream.schema();

        // Create SpillManager for this partition
        let spill_metrics = SpillMetrics::new(&self.metrics, partition);
        let spill_manager = Arc::new(SpillManager::new(
            Arc::clone(&context.runtime_env()),
            spill_metrics,
            Arc::clone(&schema),
        ));

        // Get prefetch depth from config (ensure at least 1)
        let prefetch_depth = context.session_config().options().execution.prefetch_depth;

        // Create channel for communication between background task and stream
        let (tx, rx) = tokio::sync::mpsc::channel(prefetch_depth);

        // Spawn background prefetch task
        let task = SpawnedTask::spawn(prefetch_task(
            partition,
            input_stream,
            tx,
            context,
            Arc::clone(&spill_manager),
            prefetch_depth,
        ));

        // Create metric for tracking batch wait time
        let batch_wait_time =
            MetricBuilder::new(&self.metrics).subset_time("batch_wait_time", partition);

        // Return stream that reads from channel
        Ok(Box::pin(PrefetchStream {
            schema: Arc::clone(&schema),
            receiver: rx,
            _task: task,
            partition_id: partition,
            spill_manager: (*spill_manager).clone(),
            state: PrefetchStreamState::Idle,
            wait_start: None,
            batch_count: 0,
            batch_wait_time,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn cardinality_effect(&self) -> crate::execution_plan::CardinalityEffect {
        self.input.cardinality_effect()
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: crate::filter_pushdown::FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: crate::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: crate::filter_pushdown::ChildPushdownResult,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &datafusion_common::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.input
            .repartitioned(target_partitions, config)
            .map(|opt| {
                opt.map(|new_input| {
                    Arc::new(PrefetchExec::try_new(new_input).unwrap())
                        as Arc<dyn ExecutionPlan>
                })
            })
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &crate::projection::ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.input
            .try_swapping_with_projection(projection)
            .map(|opt| {
                opt.map(|new_input| {
                    Arc::new(PrefetchExec::try_new(new_input).unwrap())
                        as Arc<dyn ExecutionPlan>
                })
            })
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input.with_fetch(limit).map(|new_input| {
            Arc::new(PrefetchExec::try_new(new_input).unwrap()) as Arc<dyn ExecutionPlan>
        })
    }
}

/// Background task that prefetches batches from the input stream
async fn prefetch_task(
    partition: usize,
    mut input: SendableRecordBatchStream,
    tx: tokio::sync::mpsc::Sender<Result<PrefetchedBatch>>,
    context: Arc<TaskContext>,
    spill_manager: Arc<SpillManager>,
    prefetch_depth: usize,
) -> Result<()> {
    // Only create memory reservation if prefetching is enabled (depth > 1)
    // With depth=1, there's no buffering so spilling is unnecessary
    let mut reservation = if prefetch_depth > 1 {
        Some(
            MemoryConsumer::new(format!("PrefetchTask[{partition}]"))
                .with_can_spill(true)
                .register(&context.runtime_env().memory_pool),
        )
    } else {
        None
    };

    while let Some(batch_result) = input.next().await {
        let batch = match batch_result {
            Ok(batch) => batch,
            Err(e) => {
                // Send error to channel
                let _ = tx.send(Err(e)).await;
                return Ok(());
            }
        };

        // Handle batch based on whether prefetching (and spilling) is enabled
        let prefetched_batch = if let Some(ref mut res) = reservation {
            // Prefetching is enabled - try to reserve memory and spill if needed
            let batch_size = get_record_batch_memory_size(&batch);

            match res.try_grow(batch_size) {
                Ok(_) => {
                    // Memory reservation succeeded - keep batch in memory
                    PrefetchedBatch::InMemory(batch)
                }
                Err(_) => {
                    // Memory reservation failed - spill to disk
                    let spill_file = match spill_manager
                        .spill_record_batch_and_finish(&[batch], "PrefetchExec")?
                    {
                        Some(file) => file,
                        None => {
                            return Err(internal_datafusion_err!(
                                "Failed to spill batch - no file was created"
                            ));
                        }
                    };

                    PrefetchedBatch::Spilled(spill_file)
                }
            }
        } else {
            // Prefetching disabled (depth=1) - just pass through without spilling
            PrefetchedBatch::InMemory(batch)
        };

        // Send batch to channel - blocks if channel is full (natural backpressure!)
        if tx.send(Ok(prefetched_batch)).await.is_err() {
            // Receiver dropped (e.g., LIMIT reached)
            return Ok(());
        }
    }

    Ok(())
}

/// Stream that reads prefetched batches from the channel
struct PrefetchStream {
    /// Schema of the stream
    schema: SchemaRef,
    /// Channel receiver for batches
    receiver: tokio::sync::mpsc::Receiver<Result<PrefetchedBatch>>,
    /// Background task handle (keeps task alive)
    _task: SpawnedTask<Result<()>>,
    /// Partition ID for debugging
    partition_id: usize,
    /// SpillManager for reading spilled batches
    spill_manager: SpillManager,
    /// Current state of the stream
    state: PrefetchStreamState,
    /// Timestamp when we started waiting for a batch
    wait_start: Option<std::time::Instant>,
    /// Count of batches received
    batch_count: usize,
    /// Metric tracking time spent waiting for batches
    batch_wait_time: Time,
}

impl Drop for PrefetchStream {
    fn drop(&mut self) {
        // Close the receiver to signal the background task to stop
        self.receiver.close();
    }
}

/// State for PrefetchStream when reading spilled batches
enum PrefetchStreamState {
    /// Waiting for next item from channel
    Idle,
    /// Reading a spilled batch from disk
    ReadingSpill(SendableRecordBatchStream),
}

impl Stream for PrefetchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PrefetchStreamState::Idle => {
                    // Start timing if we haven't already
                    if self.wait_start.is_none() {
                        self.wait_start = Some(std::time::Instant::now());
                    }

                    // Poll channel for next buffered batch
                    match self.receiver.poll_recv(cx) {
                        Poll::Ready(Some(batch_result)) => {
                            // Record wait time
                            if let Some(start) = self.wait_start.take() {
                                let wait_time = start.elapsed();
                                self.batch_wait_time.add_duration(wait_time);
                            }

                            self.batch_count += 1;

                            match batch_result {
                                Ok(PrefetchedBatch::InMemory(batch)) => {
                                    return Poll::Ready(Some(Ok(batch)));
                                }
                                Ok(PrefetchedBatch::Spilled(spill_file)) => {
                                    // Read the spilled batch from disk
                                    match self
                                        .spill_manager
                                        .read_spill_as_stream(spill_file, None)
                                    {
                                        Ok(stream) => {
                                            // Transition to ReadingSpill state
                                            self.state =
                                                PrefetchStreamState::ReadingSpill(stream);
                                            // Continue loop to immediately poll the spill stream
                                        }
                                        Err(e) => {
                                            return Poll::Ready(Some(Err(e)));
                                        }
                                    }
                                }
                                Err(e) => {
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                PrefetchStreamState::ReadingSpill(stream) => {
                    // Poll the spill stream for the batch
                    match Pin::new(stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Transition back to Idle state
                            self.state = PrefetchStreamState::Idle;
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            // Transition back to Idle state
                            self.state = PrefetchStreamState::Idle;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Spill stream exhausted (this means the batch was empty or already read)
                            // Transition back to Idle and continue to next channel item
                            self.state = PrefetchStreamState::Idle;
                            // Continue loop to get next batch from channel
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for PrefetchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestMemoryExec;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;

    fn create_test_batch(id: i32, size: usize) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![id; size]))])
            .unwrap()
    }

    #[tokio::test]
    async fn test_prefetch_basic() -> Result<()> {
        let batch1 = create_test_batch(1, 10);
        let batch2 = create_test_batch(2, 10);
        let schema = batch1.schema();

        let input =
            TestMemoryExec::try_new_exec(&[vec![batch1, batch2]], schema.clone(), None)?;
        let prefetch = PrefetchExec::try_new(input)?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = prefetch.execute(0, task_ctx)?;

        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        assert_eq!(batches.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_with_memory_pressure() -> Result<()> {
        use datafusion_execution::config::SessionConfig;

        // Create batches that will exceed memory limit
        let batch1 = create_test_batch(1, 1000);
        let batch2 = create_test_batch(2, 1000);
        let batch3 = create_test_batch(3, 1000);
        let schema = batch1.schema();

        // Set a low memory limit to trigger spilling
        // Each batch is ~4000 bytes, so with a limit of 5000 bytes, we should spill
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(5000, 1.0)
            .build_arc()?;

        let input = TestMemoryExec::try_new_exec(
            &[vec![batch1.clone(), batch2.clone(), batch3.clone()]],
            schema.clone(),
            None,
        )?;
        let prefetch = Arc::new(PrefetchExec::try_new(input)?);

        // Enable prefetching with depth > 1 to enable spilling
        let mut session_config = SessionConfig::default();
        session_config =
            session_config.set_str("datafusion.execution.prefetch_depth", "5");
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(session_config),
        );
        let mut stream = prefetch.execute(0, Arc::clone(&task_ctx))?;

        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        // Should still get all batches even with spilling
        assert_eq!(batches.len(), 3);

        // Verify that some spilling occurred
        let metrics = prefetch.metrics().unwrap();

        // Find the spill_count metric
        let spill_count = metrics
            .iter()
            .find(|m| m.value().name() == "spill_count")
            .map(|m| m.value().as_usize())
            .unwrap_or(0);

        // With a 5000 byte limit and ~4000 byte batches, we expect at least 1 spill
        assert!(
            spill_count > 0,
            "Expected at least 1 spill with memory limit of 5000 bytes, but got {spill_count}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_preserves_order() -> Result<()> {
        let batches: Vec<RecordBatch> =
            (0..5).map(|i| create_test_batch(i, 10)).collect();
        let schema = batches[0].schema();

        let input =
            TestMemoryExec::try_new_exec(&[batches.clone()], schema.clone(), None)?;
        let prefetch = PrefetchExec::try_new(input)?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = prefetch.execute(0, task_ctx)?;

        let mut output_batches = vec![];
        while let Some(batch) = stream.next().await {
            output_batches.push(batch?);
        }

        // Verify order is preserved
        assert_eq!(output_batches.len(), 5);
        for (i, batch) in output_batches.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(array.value(0), i as i32);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_early_termination() -> Result<()> {
        use crate::collect;
        use crate::test::assert_is_pending;
        use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
        use futures::FutureExt;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let prefetch = Arc::new(PrefetchExec::try_new(blocking_exec)?);

        let task_ctx = Arc::new(TaskContext::default());
        let fut = collect(prefetch, task_ctx);
        let mut fut = fut.boxed();

        // Should be pending because BlockingExec doesn't produce data immediately
        assert_is_pending(&mut fut);

        // Drop the future early (simulating LIMIT or early termination)
        drop(fut);

        // Verify all references are eventually dropped (no leaks)
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_error_propagation() -> Result<()> {
        use crate::test::exec::MockExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = create_test_batch(1, 10);

        // MockExec that returns one good batch then an error
        let mock_exec = Arc::new(MockExec::new(
            vec![
                Ok(batch),
                Err(datafusion_common::DataFusionError::Execution(
                    "Test error".to_string(),
                )),
            ],
            schema,
        ));
        let prefetch = PrefetchExec::try_new(mock_exec)?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = prefetch.execute(0, task_ctx)?;

        // First batch should succeed
        let first = stream.next().await;
        assert!(first.is_some());
        assert!(first.unwrap().is_ok());

        // Second should be an error
        match stream.next().await {
            Some(Err(e)) => {
                assert!(e.to_string().contains("Test error"));
            }
            _ => panic!("Expected error from stream"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_empty_input() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Empty input - no batches
        let input = TestMemoryExec::try_new_exec(&[vec![]], schema.clone(), None)?;
        let prefetch = PrefetchExec::try_new(input)?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = prefetch.execute(0, task_ctx)?;

        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        // Should complete cleanly with no batches
        assert_eq!(batches.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_multiple_spills() -> Result<()> {
        use datafusion_execution::config::SessionConfig;

        // Create 5 batches, but only allow memory for ~1 batch
        // This should trigger multiple spills
        let batches: Vec<RecordBatch> =
            (0..5).map(|i| create_test_batch(i, 1000)).collect();
        let schema = batches[0].schema();

        // Very low memory limit to force aggressive spilling
        // Each batch is ~4000 bytes, so with 4500 bytes we can only hold 1 batch
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(4500, 1.0)
            .build_arc()?;

        let input =
            TestMemoryExec::try_new_exec(&[batches.clone()], schema.clone(), None)?;
        let prefetch = Arc::new(PrefetchExec::try_new(input)?);

        // Enable prefetching with depth > 1 to enable spilling
        let mut session_config = SessionConfig::default();
        session_config =
            session_config.set_str("datafusion.execution.prefetch_depth", "5");
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(session_config),
        );
        let mut stream = prefetch.execute(0, Arc::clone(&task_ctx))?;

        let mut output_batches = vec![];
        while let Some(batch) = stream.next().await {
            output_batches.push(batch?);
        }

        // Should get all batches despite multiple spills
        assert_eq!(output_batches.len(), 5);

        // Verify data integrity - all batches should have correct IDs
        for (i, batch) in output_batches.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(array.value(0), i as i32);
        }

        // Verify that multiple spills occurred
        let metrics = prefetch.metrics().unwrap();

        // Find the spill_count metric
        let spill_count = metrics
            .iter()
            .find(|m| m.value().name() == "spill_count")
            .map(|m| m.value().as_usize())
            .unwrap_or(0);

        // With 5 batches and only room for 1, we expect at least 3 spills
        assert!(
            spill_count >= 3,
            "Expected at least 3 spills with 5 batches and memory for only 1, but got {spill_count}"
        );

        Ok(())
    }
}
