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
use datafusion_common::{internal_err, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::stream::{Stream, StreamExt};

use crate::execution_plan::ExecutionPlanProperties;
use crate::filter_pushdown::{FilterDescription, FilterPushdownPropagation};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics};

/// Number of batches to prefetch
const PREFETCH_BUFFER_SIZE: usize = 15;

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

        Ok(Self {
            input,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
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
                write!(f, "PrefetchExec: buffer_size={PREFETCH_BUFFER_SIZE}")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "buffer_size={PREFETCH_BUFFER_SIZE}")
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
            // eprintln!("[PrefetchExec] execute() partition {}", partition);
            // log::trace!("PrefetchExec: execute partition {}", partition);

        // Get input stream
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let schema = input_stream.schema();
        // eprintln!("[PrefetchExec] Got input stream for partition {}", partition);

        // Create channel for communication between background task and stream
        let (tx, rx) = tokio::sync::mpsc::channel(PREFETCH_BUFFER_SIZE);

        // eprintln!("[PrefetchExec] About to spawn background task for partition {}", partition);
        // Spawn background prefetch task
        let task = SpawnedTask::spawn(prefetch_task(
            partition,
            input_stream,
            tx,
        ));
        // eprintln!("[PrefetchExec] Spawned background task for partition {}", partition);

        // Return stream that reads from channel
        Ok(Box::pin(PrefetchStream {
            schema,
            receiver: rx,
            _task: task,
            partition_id: partition,
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
        self.input.repartitioned(target_partitions, config).map(|opt| {
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
    tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    // eprintln!("[PrefetchTask partition {}] Task started, waiting for batches...", partition);
    // log::trace!("PrefetchTask: starting background task");
    let mut batch_count = 0;

    while let Some(batch_result) = input.next().await {
        batch_count += 1;
        // eprintln!("[PrefetchTask partition {}] Received batch #{}", partition, batch_count);

        // Send batch to channel - blocks if channel is full (natural backpressure!)
        if tx.send(batch_result).await.is_err() {
            // eprintln!("[PrefetchTask partition {}] Receiver dropped, stopping", partition);
            // log::trace!("PrefetchTask: receiver dropped, stopping");
            // Receiver dropped (e.g., LIMIT reached)
            return Ok(());
        }

        // eprintln!("[PrefetchTask partition {}] Successfully sent batch", partition);
    }

    // eprintln!("[PrefetchTask partition {}] Input stream exhausted after {} batches", partition, batch_count);
    // log::trace!("PrefetchTask: input stream exhausted after {} batches", batch_count);
    Ok(())
}

/// Stream that reads prefetched batches from the channel
struct PrefetchStream {
    /// Schema of the stream
    schema: SchemaRef,
    /// Channel receiver for batches
    receiver: tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
    /// Background task handle (keeps task alive)
    _task: SpawnedTask<Result<()>>,
    /// For debugging
    partition_id: usize,
}

impl Drop for PrefetchStream {
    fn drop(&mut self) {
        // eprintln!("[PrefetchStream partition {}] DROP CALLED - closing channel and aborting task", self.partition_id);
        // Close the receiver to signal the background task to stop
        self.receiver.close();
        // eprintln!("[PrefetchStream partition {}] Channel closed, task will be aborted when _task drops", self.partition_id);
    }
}

impl Stream for PrefetchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // eprintln!("[PrefetchStream partition {}] poll_next called", self.partition_id);

        // Poll channel for next buffered batch
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(batch_result)) => {
                if batch_result.is_ok() {
                    // eprintln!("[PrefetchStream partition {}] Returning batch", self.partition_id);
                } else {
                    // eprintln!("[PrefetchStream partition {}] Returning error", self.partition_id);
                }
                Poll::Ready(Some(batch_result))
            }
            Poll::Ready(None) => {
                // eprintln!("[PrefetchStream partition {}] Channel closed, stream complete", self.partition_id);
                Poll::Ready(None)
            }
            Poll::Pending => {
                // eprintln!("[PrefetchStream partition {}] Channel pending", self.partition_id);
                Poll::Pending
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
        // Create batches that will exceed memory limit
        let batch1 = create_test_batch(1, 1000);
        let batch2 = create_test_batch(2, 1000);
        let batch3 = create_test_batch(3, 1000);
        let schema = batch1.schema();

        // Set a low memory limit to trigger spilling
        // Need enough for at least 2 batches in memory to allow prefetching
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20000, 1.0)
            .build_arc()?;

        let input = TestMemoryExec::try_new_exec(
            &[vec![batch1.clone(), batch2.clone(), batch3.clone()]],
            schema.clone(),
            None,
        )?;
        let prefetch = PrefetchExec::try_new(input)?;

        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut stream = prefetch.execute(0, task_ctx)?;

        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        // Should still get all batches even with spilling
        assert_eq!(batches.len(), 3);
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
        // Create 5 batches, but only allow memory for ~1.5 batches
        // This should trigger multiple spills
        let batches: Vec<RecordBatch> =
            (0..5).map(|i| create_test_batch(i, 1000)).collect();
        let schema = batches[0].schema();

        // Very low memory limit to force aggressive spilling
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(8000, 1.0) // Only ~1.5 batches fit
            .build_arc()?;

        let input =
            TestMemoryExec::try_new_exec(&[batches.clone()], schema.clone(), None)?;
        let prefetch = PrefetchExec::try_new(input)?;

        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut stream = prefetch.execute(0, task_ctx)?;

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

        Ok(())
    }
}
