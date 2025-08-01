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

//! Partial Sort deals with input data that partially
//! satisfies the required sort order. Such an input data can be
//! partitioned into segments where each segment already has the
//! required information for lexicographic sorting so sorting
//! can be done without loading the entire dataset.
//!
//! Consider a sort plan having an input with ordering `a ASC, b ASC`
//!
//! ```text
//! +---+---+---+
//! | a | b | d |
//! +---+---+---+
//! | 0 | 0 | 3 |
//! | 0 | 0 | 2 |
//! | 0 | 1 | 1 |
//! | 0 | 2 | 0 |
//! +---+---+---+
//!```
//!
//! and required ordering for the plan is `a ASC, b ASC, d ASC`.
//! The first 3 rows(segment) can be sorted as the segment already
//! has the required information for the sort, but the last row
//! requires further information as the input can continue with a
//! batch with a starting row where a and b does not change as below
//!
//! ```text
//! +---+---+---+
//! | a | b | d |
//! +---+---+---+
//! | 0 | 2 | 4 |
//! +---+---+---+
//!```
//!
//! The plan concats incoming data with such last rows of previous input
//! and continues partial sorting of the segments.

use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::sorts::sort::sort_batch;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, TaskContext};
use datafusion_physical_expr::LexOrdering;

use futures::{ready, Stream, StreamExt};
use log::trace;

/// Partial Sort execution plan.
#[derive(Debug, Clone)]
pub struct PartialSortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: LexOrdering,
    /// Length of continuous matching columns of input that satisfy
    /// the required ordering for the sort
    common_prefix_length: usize,
    /// Containing all metrics set created during sort
    metrics_set: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan. If false, the input partitions
    /// will be sorted and merged into a single output partition.
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl PartialSortExec {
    /// Create a new partial sort execution plan
    pub fn new(
        expr: LexOrdering,
        input: Arc<dyn ExecutionPlan>,
        common_prefix_length: usize,
    ) -> Self {
        debug_assert!(common_prefix_length > 0);
        let preserve_partitioning = false;
        let cache = Self::compute_properties(&input, expr.clone(), preserve_partitioning)
            .unwrap();
        Self {
            input,
            expr,
            common_prefix_length,
            metrics_set: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
            fetch: None,
            cache,
        }
    }

    /// Whether this `PartialSortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this partial sort exec
    ///
    /// If `preserve_partitioning` is true, sorts each partition
    /// individually, producing one sorted stream for each input partition.
    ///
    /// If `preserve_partitioning` is false, sorts and merges all
    /// input partitions producing a single, sorted partition.
    pub fn with_preserve_partitioning(mut self, preserve_partitioning: bool) -> Self {
        self.preserve_partitioning = preserve_partitioning;
        self.cache = self
            .cache
            .with_partitioning(Self::output_partitioning_helper(
                &self.input,
                self.preserve_partitioning,
            ));
        self
    }

    /// Modify how many rows to include in the result
    ///
    /// If None, then all rows will be returned, in sorted order.
    /// If Some, then only the top `fetch` rows will be returned.
    /// This can reduce the memory pressure required by the sort
    /// operation since rows that are not going to be included
    /// can be dropped.
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }

    /// If `Some(fetch)`, limits output to only the first "fetch" items
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Common prefix length
    pub fn common_prefix_length(&self) -> usize {
        self.common_prefix_length
    }

    fn output_partitioning_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Partitioning {
        // Get output partitioning:
        if preserve_partitioning {
            input.output_partitioning().clone()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        preserve_partitioning: bool,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties; i.e. reset the ordering equivalence
        // class with the new ordering:
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(sort_exprs)?;

        // Get output partitioning:
        let output_partitioning =
            Self::output_partitioning_helper(input, preserve_partitioning);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for PartialSortExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let common_prefix_length = self.common_prefix_length;
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "PartialSortExec: TopK(fetch={fetch}), expr=[{}], common_prefix_length=[{common_prefix_length}]", self.expr)
                    }
                    None => write!(f, "PartialSortExec: expr=[{}], common_prefix_length=[{common_prefix_length}]", self.expr),
                }
            }
            DisplayFormatType::TreeRender => match self.fetch {
                Some(fetch) => {
                    writeln!(f, "{}", self.expr)?;
                    writeln!(f, "limit={fetch}")
                }
                None => {
                    writeln!(f, "{}", self.expr)
                }
            },
        }
    }
}

impl ExecutionPlan for PartialSortExec {
    fn name(&self) -> &'static str {
        "PartialSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.preserve_partitioning {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            vec![Distribution::SinglePartition]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_partial_sort = PartialSortExec::new(
            self.expr.clone(),
            Arc::clone(&children[0]),
            self.common_prefix_length,
        )
        .with_fetch(self.fetch)
        .with_preserve_partitioning(self.preserve_partitioning);

        Ok(Arc::new(new_partial_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start PartialSortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let input = self.input.execute(partition, Arc::clone(&context))?;

        trace!("End PartialSortExec's input.execute for partition: {partition}");

        // Make sure common prefix length is larger than 0
        // Otherwise, we should use SortExec.
        debug_assert!(self.common_prefix_length > 0);

        Ok(Box::pin(PartialSortStream {
            input,
            expr: self.expr.clone(),
            common_prefix_length: self.common_prefix_length,
            in_mem_batch: RecordBatch::new_empty(Arc::clone(&self.schema())),
            fetch: self.fetch,
            is_closed: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics_set, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }
}

struct PartialSortStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// Sort expressions
    expr: LexOrdering,
    /// Length of prefix common to input ordering and required ordering of plan
    /// should be more than 0 otherwise PartialSort is not applicable
    common_prefix_length: usize,
    /// Used as a buffer for part of the input not ready for sort
    in_mem_batch: RecordBatch,
    /// Fetch top N results
    fetch: Option<usize>,
    /// Whether the stream has finished returning all of its data or not
    is_closed: bool,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
}

impl Stream for PartialSortStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

impl RecordBatchStream for PartialSortStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl PartialSortStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.is_closed {
            return Poll::Ready(None);
        }
        loop {
            // Check if we've already reached the fetch limit
            if self.fetch == Some(0) {
                self.is_closed = true;
                return Poll::Ready(None);
            }

            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    // Merge new batch into in_mem_batch
                    self.in_mem_batch = concat_batches(
                        &self.schema(),
                        &[self.in_mem_batch.clone(), batch],
                    )?;

                    // Check if we have a slice point, otherwise keep accumulating in `self.in_mem_batch`.
                    if let Some(slice_point) = self
                        .get_slice_point(self.common_prefix_length, &self.in_mem_batch)?
                    {
                        let sorted = self.in_mem_batch.slice(0, slice_point);
                        self.in_mem_batch = self.in_mem_batch.slice(
                            slice_point,
                            self.in_mem_batch.num_rows() - slice_point,
                        );
                        let sorted_batch = sort_batch(&sorted, &self.expr, self.fetch)?;
                        if let Some(fetch) = self.fetch.as_mut() {
                            *fetch -= sorted_batch.num_rows();
                        }

                        if sorted_batch.num_rows() > 0 {
                            return Poll::Ready(Some(Ok(sorted_batch)));
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    self.is_closed = true;
                    // Once input is consumed, sort the rest of the inserted batches
                    let remaining_batch = self.sort_in_mem_batch()?;
                    return if remaining_batch.num_rows() > 0 {
                        Poll::Ready(Some(Ok(remaining_batch)))
                    } else {
                        Poll::Ready(None)
                    };
                }
            };
        }
    }

    /// Returns a sorted RecordBatch from in_mem_batches and clears in_mem_batches
    ///
    /// If fetch is specified for PartialSortStream `sort_in_mem_batch` will limit
    /// the last RecordBatch returned and will mark the stream as closed
    fn sort_in_mem_batch(self: &mut Pin<&mut Self>) -> Result<RecordBatch> {
        let input_batch = self.in_mem_batch.clone();
        self.in_mem_batch = RecordBatch::new_empty(self.schema());
        let result = sort_batch(&input_batch, &self.expr, self.fetch)?;
        if let Some(remaining_fetch) = self.fetch {
            // remaining_fetch - result.num_rows() is always be >= 0
            // because result length of sort_batch with limit cannot be
            // more than the requested limit
            self.fetch = Some(remaining_fetch - result.num_rows());
            if remaining_fetch == result.num_rows() {
                self.is_closed = true;
            }
        }
        Ok(result)
    }

    /// Return the end index of the second last partition if the batch
    /// can be partitioned based on its already sorted columns
    ///
    /// Return None if the batch cannot be partitioned, which means the
    /// batch does not have the information for a safe sort
    fn get_slice_point(
        &self,
        common_prefix_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<usize>> {
        let common_prefix_sort_keys = (0..common_prefix_len)
            .map(|idx| self.expr[idx].evaluate_to_sort_column(batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &common_prefix_sort_keys)?;
        // If partition points are [0..100], [100..200], [200..300]
        // we should return 200, which is the safest and furthest partition boundary
        // Please note that we shouldn't return 300 (which is number of rows in the batch),
        // because this boundary may change with new data.
        if partition_points.len() >= 2 {
            Ok(Some(partition_points[partition_points.len() - 2].end))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_common::test_util::batches_to_string;
    use futures::FutureExt;
    use insta::allow_duplicates;
    use insta::assert_snapshot;
    use itertools::Itertools;

    use crate::collect;
    use crate::expressions::col;
    use crate::expressions::PhysicalSortExpr;
    use crate::sorts::sort::SortExec;
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::TestMemoryExec;

    use super::*;

    #[tokio::test]
    async fn test_partial_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 0, 1, 1, 1]),
            ("b", &vec![1, 1, 2, 2, 3, 3]),
            ("c", &vec![1, 0, 5, 4, 3, 2]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ]
            .into(),
            Arc::clone(&source),
            2,
        ));

        let result = collect(partial_sort_exec, Arc::clone(&task_ctx)).await?;

        assert_eq!(2, result.len());
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r#"
                +---+---+---+
                | a | b | c |
                +---+---+---+
                | 0 | 1 | 0 |
                | 0 | 1 | 1 |
                | 0 | 2 | 5 |
                | 1 | 2 | 4 |
                | 1 | 3 | 2 |
                | 1 | 3 | 3 |
                +---+---+---+
                "#);
        }
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 1, 1, 1]),
            ("b", &vec![1, 2, 2, 3, 3]),
            ("c", &vec![4, 3, 2, 1, 0]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        for common_prefix_length in [1, 2] {
            let partial_sort_exec = Arc::new(
                PartialSortExec::new(
                    [
                        PhysicalSortExpr {
                            expr: col("a", &schema)?,
                            options: option_asc,
                        },
                        PhysicalSortExpr {
                            expr: col("b", &schema)?,
                            options: option_asc,
                        },
                        PhysicalSortExpr {
                            expr: col("c", &schema)?,
                            options: option_asc,
                        },
                    ]
                    .into(),
                    Arc::clone(&source),
                    common_prefix_length,
                )
                .with_fetch(Some(4)),
            );

            let result = collect(partial_sort_exec, Arc::clone(&task_ctx)).await?;

            assert_eq!(2, result.len());
            allow_duplicates! {
                assert_snapshot!(batches_to_string(&result), @r#"
                    +---+---+---+
                    | a | b | c |
                    +---+---+---+
                    | 0 | 1 | 4 |
                    | 0 | 2 | 3 |
                    | 1 | 2 | 2 |
                    | 1 | 3 | 0 |
                    +---+---+---+
                    "#);
            }
            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort2() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source_tables = [
            test::build_table_scan_i32(
                ("a", &vec![0, 0, 0, 0, 1, 1, 1, 1]),
                ("b", &vec![1, 1, 3, 3, 4, 4, 2, 2]),
                ("c", &vec![7, 6, 5, 4, 3, 2, 1, 0]),
            ),
            test::build_table_scan_i32(
                ("a", &vec![0, 0, 0, 0, 1, 1, 1, 1]),
                ("b", &vec![1, 1, 3, 3, 2, 2, 4, 4]),
                ("c", &vec![7, 6, 5, 4, 1, 0, 3, 2]),
            ),
        ];
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        for (common_prefix_length, source) in
            [(1, &source_tables[0]), (2, &source_tables[1])]
        {
            let partial_sort_exec = Arc::new(PartialSortExec::new(
                [
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ]
                .into(),
                Arc::clone(source),
                common_prefix_length,
            ));

            let result = collect(partial_sort_exec, Arc::clone(&task_ctx)).await?;
            assert_eq!(2, result.len());
            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
            allow_duplicates! {
                assert_snapshot!(batches_to_string(&result), @r#"
                    +---+---+---+
                    | a | b | c |
                    +---+---+---+
                    | 0 | 1 | 6 |
                    | 0 | 1 | 7 |
                    | 0 | 3 | 4 |
                    | 0 | 3 | 5 |
                    | 1 | 2 | 0 |
                    | 1 | 2 | 1 |
                    | 1 | 4 | 2 |
                    | 1 | 4 | 3 |
                    +---+---+---+
                    "#);
            }
        }
        Ok(())
    }

    fn prepare_partitioned_input() -> Arc<dyn ExecutionPlan> {
        let batch1 = test::build_table_i32(
            ("a", &vec![1; 100]),
            ("b", &(0..100).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let batch2 = test::build_table_i32(
            ("a", &[&vec![1; 25][..], &vec![2; 75][..]].concat()),
            ("b", &(100..200).rev().collect()),
            ("c", &(0..100).collect()),
        );
        let batch3 = test::build_table_i32(
            ("a", &[&vec![3; 50][..], &vec![4; 50][..]].concat()),
            ("b", &(150..250).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let batch4 = test::build_table_i32(
            ("a", &vec![4; 100]),
            ("b", &(50..150).rev().collect()),
            ("c", &(0..100).rev().collect()),
        );
        let schema = batch1.schema();

        TestMemoryExec::try_new_exec(
            &[vec![batch1, batch2, batch3, batch4]],
            Arc::clone(&schema),
            None,
        )
        .unwrap() as Arc<dyn ExecutionPlan>
    }

    #[tokio::test]
    async fn test_partitioned_input_partial_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let mem_exec = prepare_partitioned_input();
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let schema = mem_exec.schema();
        let partial_sort_exec = PartialSortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_desc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ]
            .into(),
            Arc::clone(&mem_exec),
            1,
        );
        let sort_exec = Arc::new(SortExec::new(
            partial_sort_exec.expr.clone(),
            Arc::clone(&partial_sort_exec.input),
        ));
        let result = collect(Arc::new(partial_sort_exec), Arc::clone(&task_ctx)).await?;
        assert_eq!(
            result.iter().map(|r| r.num_rows()).collect_vec(),
            [125, 125, 150]
        );

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );
        let partial_sort_result = concat_batches(&schema, &result).unwrap();
        let sort_result = collect(sort_exec, Arc::clone(&task_ctx)).await?;
        assert_eq!(sort_result[0], partial_sort_result);

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_input_partial_sort_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let mem_exec = prepare_partitioned_input();
        let schema = mem_exec.schema();
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        for (fetch_size, expected_batch_num_rows) in [
            (Some(50), vec![50]),
            (Some(120), vec![120]),
            (Some(150), vec![125, 25]),
            (Some(250), vec![125, 125]),
        ] {
            let partial_sort_exec = PartialSortExec::new(
                [
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_desc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ]
                .into(),
                Arc::clone(&mem_exec),
                1,
            )
            .with_fetch(fetch_size);

            let sort_exec = Arc::new(
                SortExec::new(
                    partial_sort_exec.expr.clone(),
                    Arc::clone(&partial_sort_exec.input),
                )
                .with_fetch(fetch_size),
            );
            let result =
                collect(Arc::new(partial_sort_exec), Arc::clone(&task_ctx)).await?;
            assert_eq!(
                result.iter().map(|r| r.num_rows()).collect_vec(),
                expected_batch_num_rows
            );

            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
            let partial_sort_result = concat_batches(&schema, &result)?;
            let sort_result = collect(sort_exec, Arc::clone(&task_ctx)).await?;
            assert_eq!(sort_result[0], partial_sort_result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort_no_empty_batches() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let mem_exec = prepare_partitioned_input();
        let schema = mem_exec.schema();
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let fetch_size = Some(250);
        let partial_sort_exec = PartialSortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ]
            .into(),
            Arc::clone(&mem_exec),
            1,
        )
        .with_fetch(fetch_size);

        let result = collect(Arc::new(partial_sort_exec), Arc::clone(&task_ctx)).await?;
        for rb in result {
            assert!(rb.num_rows() > 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let field_metadata: HashMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(field_metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![1, 1, 2].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![data])?;
        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            [PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            input,
            1,
        ));

        let result: Vec<RecordBatch> = collect(partial_sort_exec, task_ctx).await?;
        let expected_batch = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(
                    vec![1, 1].into_iter().map(Some).collect::<UInt64Array>(),
                )],
            )?,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(
                    vec![2].into_iter().map(Some).collect::<UInt64Array>(),
                )],
            )?,
        ];

        // Data is correct
        assert_eq!(&expected_batch, &result);

        // explicitly ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Float64, true),
        ]));
        let option_asc = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // define data.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(20.0_f64),
                    Some(20.0_f64),
                    Some(40.0_f64),
                    Some(40.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    Some(100.0_f64),
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_desc,
                },
            ]
            .into(),
            TestMemoryExec::try_new_exec(&[vec![batch]], schema, None)?,
            2,
        ));

        assert_eq!(
            DataType::Float32,
            *partial_sort_exec.schema().field(0).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(1).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(2).data_type()
        );

        let result: Vec<RecordBatch> = collect(
            Arc::clone(&partial_sort_exec) as Arc<dyn ExecutionPlan>,
            task_ctx,
        )
        .await?;
        assert_snapshot!(batches_to_string(&result), @r#"
            +-----+------+-------+
            | a   | b    | c     |
            +-----+------+-------+
            | 1.0 | 20.0 | 20.0  |
            | 1.0 | 20.0 | 10.0  |
            | 1.0 | 40.0 | 10.0  |
            | 2.0 | 40.0 | 100.0 |
            | 2.0 | NaN  | NaN   |
            | 3.0 |      |       |
            | 3.0 |      | 100.0 |
            | 3.0 | NaN  | NaN   |
            +-----+------+-------+
            "#);
        assert_eq!(result.len(), 2);
        let metrics = partial_sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());
        assert_eq!(DataType::Float64, *columns[2].data_type());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(PartialSortExec::new(
            [PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            blocking_exec,
            1,
        ));

        let fut = collect(sort_exec, Arc::clone(&task_ctx));
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort_with_homogeneous_batches() -> Result<()> {
        // Test case for the bug where batches with homogeneous sort keys
        // (e.g., [1,1,1], [2,2,2]) would not be properly detected as having
        // slice points between batches.
        let task_ctx = Arc::new(TaskContext::default());

        // Create batches where each batch has homogeneous values for sort keys
        let batch1 = test::build_table_i32(
            ("a", &vec![1; 3]),
            ("b", &vec![1; 3]),
            ("c", &vec![3, 2, 1]),
        );
        let batch2 = test::build_table_i32(
            ("a", &vec![2; 3]),
            ("b", &vec![2; 3]),
            ("c", &vec![4, 6, 4]),
        );
        let batch3 = test::build_table_i32(
            ("a", &vec![3; 3]),
            ("b", &vec![3; 3]),
            ("c", &vec![9, 7, 8]),
        );

        let schema = batch1.schema();
        let mem_exec = TestMemoryExec::try_new_exec(
            &[vec![batch1, batch2, batch3]],
            Arc::clone(&schema),
            None,
        )?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        // Partial sort with common prefix of 2 (sorting by a, b, c)
        let partial_sort_exec = Arc::new(PartialSortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ]
            .into(),
            mem_exec,
            2,
        ));

        let result = collect(partial_sort_exec, Arc::clone(&task_ctx)).await?;

        assert_eq!(result.len(), 3,);

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&result), @r#"
                +---+---+---+
                | a | b | c |
                +---+---+---+
                | 1 | 1 | 1 |
                | 1 | 1 | 2 |
                | 1 | 1 | 3 |
                | 2 | 2 | 4 |
                | 2 | 2 | 4 |
                | 2 | 2 | 6 |
                | 3 | 3 | 7 |
                | 3 | 3 | 8 |
                | 3 | 3 | 9 |
                +---+---+---+
                "#);
        }

        assert_eq!(task_ctx.runtime_env().memory_pool.reserved(), 0,);
        Ok(())
    }
}
