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

//! [`ParquetMorsel`] implementation for sub-file parallelism in Parquet scans.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_stream::FileMorsel;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge};
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, RowSelection, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::{ParquetFileMetrics, ParquetFileReaderFactory};

/// Minimum number of rows for a morsel to be created independently.
/// Row groups smaller than this will be packed together.
pub(crate) const DEFAULT_MIN_MORSEL_ROWS: usize = 100_000;

/// A unit of work representing one or more Parquet row groups that can be
/// independently read and decoded.
///
/// Each `ParquetMorsel` is self-contained with all the information needed to
/// create a reader, apply projections and filters, and produce a stream of
/// [`RecordBatch`]es.
pub struct ParquetMorsel {
    /// Factory to create AsyncFileReader instances
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// The file being read
    partitioned_file: PartitionedFile,
    /// Row group indices this morsel is responsible for
    row_group_indexes: Vec<usize>,
    /// Optional row selection (from page pruning)
    row_selection: Option<RowSelection>,
    /// Projection expressions to apply
    projection: ProjectionExprs,
    /// Target number of rows per output RecordBatch
    batch_size: usize,
    /// Schema of the output
    output_schema: SchemaRef,
    /// The full file metadata (shared across morsels from the same file)
    metadata: ArrowReaderMetadata,
    /// Execution partition index
    partition_index: usize,
    /// Metrics for reporting
    metrics: ExecutionPlanMetricsSet,
    /// Metadata size hint for the reader
    metadata_size_hint: Option<usize>,
    /// Force row selection policy
    force_filter_selections: bool,
    /// Optional limit on rows to read
    limit: Option<usize>,
    /// Maximum predicate cache size
    max_predicate_cache_size: Option<usize>,
    /// Estimated total rows in this morsel
    est_rows: Option<usize>,
    /// Estimated total bytes in this morsel
    est_bytes: Option<usize>,
}

impl ParquetMorsel {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        partitioned_file: PartitionedFile,
        row_group_indexes: Vec<usize>,
        row_selection: Option<RowSelection>,
        projection: ProjectionExprs,
        batch_size: usize,
        output_schema: SchemaRef,
        metadata: ArrowReaderMetadata,
        partition_index: usize,
        metrics: ExecutionPlanMetricsSet,
        metadata_size_hint: Option<usize>,
        force_filter_selections: bool,
        limit: Option<usize>,
        max_predicate_cache_size: Option<usize>,
        est_rows: Option<usize>,
        est_bytes: Option<usize>,
    ) -> Self {
        Self {
            reader_factory,
            partitioned_file,
            row_group_indexes,
            row_selection,
            projection,
            batch_size,
            output_schema,
            metadata,
            partition_index,
            metrics,
            metadata_size_hint,
            force_filter_selections,
            limit,
            max_predicate_cache_size,
            est_rows,
            est_bytes,
        }
    }
}

impl FileMorsel for ParquetMorsel {
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        let file_name = self.partitioned_file.object_meta.location.to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        // Create a new reader for this morsel
        let metadata_size_hint = self
            .partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);
        let async_file_reader: Box<dyn AsyncFileReader> = self
            .reader_factory
            .create_reader(
                self.partition_index,
                self.partitioned_file.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        // Build the stream from the shared metadata
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            async_file_reader,
            self.metadata.clone(),
        );

        if self.force_filter_selections {
            builder =
                builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }

        // Apply row groups and row selection
        builder = builder.with_row_groups(self.row_group_indexes.clone());
        if let Some(ref row_selection) = self.row_selection {
            builder = builder.with_row_selection(row_selection.clone());
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        if let Some(max_predicate_cache_size) = self.max_predicate_cache_size {
            builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
        }

        let arrow_reader_metrics = ArrowReaderMetrics::enabled();

        let indices = self.projection.column_indices();
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

        let stream = builder
            .with_projection(mask)
            .with_batch_size(self.batch_size)
            .with_metrics(arrow_reader_metrics.clone())
            .build()?;

        let predicate_cache_inner_records =
            file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records = file_metrics.predicate_cache_records.clone();

        let stream_schema: SchemaRef = stream.schema().clone();
        let output_schema = Arc::clone(&self.output_schema);
        let replace_schema = stream_schema != output_schema;

        // Rebase column indices to match the narrowed stream schema
        let projection = self
            .projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = projection.make_projector(&stream_schema)?;

        let adapted = stream
            .map_err(|e| DataFusionError::from(e))
            .map(move |batch_result: Result<RecordBatch>| {
                batch_result.and_then(|b| {
                    copy_arrow_reader_metrics(
                        &arrow_reader_metrics,
                        &predicate_cache_inner_records,
                        &predicate_cache_records,
                    );
                    let projected = projector.project_batch(&b)?;
                    if replace_schema {
                        let (_schema, arrays, num_rows) = projected.into_parts();
                        let options =
                            RecordBatchOptions::new().with_row_count(Some(num_rows));
                        RecordBatch::try_new_with_options(
                            Arc::clone(&output_schema),
                            arrays,
                            &options,
                        )
                        .map_err(Into::into)
                    } else {
                        Ok(projected)
                    }
                })
            });

        Ok(Box::pin(ParquetMorselStream {
            stream: Box::pin(adapted),
            schema: Arc::clone(&self.output_schema),
        }))
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.est_rows
    }

    fn estimated_bytes(&self) -> Option<usize> {
        self.est_bytes
    }
}

/// Copies metrics from ArrowReaderMetrics to DataFusion metrics
fn copy_arrow_reader_metrics(
    arrow_reader_metrics: &ArrowReaderMetrics,
    predicate_cache_inner_records: &Gauge,
    predicate_cache_records: &Gauge,
) {
    if let Some(v) = arrow_reader_metrics.records_read_from_inner() {
        predicate_cache_inner_records.set(v);
    }
    if let Some(v) = arrow_reader_metrics.records_read_from_cache() {
        predicate_cache_records.set(v);
    }
}

/// Stream adapter for `ParquetMorsel::execute()`
struct ParquetMorselStream {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    schema: SchemaRef,
}

impl Stream for ParquetMorselStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ParquetMorselStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Split row group indices into morsel groups, packing small row groups together.
///
/// Each morsel will contain enough row groups to have at least `min_rows` rows,
/// except possibly the last morsel.
pub(crate) fn split_row_groups_into_morsels(
    row_group_indexes: &[usize],
    row_group_metadata: &[parquet::file::metadata::RowGroupMetaData],
    min_rows: usize,
) -> Vec<Vec<usize>> {
    if row_group_indexes.is_empty() {
        return vec![];
    }

    let mut morsels: Vec<Vec<usize>> = Vec::new();
    let mut current_morsel: Vec<usize> = Vec::new();
    let mut current_rows: usize = 0;

    for &rg_idx in row_group_indexes {
        let rg_rows = row_group_metadata[rg_idx].num_rows() as usize;
        current_morsel.push(rg_idx);
        current_rows += rg_rows;

        if current_rows >= min_rows {
            morsels.push(std::mem::take(&mut current_morsel));
            current_rows = 0;
        }
    }

    if !current_morsel.is_empty() {
        morsels.push(current_morsel);
    }

    morsels
}
