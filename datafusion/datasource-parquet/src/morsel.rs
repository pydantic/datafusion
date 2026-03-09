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
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_stream::FileMorsel;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge};
use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, RowSelection, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::{ParquetFileMetrics, ParquetFileReaderFactory, row_filter};

/// Default target projected bytes per morsel (1 MB).
///
/// Small row groups are packed together until their cumulative projected
/// compressed size reaches this target. Each row group is the minimum
/// unit — row groups are never split into sub-morsels.
pub(crate) const DEFAULT_TARGET_MORSEL_BYTES: usize = 1_000_000;

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
    /// Force row selection policy
    force_filter_selections: bool,
    /// Optional limit on rows to read
    limit: Option<usize>,
    /// Maximum predicate cache size
    max_predicate_cache_size: Option<usize>,
    /// Predicate for row-level filter pushdown
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Physical file schema (needed for building row filters)
    physical_file_schema: SchemaRef,
    /// Whether filter pushdown is enabled
    pushdown_filters: bool,
    /// Whether to reorder filter predicates for efficiency
    reorder_filters: bool,
    /// Estimated total rows in this morsel
    est_rows: Option<usize>,
    /// Estimated total bytes in this morsel
    est_bytes: Option<usize>,
}

impl ParquetMorsel {
    #[expect(clippy::too_many_arguments)]
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
        force_filter_selections: bool,
        limit: Option<usize>,
        max_predicate_cache_size: Option<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        physical_file_schema: SchemaRef,
        pushdown_filters: bool,
        reorder_filters: bool,
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
            force_filter_selections,
            limit,
            max_predicate_cache_size,
            predicate,
            physical_file_schema,
            pushdown_filters,
            reorder_filters,
            est_rows,
            est_bytes,
        }
    }
}

impl FileMorsel for ParquetMorsel {
    fn execute(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let file_name = self.partitioned_file.object_meta.location.to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        // Create a new reader for this morsel
        // Pass None for metadata_size_hint since metadata is already loaded
        // and passed via new_with_metadata — the hint is irrelevant here.
        let metadata_size_hint = None;
        let async_file_reader: Box<dyn AsyncFileReader> =
            self.reader_factory.create_reader(
                self.partition_index,
                self.partitioned_file,
                metadata_size_hint,
                &self.metrics,
            )?;

        // Build the stream from the shared metadata
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            async_file_reader,
            self.metadata,
        );

        if self.force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }

        // Apply row groups and row selection
        builder = builder.with_row_groups(self.row_group_indexes);
        if let Some(row_selection) = self.row_selection {
            builder = builder.with_row_selection(row_selection);
        }

        // Apply row filter (predicate pushdown into parquet scan)
        if let Some(predicate) = self
            .pushdown_filters
            .then_some(&self.predicate)
            .and_then(|p| p.as_ref())
        {
            let row_filter = row_filter::build_row_filter(
                predicate,
                &self.physical_file_schema,
                builder.metadata(),
                self.reorder_filters,
                &file_metrics,
            );
            match row_filter {
                Ok(Some(filter)) => {
                    builder = builder.with_row_filter(filter);
                }
                Ok(None) => {}
                Err(e) => {
                    debug!("Ignoring error building row filter for '{predicate:?}': {e}");
                }
            }
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

        let stream_schema: SchemaRef = Arc::clone(stream.schema());
        let output_schema = Arc::clone(&self.output_schema);
        let replace_schema = stream_schema != output_schema;

        // Rebase column indices to match the narrowed stream schema
        let projection = self
            .projection
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = projection.make_projector(&stream_schema)?;

        let adapted = stream.map_err(DataFusionError::from).map(
            move |batch_result: Result<RecordBatch>| {
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
            },
        );

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

/// A plan for a single morsel: which row groups to read and how many rows.
pub(crate) struct MorselPlan {
    pub row_group_indexes: Vec<usize>,
    pub row_selection: Option<RowSelection>,
    pub est_rows: usize,
    pub est_bytes: usize,
}

/// Compute the projected compressed size for a row group.
///
/// Sums `compressed_size()` of only the projected columns. If `projected_columns`
/// is empty, falls back to the row group's total `compressed_size()`.
fn projected_rg_bytes(
    rg_meta: &parquet::file::metadata::RowGroupMetaData,
    projected_columns: &[usize],
) -> usize {
    if projected_columns.is_empty() {
        return rg_meta.compressed_size() as usize;
    }
    projected_columns
        .iter()
        .filter_map(|&col_idx| {
            // Column indices from projection may exceed the number of columns
            // in the parquet file (e.g. partition columns). Skip those.
            if col_idx < rg_meta.num_columns() {
                Some(rg_meta.column(col_idx).compressed_size() as usize)
            } else {
                None
            }
        })
        .sum()
}

/// Group row group indices into morsels by packing small row groups together.
///
/// Uses projected compressed byte sizes from Parquet column metadata.
/// Row groups are packed together until their cumulative projected bytes
/// reach `target_bytes`. Each row group is the minimum unit — row groups
/// are never split into sub-morsels.
pub(crate) fn split_row_groups_into_morsels(
    row_group_indexes: &[usize],
    row_group_metadata: &[parquet::file::metadata::RowGroupMetaData],
    row_selection: Option<&RowSelection>,
    projected_columns: &[usize],
    target_bytes: usize,
) -> Vec<MorselPlan> {
    if row_group_indexes.is_empty() {
        return vec![];
    }

    let mut morsels: Vec<MorselPlan> = Vec::new();
    let mut current_rg_indexes: Vec<usize> = Vec::new();
    let mut current_rows: usize = 0;
    let mut current_bytes: usize = 0;

    for &rg_idx in row_group_indexes {
        let rg_meta = &row_group_metadata[rg_idx];
        let rg_rows = rg_meta.num_rows() as usize;
        let rg_proj_bytes = projected_rg_bytes(rg_meta, projected_columns);

        current_rg_indexes.push(rg_idx);
        current_rows += rg_rows;
        current_bytes += rg_proj_bytes;

        if current_bytes >= target_bytes {
            morsels.push(MorselPlan {
                row_group_indexes: std::mem::take(&mut current_rg_indexes),
                row_selection: row_selection.cloned(),
                est_rows: current_rows,
                est_bytes: current_bytes,
            });
            current_rows = 0;
            current_bytes = 0;
        }
    }

    if !current_rg_indexes.is_empty() {
        morsels.push(MorselPlan {
            row_group_indexes: current_rg_indexes,
            row_selection: row_selection.cloned(),
            est_rows: current_rows,
            est_bytes: current_bytes,
        });
    }

    morsels
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};
    use std::sync::Arc;

    /// Create test RG metadata with a single INT32 column.
    fn make_rg_metadata_with_bytes(
        num_rows: i64,
        compressed_bytes: i64,
    ) -> RowGroupMetaData {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("col", PhysicalType::INT32)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));
        let column = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_num_values(num_rows)
            .set_total_compressed_size(compressed_bytes)
            .set_total_uncompressed_size(compressed_bytes)
            .build()
            .unwrap();
        RowGroupMetaData::builder(schema_descr)
            .set_num_rows(num_rows)
            .set_total_byte_size(compressed_bytes)
            .set_column_metadata(vec![column])
            .build()
            .unwrap()
    }

    /// Create test RG metadata with two columns.
    fn make_rg_metadata_2col(
        num_rows: i64,
        col0_bytes: i64,
        col1_bytes: i64,
    ) -> RowGroupMetaData {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        Type::primitive_type_builder("col0", PhysicalType::INT32)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("col1", PhysicalType::INT64)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));
        let col0 = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_num_values(num_rows)
            .set_total_compressed_size(col0_bytes)
            .set_total_uncompressed_size(col0_bytes)
            .build()
            .unwrap();
        let col1 = ColumnChunkMetaData::builder(schema_descr.column(1))
            .set_num_values(num_rows)
            .set_total_compressed_size(col1_bytes)
            .set_total_uncompressed_size(col1_bytes)
            .build()
            .unwrap();
        RowGroupMetaData::builder(schema_descr)
            .set_num_rows(num_rows)
            .set_total_byte_size(col0_bytes + col1_bytes)
            .set_column_metadata(vec![col0, col1])
            .build()
            .unwrap()
    }

    const MB: usize = 1_000_000;

    #[test]
    fn test_small_rgs_pack_together() {
        // target_bytes=15MB, so RGs pack until cumulative bytes >= 15MB
        let metadata = vec![
            make_rg_metadata_with_bytes(30_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(40_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(50_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(20_000, 5 * MB as i64),
        ];
        let indexes: Vec<usize> = (0..4).collect();

        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[0], 15 * MB);

        // 5MB + 5MB + 5MB = 15MB >= 15MB target → first morsel
        // 5MB → second morsel (remainder)
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].row_group_indexes, vec![0, 1, 2]);
        assert_eq!(morsels[0].est_rows, 120_000);
        assert_eq!(morsels[1].row_group_indexes, vec![3]);
        assert_eq!(morsels[1].est_rows, 20_000);
        assert!(morsels[0].row_selection.is_none());
        assert!(morsels[1].row_selection.is_none());
    }

    #[test]
    fn test_large_rg_becomes_single_morsel() {
        // 1M rows, 150MB compressed — large RG should NOT be split
        let metadata = vec![make_rg_metadata_with_bytes(1_000_000, 150 * MB as i64)];
        let indexes = vec![0];

        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[0], 15 * MB);

        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
        assert_eq!(morsels[0].est_rows, 1_000_000);
        assert!(morsels[0].row_selection.is_none());
    }

    #[test]
    fn test_each_rg_own_morsel_when_above_target() {
        // Each RG is 20MB > 15MB target → each becomes its own morsel
        let metadata = vec![
            make_rg_metadata_with_bytes(100_000, 20 * MB as i64),
            make_rg_metadata_with_bytes(100_000, 20 * MB as i64),
            make_rg_metadata_with_bytes(100_000, 20 * MB as i64),
        ];
        let indexes = vec![0, 1, 2];

        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[0], 15 * MB);

        assert_eq!(morsels.len(), 3);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
        assert_eq!(morsels[1].row_group_indexes, vec![1]);
        assert_eq!(morsels[2].row_group_indexes, vec![2]);
    }

    #[test]
    fn test_mixed_small_and_large() {
        let metadata = vec![
            make_rg_metadata_with_bytes(50_000, 5 * MB as i64), // small
            make_rg_metadata_with_bytes(500_000, 75 * MB as i64), // large
            make_rg_metadata_with_bytes(30_000, 3 * MB as i64), // small
        ];
        let indexes = vec![0, 1, 2];

        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[0], 15 * MB);

        // RG 0 (5MB) + RG 1 (75MB) = 80MB >= 15MB → first morsel
        // RG 2 (3MB) → remainder morsel
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].row_group_indexes, vec![0, 1]);
        assert_eq!(morsels[0].est_rows, 550_000);
        assert_eq!(morsels[1].row_group_indexes, vec![2]);
        assert_eq!(morsels[1].est_rows, 30_000);
    }

    #[test]
    fn test_row_selection_passed_through() {
        // Row selection from page pruning should be passed through to morsels
        let metadata = vec![
            make_rg_metadata_with_bytes(100_000, 10 * MB as i64),
            make_rg_metadata_with_bytes(100_000, 10 * MB as i64),
        ];
        let indexes = vec![0, 1];

        let selection = RowSelection::from(vec![
            parquet::arrow::arrow_reader::RowSelector::skip(50_000),
            parquet::arrow::arrow_reader::RowSelector::select(150_000),
        ]);

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            Some(&selection),
            &[0],
            15 * MB,
        );

        // 10MB + 10MB = 20MB >= 15MB → single morsel with both RGs
        assert_eq!(morsels.len(), 1);
        assert!(morsels[0].row_selection.is_some());
    }

    #[test]
    fn test_empty_input() {
        let metadata = vec![];
        let morsels = split_row_groups_into_morsels(&[], &metadata, None, &[], 15 * MB);
        assert!(morsels.is_empty());
    }

    #[test]
    fn test_projection_affects_packing() {
        // 2-column RG: col0=5MB, col1=20MB.
        // Projecting col0 only: 5MB < 15MB → packs with next
        // Projecting col1 only: 20MB >= 15MB → own morsel
        let metadata = vec![
            make_rg_metadata_2col(100_000, 5 * MB as i64, 20 * MB as i64),
            make_rg_metadata_2col(100_000, 5 * MB as i64, 20 * MB as i64),
        ];
        let indexes = vec![0, 1];

        // Project col0 only (5MB each) → packed into 1 morsel
        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[0], 15 * MB);
        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_indexes, vec![0, 1]);

        // Project col1 only (20MB each) → each RG is its own morsel
        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[1], 15 * MB);
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
        assert_eq!(morsels[1].row_group_indexes, vec![1]);
    }

    #[test]
    fn test_empty_projection_uses_total_size() {
        // Empty projection should fall back to compressed_size()
        let metadata = vec![make_rg_metadata_with_bytes(200_000, 30 * MB as i64)];
        let indexes = vec![0];

        let morsels =
            split_row_groups_into_morsels(&indexes, &metadata, None, &[], 15 * MB);

        // 30MB >= 15MB → single morsel (no splitting, just one RG)
        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
    }
}
