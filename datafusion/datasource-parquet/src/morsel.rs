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

/// Minimum number of rows for a morsel to be created independently.
/// Row groups smaller than this will be packed together.
pub(crate) const DEFAULT_MIN_MORSEL_ROWS: usize = 100_000;

/// Default maximum projected bytes per morsel (15 MB).
/// Large row groups exceeding 2× this will be split into multiple morsels.
pub(crate) const DEFAULT_MAX_MORSEL_BYTES: usize = 15_000_000;

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
        metadata_size_hint: Option<usize>,
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
            metadata_size_hint,
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
        let metadata_size_hint = self
            .partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);
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

/// Split row group indices into morsel groups, packing small row groups together
/// and splitting large row groups into multiple morsels using RowSelection.
///
/// Uses projected compressed byte sizes from Parquet column metadata to decide
/// splitting. Row groups whose projected bytes exceed `2 * max_bytes` are split
/// into `ceil(bytes / max_bytes)` sub-morsels with evenly distributed rows.
/// Smaller row groups are packed together using `min_rows` as the packing threshold.
pub(crate) fn split_row_groups_into_morsels(
    row_group_indexes: &[usize],
    row_group_metadata: &[parquet::file::metadata::RowGroupMetaData],
    row_selection: Option<&RowSelection>,
    projected_columns: &[usize],
    min_rows: usize,
    max_bytes: usize,
) -> Vec<MorselPlan> {
    if row_group_indexes.is_empty() {
        return vec![];
    }

    let mut morsels: Vec<MorselPlan> = Vec::new();
    let mut current_rg_indexes: Vec<usize> = Vec::new();
    let mut current_rows: usize = 0;
    let mut current_bytes: usize = 0;

    // Build per-row-group selections from the overall row_selection if present.
    // The overall RowSelection covers all selected row groups sequentially.
    let per_rg_selections: Option<Vec<(usize, RowSelection)>> =
        row_selection.map(|sel| {
            split_row_selection_by_row_groups(sel, row_group_indexes, row_group_metadata)
        });

    for (i, &rg_idx) in row_group_indexes.iter().enumerate() {
        let rg_meta = &row_group_metadata[rg_idx];
        let rg_rows = rg_meta.num_rows() as usize;
        let rg_proj_bytes = projected_rg_bytes(rg_meta, projected_columns);

        if rg_proj_bytes >= 2 * max_bytes {
            // Flush any accumulated small RGs first
            if !current_rg_indexes.is_empty() {
                morsels.push(MorselPlan {
                    row_group_indexes: std::mem::take(&mut current_rg_indexes),
                    row_selection: row_selection.cloned(),
                    est_rows: current_rows,
                    est_bytes: current_bytes,
                });
                current_rows = 0;
                current_bytes = 0;
            }

            // Split large RG into sub-morsels based on byte budget
            let n = rg_proj_bytes.div_ceil(max_bytes);
            let chunk_rows = rg_rows / n;
            let remainder = rg_rows % n;
            let bytes_per_row = if rg_rows > 0 {
                rg_proj_bytes as f64 / rg_rows as f64
            } else {
                0.0
            };

            let rg_selection = per_rg_selections.as_ref().map(|sels| &sels[i].1);

            let mut offset = 0;
            for chunk_idx in 0..n {
                let rows = if chunk_idx < remainder {
                    chunk_rows + 1
                } else {
                    chunk_rows
                };

                let selection = match rg_selection {
                    Some(sel) => Some(sub_select_from_row_selection(sel, offset, rows)),
                    None => {
                        // Create a simple Skip/Select RowSelection
                        let mut selectors = Vec::new();
                        if offset > 0 {
                            selectors.push(
                                parquet::arrow::arrow_reader::RowSelector::skip(offset),
                            );
                        }
                        selectors.push(
                            parquet::arrow::arrow_reader::RowSelector::select(rows),
                        );
                        let remaining = rg_rows - offset - rows;
                        if remaining > 0 {
                            selectors.push(
                                parquet::arrow::arrow_reader::RowSelector::skip(
                                    remaining,
                                ),
                            );
                        }
                        Some(RowSelection::from(selectors))
                    }
                };

                let est_bytes = (rows as f64 * bytes_per_row) as usize;
                morsels.push(MorselPlan {
                    row_group_indexes: vec![rg_idx],
                    row_selection: selection,
                    est_rows: rows,
                    est_bytes,
                });

                offset += rows;
            }
        } else {
            // Small RG: pack into current morsel
            current_rg_indexes.push(rg_idx);
            current_rows += rg_rows;
            current_bytes += rg_proj_bytes;

            if current_rows >= min_rows {
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

/// Split an overall RowSelection into per-row-group RowSelections.
///
/// The overall RowSelection covers all selected row groups sequentially.
/// This function walks the selectors, consuming rows for each row group
/// based on its total row count, producing a RowSelection scoped to each RG.
fn split_row_selection_by_row_groups(
    row_selection: &RowSelection,
    row_group_indexes: &[usize],
    row_group_metadata: &[parquet::file::metadata::RowGroupMetaData],
) -> Vec<(usize, RowSelection)> {
    use parquet::arrow::arrow_reader::RowSelector;

    let selectors: Vec<RowSelector> = row_selection.iter().cloned().collect();
    let mut sel_idx = 0;
    let mut sel_offset = 0; // how many rows consumed in current selector
    let mut result = Vec::with_capacity(row_group_indexes.len());

    for &rg_idx in row_group_indexes {
        let rg_rows = row_group_metadata[rg_idx].num_rows() as usize;
        let mut remaining = rg_rows;
        let mut rg_selectors = Vec::new();

        while remaining > 0 && sel_idx < selectors.len() {
            let sel = &selectors[sel_idx];
            let available = sel.row_count - sel_offset;
            let take = remaining.min(available);

            if sel.skip {
                rg_selectors.push(RowSelector::skip(take));
            } else {
                rg_selectors.push(RowSelector::select(take));
            }

            sel_offset += take;
            remaining -= take;

            if sel_offset >= sel.row_count {
                sel_idx += 1;
                sel_offset = 0;
            }
        }

        result.push((rg_idx, RowSelection::from(rg_selectors)));
    }

    result
}

/// Extract a sub-selection from a per-RG RowSelection targeting `target_selected` rows
/// starting at the `skip_selected`-th selected row.
///
/// This walks the selectors, preserving skip/select structure, but only including
/// the portion that covers the target range of *selected* rows.
fn sub_select_from_row_selection(
    rg_selection: &RowSelection,
    skip_selected: usize,
    target_selected: usize,
) -> RowSelection {
    use parquet::arrow::arrow_reader::RowSelector;

    let selectors: Vec<RowSelector> = rg_selection.iter().cloned().collect();
    let mut result = Vec::new();
    let mut selected_seen = 0usize;
    let mut selected_taken = 0usize;
    let mut physical_skip_prefix = 0usize;
    let mut in_range = false;

    for sel in &selectors {
        if selected_taken >= target_selected {
            // We have enough selected rows; remaining rows become trailing skip
            break;
        }

        if sel.skip {
            if !in_range {
                physical_skip_prefix += sel.row_count;
            } else {
                result.push(RowSelector::skip(sel.row_count));
            }
        } else {
            // This is a select range
            let sel_end = selected_seen + sel.row_count;

            if sel_end <= skip_selected {
                // Entirely before our range — skip all these physical rows
                physical_skip_prefix += sel.row_count;
                selected_seen = sel_end;
                continue;
            }

            // Some or all of this selector is in our range
            let start_in_sel = skip_selected.saturating_sub(selected_seen);
            let available = sel.row_count - start_in_sel;
            let take = available.min(target_selected - selected_taken);

            if !in_range {
                // Add accumulated skip prefix + partial skip within this selector
                let total_skip = physical_skip_prefix + start_in_sel;
                if total_skip > 0 {
                    result.push(RowSelector::skip(total_skip));
                }
                in_range = true;
            } else if start_in_sel > 0 {
                result.push(RowSelector::skip(start_in_sel));
            }

            result.push(RowSelector::select(take));
            selected_taken += take;
            selected_seen = sel_end;
            continue;
        }

        selected_seen += if sel.skip { 0 } else { sel.row_count };
    }

    // Add trailing skip for remaining physical rows in the RG
    // (arrow-rs needs the selection to cover exactly the RG's row count)
    let total_physical: usize = selectors.iter().map(|s| s.row_count).sum();
    let used_physical: usize = result.iter().map(|s| s.row_count).sum();
    let trailing = total_physical - used_physical;
    if trailing > 0 {
        result.push(RowSelector::skip(trailing));
    }

    RowSelection::from(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::RowSelector;
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};
    use std::sync::Arc;

    /// Create test RG metadata with a single INT32 column.
    /// `compressed_bytes` sets the column's compressed size.
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
        // Each RG is 5MB — well below 2*15MB threshold
        let metadata = vec![
            make_rg_metadata_with_bytes(30_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(40_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(50_000, 5 * MB as i64),
            make_rg_metadata_with_bytes(20_000, 5 * MB as i64),
        ];
        let indexes: Vec<usize> = (0..4).collect();

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        // 30k + 40k + 50k = 120k >= 100k min_rows → first morsel
        // 20k → second morsel (remainder)
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].row_group_indexes, vec![0, 1, 2]);
        assert_eq!(morsels[0].est_rows, 120_000);
        assert_eq!(morsels[1].row_group_indexes, vec![3]);
        assert_eq!(morsels[1].est_rows, 20_000);
        assert!(morsels[0].row_selection.is_none());
        assert!(morsels[1].row_selection.is_none());
    }

    #[test]
    fn test_large_rg_split_by_bytes() {
        // 1M rows, 150MB compressed → 150MB / 15MB = 10 morsels
        let metadata = vec![make_rg_metadata_with_bytes(1_000_000, 150 * MB as i64)];
        let indexes = vec![0];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        assert_eq!(morsels.len(), 10);
        for morsel in &morsels {
            assert_eq!(morsel.row_group_indexes, vec![0]);
            assert_eq!(morsel.est_rows, 100_000);
            assert!(morsel.row_selection.is_some());
        }
    }

    #[test]
    fn test_large_rg_uneven_split() {
        // 100K rows, 40MB → n=ceil(40/15)=3, rows split: 33334+33333+33333
        let metadata = vec![make_rg_metadata_with_bytes(100_000, 40 * MB as i64)];
        let indexes = vec![0];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        assert_eq!(morsels.len(), 3);
        assert_eq!(morsels[0].est_rows, 33334);
        assert_eq!(morsels[1].est_rows, 33333);
        assert_eq!(morsels[2].est_rows, 33333);

        for morsel in &morsels {
            assert_eq!(morsel.row_group_indexes, vec![0]);
            assert!(morsel.row_selection.is_some());
        }
    }

    #[test]
    fn test_mixed_small_and_large() {
        let metadata = vec![
            make_rg_metadata_with_bytes(50_000, 5 * MB as i64), // small
            make_rg_metadata_with_bytes(500_000, 75 * MB as i64), // large (75MB >= 30MB)
            make_rg_metadata_with_bytes(30_000, 3 * MB as i64), // small
        ];
        let indexes = vec![0, 1, 2];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        // RG 0: 5MB → accumulate
        // RG 1: 75MB >= 30MB → flush RG 0, split RG 1 into ceil(75/15)=5
        // RG 2: 3MB → remainder morsel
        assert_eq!(morsels.len(), 7);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
        assert_eq!(morsels[0].est_rows, 50_000);
        for morsel in &morsels[1..6] {
            assert_eq!(morsel.row_group_indexes, vec![1]);
            assert_eq!(morsel.est_rows, 100_000);
        }
        assert_eq!(morsels[6].row_group_indexes, vec![2]);
        assert_eq!(morsels[6].est_rows, 30_000);
    }

    #[test]
    fn test_borderline_not_split() {
        // 29MB < 2*15MB = 30MB → should NOT be split
        let metadata = vec![make_rg_metadata_with_bytes(200_000, 29 * MB as i64)];
        let indexes = vec![0];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        assert_eq!(morsels.len(), 1);
        assert_eq!(morsels[0].row_group_indexes, vec![0]);
        assert_eq!(morsels[0].est_rows, 200_000);
        assert!(morsels[0].row_selection.is_none());
    }

    #[test]
    fn test_split_with_existing_row_selection() {
        // 300K rows, 45MB → split into 3
        let metadata = vec![make_rg_metadata_with_bytes(300_000, 45 * MB as i64)];
        let indexes = vec![0];

        // Selection: skip 50K, select 200K, skip 50K
        let selection = RowSelection::from(vec![
            RowSelector::skip(50_000),
            RowSelector::select(200_000),
            RowSelector::skip(50_000),
        ]);

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            Some(&selection),
            &[0],
            100_000,
            15 * MB,
        );

        assert_eq!(morsels.len(), 3);
        for morsel in &morsels {
            assert_eq!(morsel.row_group_indexes, vec![0]);
            assert!(morsel.row_selection.is_some());
        }
    }

    #[test]
    fn test_empty_input() {
        let metadata = vec![];
        let morsels =
            split_row_groups_into_morsels(&[], &metadata, None, &[], 100_000, 15 * MB);
        assert!(morsels.is_empty());
    }

    #[test]
    fn test_row_selection_for_simple_split() {
        // 200K rows, 30MB = exactly 2*15MB → split into 2
        let metadata = vec![make_rg_metadata_with_bytes(200_000, 30 * MB as i64)];
        let indexes = vec![0];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );

        assert_eq!(morsels.len(), 2);

        // First morsel: select first 100K, skip last 100K
        let sel0 = morsels[0].row_selection.as_ref().unwrap();
        let sels0: Vec<_> = sel0.iter().collect();
        assert_eq!(sels0.len(), 2);
        assert!(!sels0[0].skip);
        assert_eq!(sels0[0].row_count, 100_000);
        assert!(sels0[1].skip);
        assert_eq!(sels0[1].row_count, 100_000);

        // Second morsel: skip first 100K, select last 100K
        let sel1 = morsels[1].row_selection.as_ref().unwrap();
        let sels1: Vec<_> = sel1.iter().collect();
        assert_eq!(sels1.len(), 2);
        assert!(sels1[0].skip);
        assert_eq!(sels1[0].row_count, 100_000);
        assert!(!sels1[1].skip);
        assert_eq!(sels1[1].row_count, 100_000);
    }

    #[test]
    fn test_projection_affects_split() {
        // 2-column RG: col0=10MB, col1=50MB. Total=60MB.
        // Projecting col0 only: 10MB < 30MB → no split
        // Projecting col1 only: 50MB >= 30MB → split into ceil(50/15)=4
        // Projecting both: 60MB >= 30MB → split into ceil(60/15)=4
        let metadata = vec![make_rg_metadata_2col(
            100_000,
            10 * MB as i64,
            50 * MB as i64,
        )];
        let indexes = vec![0];

        // Project col0 only (small column) → no split
        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0],
            100_000,
            15 * MB,
        );
        assert_eq!(morsels.len(), 1);
        assert!(morsels[0].row_selection.is_none());

        // Project col1 only (large column) → split
        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[1],
            100_000,
            15 * MB,
        );
        assert_eq!(morsels.len(), 4);
        for morsel in &morsels {
            assert_eq!(morsel.row_group_indexes, vec![0]);
            assert!(morsel.row_selection.is_some());
        }

        // Project both columns → split based on total projected bytes
        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[0, 1],
            100_000,
            15 * MB,
        );
        assert_eq!(morsels.len(), 4);
    }

    #[test]
    fn test_empty_projection_uses_total_size() {
        // Empty projection should fall back to compressed_size()
        let metadata = vec![make_rg_metadata_with_bytes(200_000, 30 * MB as i64)];
        let indexes = vec![0];

        let morsels = split_row_groups_into_morsels(
            &indexes,
            &metadata,
            None,
            &[],
            100_000,
            15 * MB,
        );

        // 30MB >= 2*15MB → should split
        assert_eq!(morsels.len(), 2);
    }
}
