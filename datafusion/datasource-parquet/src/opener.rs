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

//! [`ParquetOpener`] for opening Parquet files

use crate::page_filter::PagePruningAccessPlanFilter;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    apply_file_schema_type_coercions, coerce_int96_to_resolution, row_filter,
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory,
};
use arrow::array::RecordBatch;
use datafusion_datasource::file_scan_config::take_column_projections;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_physical_plan::projection::{
    project_schema, ProjectionExpr, ProjectionStream,
};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{FieldRef, SchemaRef, TimeUnit};
use datafusion_common::encryption::FileDecryptionProperties;

use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::{
    is_dynamic_physical_expr, PhysicalExpr,
};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder,
};
use datafusion_pruning::{build_pruning_predicate, FilePruner, PruningPredicate};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use log::debug;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

/// Implements [`FileOpener`] for a parquet file
pub(super) struct ParquetOpener {
    /// Execution partition index
    pub partition_index: usize,
    /// Projection to be applied when reading the file.
    /// This projection is relative to the logical file schema and may need to be adjusted to be applied to the physical
    /// file schema.
    /// It may reference table partition columns, which should be filled in with partition values.
    pub projection: Arc<[Arc<dyn PhysicalExpr>]>,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub limit: Option<usize>,
    /// Optional predicate to apply during the scan
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Schema of the output table without partition columns.
    /// This is the schema we coerce the physical file schema into.
    pub logical_file_schema: SchemaRef,
    /// Partition columns
    pub partition_fields: Vec<FieldRef>,
    /// Optional hint for how large the initial request to read parquet metadata
    /// should be
    pub metadata_size_hint: Option<usize>,
    /// Metrics for reporting
    pub metrics: ExecutionPlanMetricsSet,
    /// Factory for instantiating parquet reader
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Should the filters be evaluated during the parquet scan using
    /// [`DataFusionArrowPredicate`](row_filter::DatafusionArrowPredicate)?
    pub pushdown_filters: bool,
    /// Should the filters be reordered to optimize the scan?
    pub reorder_filters: bool,
    /// Should the page index be read from parquet files, if present, to skip
    /// data pages
    pub enable_page_index: bool,
    /// Should the bloom filter be read from parquet, if present, to skip row
    /// groups
    pub enable_bloom_filter: bool,
    /// Should row group pruning be applied
    pub enable_row_group_stats_pruning: bool,
    /// Coerce INT96 timestamps to specific TimeUnit
    pub coerce_int96: Option<TimeUnit>,
    /// Optional parquet FileDecryptionProperties
    #[cfg(feature = "parquet_encryption")]
    pub file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    /// Optional factory to create file decryption properties dynamically
    #[cfg(feature = "parquet_encryption")]
    pub encryption_factory:
        Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
    /// Maximum size of the predicate cache, in bytes. If none, uses
    /// the arrow-rs default.
    pub max_predicate_cache_size: Option<usize>,
}

impl FileOpener for ParquetOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let file_range = partitioned_file.range.clone();
        let extensions = partitioned_file.extensions.clone();
        let file_location = partitioned_file.object_meta.location.clone();
        let file_name = file_location.to_string();
        let file_metrics =
            ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);

        let metadata_size_hint = partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);

        let mut async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                partitioned_file.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        let batch_size = self.batch_size;

        let mut predicate = self.predicate.clone();
        let mut projection = self.projection.clone();
        let logical_file_schema = Arc::clone(&self.logical_file_schema);
        let partition_fields = self.partition_fields.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let coerce_int96 = self.coerce_int96;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let limit = self.limit;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .global_counter("num_predicate_creation_errors");

        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0); // TODO: do we need to pipe partition index here?

        let expr_adapter_factory = self.expr_adapter_factory.clone();
        let mut predicate_file_schema = Arc::clone(&self.logical_file_schema);

        let enable_page_index = self.enable_page_index;
        #[cfg(feature = "parquet_encryption")]
        let encryption_context = self.get_encryption_context();
        let max_predicate_cache_size = self.max_predicate_cache_size;

        Ok(Box::pin(async move {
            #[cfg(feature = "parquet_encryption")]
            let file_decryption_properties = encryption_context
                .get_file_decryption_properties(&file_location)
                .await?;

            // Prune this file using the file level statistics and partition values.
            // Since dynamic filters may have been updated since planning it is possible that we are able
            // to prune files now that we couldn't prune at planning time.
            // It is assumed that there is no point in doing pruning here if the predicate is not dynamic,
            // as it would have been done at planning time.
            // We'll also check this after every record batch we read,
            // and if at some point we are able to prove we can prune the file using just the file level statistics
            // we can end the stream early.
            let mut file_pruner = predicate
                .as_ref()
                .map(|p| {
                    Ok::<_, DataFusionError>(
                        (is_dynamic_physical_expr(p) | partitioned_file.has_statistics())
                            .then_some(FilePruner::new(
                                Arc::clone(p),
                                &logical_file_schema,
                                partition_fields.clone(),
                                partitioned_file.clone(),
                                predicate_creation_errors.clone(),
                            )?),
                    )
                })
                .transpose()?
                .flatten();

            if let Some(file_pruner) = &mut file_pruner {
                if file_pruner.should_prune()? {
                    // Return an empty stream immediately to skip the work of setting up the actual stream
                    file_metrics.files_ranges_pruned_statistics.add(1);
                    return Ok(futures::stream::empty().boxed());
                }
            }

            // Don't load the page index yet. Since it is not stored inline in
            // the footer, loading the page index if it is not needed will do
            // unnecessary I/O. We decide later if it is needed to evaluate the
            // pruning predicates. Thus default to not requesting if from the
            // underlying reader.
            let mut options = ArrowReaderOptions::new().with_page_index(false);
            #[cfg(feature = "parquet_encryption")]
            if let Some(fd_val) = file_decryption_properties {
                options = options.with_file_decryption_properties((*fd_val).clone());
            }
            let mut metadata_timer = file_metrics.metadata_load_time.timer();

            // Begin by loading the metadata from the underlying reader (note
            // the returned metadata may actually include page indexes as some
            // readers may return page indexes even when not requested -- for
            // example when they are cached)
            let mut reader_metadata =
                ArrowReaderMetadata::load_async(&mut async_file_reader, options.clone())
                    .await?;

            // Note about schemas: we are actually dealing with **3 different schemas** here:
            // - The table schema as defined by the TableProvider.
            //   This is what the user sees, what they get when they `SELECT * FROM table`, etc.
            // - The logical file schema: this is the table schema minus any hive partition columns and projections.
            //   This is what the physicalfile schema is coerced to.
            // - The physical file schema: this is the schema as defined by the parquet file. This is what the parquet file actually contains.
            let mut physical_file_schema = Arc::clone(reader_metadata.schema());

            // The schema loaded from the file may not be the same as the
            // desired schema (for example if we want to instruct the parquet
            // reader to read strings using Utf8View instead). Update if necessary
            if let Some(merged) = apply_file_schema_type_coercions(
                &logical_file_schema,
                &physical_file_schema,
            ) {
                physical_file_schema = Arc::new(merged);
                options = options.with_schema(Arc::clone(&physical_file_schema));
                reader_metadata = ArrowReaderMetadata::try_new(
                    Arc::clone(reader_metadata.metadata()),
                    options.clone(),
                )?;
            }

            if let Some(ref coerce) = coerce_int96 {
                if let Some(merged) = coerce_int96_to_resolution(
                    reader_metadata.parquet_schema(),
                    &physical_file_schema,
                    coerce,
                ) {
                    physical_file_schema = Arc::new(merged);
                    options = options.with_schema(Arc::clone(&physical_file_schema));
                    reader_metadata = ArrowReaderMetadata::try_new(
                        Arc::clone(reader_metadata.metadata()),
                        options.clone(),
                    )?;
                }
            }

            let expr_adapter = expr_adapter_factory.as_ref().map(|factory| {
                let partition_values = partition_fields
                    .iter()
                    .cloned()
                    .zip(partitioned_file.partition_values)
                    .collect_vec();
                factory
                    .create(
                        Arc::clone(&logical_file_schema),
                        Arc::clone(&physical_file_schema),
                    )
                    .with_partition_values(partition_values)
            });
            let expr_simplifier = PhysicalExprSimplifier::new(&physical_file_schema);

            if let Some(expr_adapter) = expr_adapter {
                // Adapt the predicate to the physical file schema.
                // This evaluates missing columns and inserts any necessary casts.
                predicate = predicate
                    .map(|p| {
                        // After rewriting to the file schema, further simplifications may be possible.
                        // For example, if `'a' = col_that_is_missing` becomes `'a' = NULL` that can then be simplified to `FALSE`
                        // and we can avoid doing any more work on the file (bloom filters, loading the page index, etc.).
                        expr_simplifier.simplify(expr_adapter.rewrite(p.as_ref())?)
                    })
                    .transpose()?;
                // Adapt the projection to the physical file schema
                projection = Arc::from(
                    projection
                        .iter()
                        .map(|p| {
                            expr_simplifier.simplify(expr_adapter.rewrite(p.as_ref())?)
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
                predicate_file_schema = Arc::clone(&physical_file_schema);
            }

            // Build predicates for this specific file
            let (pruning_predicate, page_pruning_predicate) = build_pruning_predicates(
                predicate.as_ref(),
                &predicate_file_schema,
                &predicate_creation_errors,
            );

            // The page index is not stored inline in the parquet footer so the
            // code above may not have read the page index structures yet. If we
            // need them for reading and they aren't yet loaded, we need to load them now.
            if should_enable_page_index(enable_page_index, &page_pruning_predicate) {
                reader_metadata = load_page_index(
                    reader_metadata,
                    &mut async_file_reader,
                    // Since we're manually loading the page index the option here should not matter but we pass it in for consistency
                    options.with_page_index(true),
                )
                .await?;
            }

            metadata_timer.stop();

            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                async_file_reader,
                reader_metadata,
            );

            // Now we need to split up our projection into:
            // 1) A ProjectionMask to read from the Parquet file. This can handle struct field access, etc.
            // 2) A Vec<ProjectionExpr> to apply on top of that. For example, this might have partition column literals.
            // For now we only handle column references, e.g. if there is a struct field access we apply the projection after reading the entire struct.
            let (adapted_projections, remaining_exprs) =
                take_column_projections(projection);
            let projected_schema =
                Arc::new(physical_file_schema.project(&adapted_projections)?);
            let projection_exprs = remaining_exprs
                .into_iter()
                .map(|e| {
                    let return_field = e.return_field(&projected_schema)?;
                    Ok(ProjectionExpr::new(e, return_field.name().to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            let output_schema =
                Arc::new(project_schema(&projection_exprs, &projected_schema)?);

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &physical_file_schema,
                    &predicate_file_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                    &schema_adapter_factory,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "Ignoring error building row filter for '{predicate:?}': {e}"
                        );
                    }
                };
            };

            // Determine which row groups to actually read. The idea is to skip
            // as many row groups as possible based on the metadata and query
            let file_metadata = Arc::clone(builder.metadata());
            let predicate = pruning_predicate.as_ref().map(|p| p.as_ref());
            let rg_metadata = file_metadata.row_groups();
            // track which row groups to actually read
            let access_plan =
                create_initial_plan(&file_name, extensions, rg_metadata.len())?;
            let mut row_groups = RowGroupAccessPlanFilter::new(access_plan);
            // if there is a range restricting what parts of the file to read
            if let Some(range) = file_range.as_ref() {
                row_groups.prune_by_range(rg_metadata, range);
            }
            // If there is a predicate that can be evaluated against the metadata
            if let Some(predicate) = predicate.as_ref() {
                if enable_row_group_stats_pruning {
                    row_groups.prune_by_statistics(
                        &physical_file_schema,
                        builder.parquet_schema(),
                        rg_metadata,
                        predicate,
                        &file_metrics,
                    );
                }

                if enable_bloom_filter && !row_groups.is_empty() {
                    row_groups
                        .prune_by_bloom_filters(
                            &physical_file_schema,
                            &mut builder,
                            predicate,
                            &file_metrics,
                        )
                        .await;
                }
            }

            let mut access_plan = row_groups.build();

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index && !access_plan.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    access_plan = p.prune_plan_with_page_index(
                        access_plan,
                        &physical_file_schema,
                        builder.parquet_schema(),
                        file_metadata.as_ref(),
                        &file_metrics,
                    );
                }
            }

            let row_group_indexes = access_plan.row_group_indexes();
            if let Some(row_selection) =
                access_plan.into_overall_row_selection(rg_metadata)?
            {
                builder = builder.with_row_selection(row_selection);
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            if let Some(max_predicate_cache_size) = max_predicate_cache_size {
                builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
            }

            // metrics from the arrow reader itself
            let arrow_reader_metrics = ArrowReaderMetrics::enabled();

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_group_indexes)
                .with_metrics(arrow_reader_metrics.clone())
                .build()?;

            let files_ranges_pruned_statistics =
                file_metrics.files_ranges_pruned_statistics.clone();
            let predicate_cache_inner_records =
                file_metrics.predicate_cache_inner_records.clone();
            let predicate_cache_records = file_metrics.predicate_cache_records.clone();

            let stream = stream.map_err(DataFusionError::from).map(move |b| {
                b.and_then(|b| {
                    copy_arrow_reader_metrics(
                        &arrow_reader_metrics,
                        &predicate_cache_inner_records,
                        &predicate_cache_records,
                    );
                    schema_mapping.map_batch(b)
                })
            });

            let stream = if let Some(file_pruner) = file_pruner {
                EarlyStoppingStream::new(
                    stream,
                    file_pruner,
                    files_ranges_pruned_statistics,
                )
                .boxed()
            } else {
                stream.boxed()
            };
            let stream = ProjectionStream::new(
                output_schema,
                projection_exprs.into_iter().map(|e| e.expr).collect(),
                stream,
                baseline_metrics,
            );
            Ok(stream.boxed())
        }))
    }
}

fn take_column_indices(
    projection: &[Arc<dyn PhysicalExpr>],
) -> (Vec<usize>, Vec<Arc<dyn PhysicalExpr>>) {
    let mut column_index_map: BTreeMap<usize, Arc<dyn PhysicalExpr>> = BTreeMap::new();
    let mut remainder_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![];

    for expr in projection {
        match expr.expr.as_any().downcast_ref::<Column>() {
            Some(col) => {
                // Check if we already have a remapped version of this column
                if let Some(remapped_col) = column_index_map.get(&col.index()) {
                    remainder_exprs.push(ProjectionExpr::new(
                        Arc::clone(remapped_col),
                        expr.alias.clone(),
                    ));
                    continue;
                }
                let new_index = column_index_map.len() - 1; // New index in the projected schema
                let remapped_column = Column::new(col.name(), new_index);
                remainder_exprs.push(ProjectionExpr::new(
                    Arc::new(remapped_column),
                    expr.alias.clone(),
                ));
            }
            None => {
                // We need to remap any Column references in the expression to match the projected schema
                let new_expr = Arc::clone(&expr.expr)
                    .transform(|e| {
                        if let Some(col) = e.as_any().downcast_ref::<Column>() {
                            // Check if we already have a remapped version of this column
                            if let Some(remapped_col) = column_index_map.get(&col.index())
                            {
                                return Ok(Transformed::yes(Arc::clone(remapped_col)));
                            }
                            let new_index = column_index_map.len() - 1; // New index in the projected schema
                            let remapped_column = Column::new(col.name(), new_index);
                            column_index_map
                                .insert(col.index(), Arc::new(remapped_column));
                            return Ok(Transformed::yes(Arc::new(Column::new(
                                col.name(),
                                new_index,
                            ))));
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .data()
                    .expect("infallible transform");
                remainder_exprs.push(ProjectionExpr::new(new_expr, expr.alias.clone()));
            }
        }
    }

    (column_index_map.keys().cloned().collect(), remainder_exprs)
}

/// Copies metrics from ArrowReaderMetrics (the metrics collected by the
/// arrow-rs parquet reader) to the parquet file metrics for DataFusion
fn copy_arrow_reader_metrics(
    arrow_reader_metrics: &ArrowReaderMetrics,
    predicate_cache_inner_records: &Count,
    predicate_cache_records: &Count,
) {
    if let Some(v) = arrow_reader_metrics.records_read_from_inner() {
        predicate_cache_inner_records.add(v);
    }

    if let Some(v) = arrow_reader_metrics.records_read_from_cache() {
        predicate_cache_records.add(v);
    }
}

/// Wraps an inner RecordBatchStream and a [`FilePruner`]
///
/// This can terminate the scan early when some dynamic filters is updated after
/// the scan starts, so we discover after the scan starts that the file can be
/// pruned (can't have matching rows).
struct EarlyStoppingStream<S> {
    /// Has the stream finished processing? All subsequent polls will return
    /// None
    done: bool,
    file_pruner: FilePruner,
    files_ranges_pruned_statistics: Count,
    /// The inner stream
    inner: S,
}

impl<S> EarlyStoppingStream<S> {
    pub fn new(
        stream: S,
        file_pruner: FilePruner,
        files_ranges_pruned_statistics: Count,
    ) -> Self {
        Self {
            done: false,
            inner: stream,
            file_pruner,
            files_ranges_pruned_statistics,
        }
    }
}
impl<S> EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn check_prune(&mut self, input: Result<RecordBatch>) -> Result<Option<RecordBatch>> {
        let batch = input?;

        // Since dynamic filters may have been updated, see if we can stop
        // reading this stream entirely.
        if self.file_pruner.should_prune()? {
            self.files_ranges_pruned_statistics.add(1);
            self.done = true;
            Ok(None)
        } else {
            // Return the adapted batch
            Ok(Some(batch))
        }
    }
}

impl<S> Stream for EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match ready!(self.inner.poll_next_unpin(cx)) {
            None => {
                // input done
                self.done = true;
                Poll::Ready(None)
            }
            Some(input_batch) => {
                let output = self.check_prune(input_batch);
                Poll::Ready(output.transpose())
            }
        }
    }
}

#[derive(Default)]
#[cfg_attr(not(feature = "parquet_encryption"), allow(dead_code))]
struct EncryptionContext {
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    #[cfg(feature = "parquet_encryption")]
    encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
}

#[cfg(feature = "parquet_encryption")]
impl EncryptionContext {
    fn new(
        file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
        encryption_factory: Option<(
            Arc<dyn EncryptionFactory>,
            EncryptionFactoryOptions,
        )>,
    ) -> Self {
        Self {
            file_decryption_properties,
            encryption_factory,
        }
    }

    async fn get_file_decryption_properties(
        &self,
        file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        match &self.file_decryption_properties {
            Some(file_decryption_properties) => {
                Ok(Some(Arc::clone(file_decryption_properties)))
            }
            None => match &self.encryption_factory {
                Some((encryption_factory, encryption_config)) => Ok(encryption_factory
                    .get_file_decryption_properties(encryption_config, file_location)
                    .await?
                    .map(Arc::new)),
                None => Ok(None),
            },
        }
    }
}

#[cfg(not(feature = "parquet_encryption"))]
#[allow(dead_code)]
impl EncryptionContext {
    async fn get_file_decryption_properties(
        &self,
        _file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(None)
    }
}

impl ParquetOpener {
    #[cfg(feature = "parquet_encryption")]
    fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::new(
            self.file_decryption_properties.clone(),
            self.encryption_factory.clone(),
        )
    }

    #[cfg(not(feature = "parquet_encryption"))]
    #[allow(dead_code)]
    fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::default()
    }
}

/// Return the initial [`ParquetAccessPlan`]
///
/// If the user has supplied one as an extension, use that
/// otherwise return a plan that scans all row groups
///
/// Returns an error if an invalid `ParquetAccessPlan` is provided
///
/// Note: file_name is only used for error messages
fn create_initial_plan(
    file_name: &str,
    extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    row_group_count: usize,
) -> Result<ParquetAccessPlan> {
    if let Some(extensions) = extensions {
        if let Some(access_plan) = extensions.downcast_ref::<ParquetAccessPlan>() {
            let plan_len = access_plan.len();
            if plan_len != row_group_count {
                return exec_err!(
                    "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
                );
            }

            // check row group count matches the plan
            return Ok(access_plan.clone());
        } else {
            debug!("DataSourceExec Ignoring unknown extension specified for {file_name}");
        }
    }

    // default to scanning all row groups
    Ok(ParquetAccessPlan::new_all(row_group_count))
}

/// Build a page pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a page pruning
/// predicate, return None.
pub(crate) fn build_page_pruning_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
) -> Arc<PagePruningAccessPlanFilter> {
    Arc::new(PagePruningAccessPlanFilter::new(
        predicate,
        Arc::clone(file_schema),
    ))
}

pub(crate) fn build_pruning_predicates(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> (
    Option<Arc<PruningPredicate>>,
    Option<Arc<PagePruningAccessPlanFilter>>,
) {
    let Some(predicate) = predicate.as_ref() else {
        return (None, None);
    };
    let pruning_predicate = build_pruning_predicate(
        Arc::clone(predicate),
        file_schema,
        predicate_creation_errors,
    );
    let page_pruning_predicate = build_page_pruning_predicate(predicate, file_schema);
    (pruning_predicate, Some(page_pruning_predicate))
}

/// Returns a `ArrowReaderMetadata` with the page index loaded, loading
/// it from the underlying `AsyncFileReader` if necessary.
async fn load_page_index<T: AsyncFileReader>(
    reader_metadata: ArrowReaderMetadata,
    input: &mut T,
    options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata> {
    let parquet_metadata = reader_metadata.metadata();
    let missing_column_index = parquet_metadata.column_index().is_none();
    let missing_offset_index = parquet_metadata.offset_index().is_none();
    // You may ask yourself: why are we even checking if the page index is already loaded here?
    // Didn't we explicitly *not* load it above?
    // Well it's possible that a custom implementation of `AsyncFileReader` gives you
    // the page index even if you didn't ask for it (e.g. because it's cached)
    // so it's important to check that here to avoid extra work.
    if missing_column_index || missing_offset_index {
        let m = Arc::try_unwrap(Arc::clone(parquet_metadata))
            .unwrap_or_else(|e| e.as_ref().clone());
        let mut reader = ParquetMetaDataReader::new_with_metadata(m)
            .with_page_index_policy(PageIndexPolicy::Optional);
        reader.load_page_index(input).await?;
        let new_parquet_metadata = reader.finish()?;
        let new_arrow_reader =
            ArrowReaderMetadata::try_new(Arc::new(new_parquet_metadata), options)?;
        Ok(new_arrow_reader)
    } else {
        // No need to load the page index again, just return the existing metadata
        Ok(reader_metadata)
    }
}

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningAccessPlanFilter>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        compute::cast,
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use bytes::{BufMut, BytesMut};
    use datafusion_common::{
        assert_batches_eq, record_batch, stats::Precision, ColumnStatistics,
        DataFusionError, ScalarValue, Statistics,
    };
    use datafusion_datasource::{
        file_stream::FileOpener,
        schema_adapter::{
            DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory,
            SchemaMapper,
        },
        PartitionedFile,
    };
    use datafusion_expr::{col, lit};
    use datafusion_physical_expr::{
        expressions::DynamicFilterPhysicalExpr, planner::logical2physical, PhysicalExpr,
    };
    use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
    use futures::{Stream, StreamExt};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use parquet::arrow::ArrowWriter;

    use crate::{opener::ParquetOpener, DefaultParquetFileReaderFactory};

    async fn count_batches_and_rows(
        mut stream: std::pin::Pin<
            Box<
                dyn Stream<Item = Result<arrow::array::RecordBatch, DataFusionError>>
                    + Send,
            >,
        >,
    ) -> (usize, usize) {
        let mut num_batches = 0;
        let mut num_rows = 0;
        while let Some(Ok(batch)) = stream.next().await {
            num_rows += batch.num_rows();
            num_batches += 1;
        }
        (num_batches, num_rows)
    }

    async fn collect_batches(
        mut stream: std::pin::Pin<
            Box<
                dyn Stream<Item = Result<arrow::array::RecordBatch, DataFusionError>>
                    + Send,
            >,
        >,
    ) -> Vec<arrow::array::RecordBatch> {
        let mut batches = vec![];
        while let Some(Ok(batch)) = stream.next().await {
            batches.push(batch);
        }
        batches
    }

    async fn write_parquet(
        store: Arc<dyn ObjectStore>,
        filename: &str,
        batch: arrow::record_batch::RecordBatch,
    ) -> usize {
        let mut out = BytesMut::new().writer();
        {
            let mut writer =
                ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let data = out.into_inner().freeze();
        let data_len = data.len();
        store.put(&Path::from(filename), data.into()).await.unwrap();
        data_len
    }

    fn make_dynamic_expr(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            expr.children().into_iter().map(Arc::clone).collect(),
            expr,
        ))
    }

    #[tokio::test]
    async fn test_prune_on_statistics() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(2)]),
            ("b", Float32, vec![Some(1.0), Some(2.0), None])
        )
        .unwrap();

        let data_size =
            write_parquet(Arc::clone(&store), "test.parquet", batch.clone()).await;

        let schema = batch.schema();
        let file = PartitionedFile::new(
            "test.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        )
        .with_statistics(Arc::new(
            Statistics::new_unknown(&schema)
                .add_column_statistics(ColumnStatistics::new_unknown())
                .add_column_statistics(
                    ColumnStatistics::new_unknown()
                        .with_min_value(Precision::Exact(ScalarValue::Float32(Some(1.0))))
                        .with_max_value(Precision::Exact(ScalarValue::Float32(Some(2.0))))
                        .with_null_count(Precision::Exact(1)),
                ),
        ));

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0, 1]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
            }
        };

        // A filter on "a" should not exclude any rows even if it matches the data
        let expr = col("a").eq(lit(1));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // A filter on `b = 5.0` should exclude all rows
        let expr = col("b").eq(lit(ScalarValue::Float32(Some(5.0))));
        let predicate = logical2physical(&expr, &schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_statistics_with_dynamic_expression() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
            }
        };

        // Filter should match the partition value
        let expr = col("part").eq(lit(1));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value
        let expr = col("part").eq(lit(2));
        // Mark the expression as dynamic even if it's not to force partition pruning to happen
        // Otherwise we assume it already happened at the planning stage and won't re-do the work here
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_values_and_file_statistics() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(3)]),
            ("b", Float64, vec![Some(1.0), Some(2.0), None])
        )
        .unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;
        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];
        file.statistics = Some(Arc::new(
            Statistics::new_unknown(&file_schema)
                .add_column_statistics(ColumnStatistics::new_unknown())
                .add_column_statistics(
                    ColumnStatistics::new_unknown()
                        .with_min_value(Precision::Exact(ScalarValue::Float64(Some(1.0))))
                        .with_max_value(Precision::Exact(ScalarValue::Float64(Some(2.0))))
                        .with_null_count(Precision::Exact(1)),
                ),
        ));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, true),
        ]));
        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
            }
        };

        // Filter should match the partition value and file statistics
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Should prune based on partition value but not file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(1.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on file statistics but not partition value
        let expr = col("part").eq(lit(1)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);

        // Should prune based on both partition value and file statistics
        let expr = col("part").eq(lit(2)).and(col("b").eq(lit(7.0)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    #[tokio::test]
    async fn test_prune_on_partition_value_and_data_value() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        // Note: number 3 is missing!
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(4)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: true, // note that this is true!
                reorder_filters: true,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: false, // note that this is false!
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
            }
        };

        // Filter should match the partition value and data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should match the partition value but not the data value
        let expr = col("part").eq(lit(1)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // Filter should not match the partition value but match the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(1)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 1);

        // Filter should not match the partition value or the data value
        let expr = col("part").eq(lit(2)).or(col("a").eq(lit(3)));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }

    /// Test that if the filter is not a dynamic filter and we have no stats we don't do extra pruning work at the file level.
    #[tokio::test]
    async fn test_opener_pruning_skipped_on_static_filters() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_parquet(Arc::clone(&store), "part=1/file.parquet", batch.clone()).await;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(
            "part=1/file.parquet".to_string(),
            u64::try_from(data_size).unwrap(),
        );
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));

        let make_opener = |predicate| {
            ParquetOpener {
                partition_index: 0,
                projection: Arc::new([0]),
                batch_size: 1024,
                limit: None,
                predicate: Some(predicate),
                logical_file_schema: file_schema.clone(),
                metadata_size_hint: None,
                metrics: ExecutionPlanMetricsSet::new(),
                parquet_file_reader_factory: Arc::new(
                    DefaultParquetFileReaderFactory::new(Arc::clone(&store)),
                ),
                partition_fields: vec![Arc::new(Field::new(
                    "part",
                    DataType::Int32,
                    false,
                ))],
                pushdown_filters: false, // note that this is false!
                reorder_filters: false,
                enable_page_index: false,
                enable_bloom_filter: false,
                enable_row_group_stats_pruning: true,
                coerce_int96: None,
                #[cfg(feature = "parquet_encryption")]
                file_decryption_properties: None,
                expr_adapter_factory: Some(Arc::new(DefaultPhysicalExprAdapterFactory)),
                #[cfg(feature = "parquet_encryption")]
                encryption_factory: None,
                max_predicate_cache_size: None,
            }
        };

        // Filter should NOT match the stats but the file is never attempted to be pruned because the filters are not dynamic
        let expr = col("part").eq(lit(2));
        let predicate = logical2physical(&expr, &table_schema);
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 1);
        assert_eq!(num_rows, 3);

        // If we make the filter dynamic, it should prune
        let predicate = make_dynamic_expr(logical2physical(&expr, &table_schema));
        let opener = make_opener(predicate);
        let stream = opener.open(file.clone()).unwrap().await.unwrap();
        let (num_batches, num_rows) = count_batches_and_rows(stream).await;
        assert_eq!(num_batches, 0);
        assert_eq!(num_rows, 0);
    }
}
