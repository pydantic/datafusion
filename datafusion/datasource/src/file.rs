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

//! Common behaviors that every file format needs to implement

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::file_groups::FileGroupPartitioner;
use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use crate::file_stream::{FileOpener, FileStream};
use crate::schema_adapter::SchemaAdapterFactory;
use crate::source::DataSource;
use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{not_impl_err, Result, Statistics};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr,
};
use datafusion_physical_plan::coop::cooperative;
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::{
    all_alias_free_columns, new_projections_for_columns,
};
use datafusion_physical_plan::DisplayFormatType;

use object_store::ObjectStore;

/// Helper function to convert any type implementing FileSource to Arc&lt;dyn FileSource&gt;
pub fn as_file_source<T: FileSource + 'static>(source: T) -> Arc<dyn FileSource> {
    Arc::new(source)
}

/// file format specific behaviors for elements in [`DataSource`]
///
/// See more details on specific implementations:
/// * [`ArrowSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ArrowSource.html)
/// * [`AvroSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.AvroSource.html)
/// * [`CsvSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.CsvSource.html)
/// * [`JsonSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.JsonSource.html)
/// * [`ParquetSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ParquetSource.html)
///
/// [`DataSource`]: crate::source::DataSource
pub trait FileSource: Send + Sync + fmt::Debug {
    fn config(&self) -> FileScanConfig;

    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        partition: usize,
    ) -> Arc<dyn FileOpener>;
    /// Any
    fn as_any(&self) -> &dyn Any;
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource>;
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource>;
    /// Initialize new instance with projection information
    fn with_projection(&self) -> Arc<dyn FileSource>;
    /// Initialize new instance with projected statistics
    fn with_projected_statistics(
        &self,
        projected_statistics: Statistics,
    ) -> Arc<dyn FileSource>;
    /// Return projected statistics
    fn projected_statistics(&self) -> Result<Statistics>;
    /// String representation of file source such as "csv", "json", "parquet"
    fn file_type(&self) -> &str;
    /// Format FileType specific information
    fn fmt_extra(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }

    /// If supported by the [`FileSource`], redistribute files across partitions
    /// according to their size. Allows custom file formats to implement their
    /// own repartitioning logic.
    ///
    /// The default implementation uses [`FileGroupPartitioner`]. See that
    /// struct for more details.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if config.file_compression_type.is_compressed() || config.new_lines_in_values {
            return Ok(None);
        }

        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut source = config.clone();
            source.file_groups = repartitioned_file_groups;
            return Ok(Some(source));
        }
        Ok(None)
    }

    /// Try to push down filters into this FileSource.
    /// See [`ExecutionPlan::handle_child_pushdown_result`] for more details.
    ///
    /// [`ExecutionPlan::handle_child_pushdown_result`]: datafusion_physical_plan::ExecutionPlan::handle_child_pushdown_result
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
    }

    /// Set optional schema adapter factory.
    ///
    /// [`SchemaAdapterFactory`] allows user to specify how fields from the
    /// file get mapped to that of the table schema.  If you implement this
    /// method, you should also implement [`schema_adapter_factory`].
    ///
    /// The default implementation returns a not implemented error.
    ///
    /// [`schema_adapter_factory`]: Self::schema_adapter_factory
    fn with_schema_adapter_factory(
        &self,
        _factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        not_impl_err!(
            "FileSource {} does not support schema adapter factory",
            self.file_type()
        )
    }

    /// Returns the current schema adapter factory if set
    ///
    /// Default implementation returns `None`.
    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        None
    }
}

impl<T: FileSource + 'static> DataSource for T {
    fn open(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.config().object_store_url)?;
        let batch_size = self
            .config()
            .batch_size
            .unwrap_or_else(|| context.session_config().batch_size());

        let source = self.with_batch_size(batch_size).with_projection();

        let opener = source.create_file_opener(object_store, partition);

        let stream =
            FileStream::new(&self.config(), partition, opener, &source.config().metrics)?;
        Ok(Box::pin(cooperative(stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.config().file_groups.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        let (schema, constraints, _, orderings) =
            self.config().project(self.projected_statistics().ok());
        EquivalenceProperties::new_with_orderings(schema, orderings)
            .with_constraints(constraints)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self
            .config()
            .projected_stats(self.projected_statistics().ok()))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        todo!("")
    }

    fn fetch(&self) -> Option<usize> {
        self.config().limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.config().metrics.clone()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &datafusion_physical_plan::projection::ProjectionExec,
    ) -> Result<Option<Arc<dyn datafusion_physical_plan::ExecutionPlan>>> {
        // This process can be moved into CsvExec, but it would be an overlap of their responsibility.

        // Must be all column references, with no table partition columns (which can not be projected)
        let partitioned_columns_in_proj = projection.expr().iter().any(|(expr, _)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|expr| expr.index() >= self.config().file_schema.fields().len())
                .unwrap_or(false)
        });

        // If there is any non-column or alias-carrier expression, Projection should not be removed.
        let no_aliases = all_alias_free_columns(projection.expr());

        Ok((no_aliases && !partitioned_columns_in_proj).then(|| {
            let file_scan = self.config();
            let new_projections = new_projections_for_columns(
                projection,
                &file_scan.projection.clone().unwrap_or_else(|| {
                    (0..file_scan.file_schema.fields().len()).collect()
                }),
            );

            let _config = FileScanConfigBuilder::from(file_scan)
                // Assign projected statistics to source
                .with_projection(Some(new_projections))
                .build();

            // DataSourceExec::from_data_source() as _
            todo!("I need to find a way to abstract constructors across file sources...")
        }))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let result = FileSource::try_pushdown_filters(self, filters, config)?;
        match result.updated_node {
            Some(_new_file_source) => {
                // // this is safe, but i don't like this
                // let raw_ptr = Arc::into_raw(new_file_source);
                // let data_source =
                //     unsafe { Arc::from_raw(raw_ptr as *const dyn DataSource) };

                // Ok(FilterPushdownPropagation {
                //     filters: result.filters,
                //     updated_node: Some(data_source),
                // })

                todo!("golf: ask dh")
            }
            None => {
                // If the file source does not support filter pushdown, return the original config
                Ok(FilterPushdownPropagation {
                    filters: result.filters,
                    updated_node: None,
                })
            }
        }
    }
}
