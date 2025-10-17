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
use crate::file_scan_config::FileScanConfig;
use crate::file_stream::FileOpener;
use crate::schema_adapter::SchemaAdapterFactory;
use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{not_impl_err, Result, Statistics};
use datafusion_physical_expr::{LexOrdering, PhysicalExpr};
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
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
pub trait FileSource: Send + Sync {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener>;
    /// Any
    fn as_any(&self) -> &dyn Any;
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource>;
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource>;
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource>;
    /// Initialize new instance with projected statistics
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource>;
    /// Returns the filter expression that will be applied during the file scan.
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        None
    }
    /// Return the projected statistics.
    fn projected_statistics(&self) -> Option<&Statistics> {
        None
    }
    /// Return the projected schema
    fn projected_schema(&self);
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet;
    /// Return projected statistics
    fn statistics(&self) -> Result<Statistics>;
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

    /// Try to push down projections into this FileSource.
    ///
    /// This method allows file sources to optimize projection operations by:
    /// - Accepting simple column projections they can handle efficiently
    /// - Returning complex expressions that need to be evaluated by a ProjectionExec
    ///
    /// # Arguments
    /// * `projection` - The projection expressions to consider pushing down
    /// * `config` - The current file scan configuration
    ///
    /// # Returns
    /// `Ok(Some((new_file_source, remainder_projections)))` if pushdown is possible, where:
    /// - `new_file_source` is an updated FileSource with the projection applied
    /// - `remainder_projections` are expressions that couldn't be pushed down and need
    ///   to be evaluated by a ProjectionExec above the scan
    ///
    /// `Ok(None)` if no projection pushdown is applicable
    ///
    /// The default implementation uses [`split_projection_into_simple_column_indices`]
    /// to separate simple column references from complex expressions, then delegates
    /// to [`with_projection`] if simple columns can be pushed down.
    ///
    /// [`split_projection_into_simple_column_indices`]: crate::file_scan_config::split_projection_into_simple_column_indices
    /// [`with_projection`]: Self::with_projection
    fn try_projection_pushdown(
        &self,
        projection: &[datafusion_physical_plan::projection::ProjectionExpr],
        config: &FileScanConfig,
    ) -> Result<Option<(Arc<dyn FileSource>, Vec<datafusion_physical_plan::projection::ProjectionExpr>)>> {
        use crate::file_scan_config::split_projection_into_simple_column_indices;

        let (column_indices, remainder) = split_projection_into_simple_column_indices(projection);

        // If we have simple column indices, we can handle them
        if !column_indices.is_empty() {
            // Create a new config with the simple column projection
            let mut new_config = config.clone();
            new_config.projection = Some(column_indices);

            // Apply the projection to get a new FileSource
            let new_source = self.with_projection(&new_config);

            Ok(Some((new_source, remainder)))
        } else {
            // No simple columns to push down
            Ok(None)
        }
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
