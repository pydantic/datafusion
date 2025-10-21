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

use crate::{
    file::FileSource, file_scan_config::FileScanConfig, file_stream::FileOpener,
    schema_adapter::SchemaAdapterFactory,
};

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

/// Minimal [`crate::file::FileSource`] implementation for use in tests.
#[derive(Clone, Default)]
pub(crate) struct MockSource {
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    file_schema: Option<SchemaRef>,
    projection: Option<Vec<datafusion_physical_plan::projection::ProjectionExpr>>,
}

impl MockSource {
    pub fn with_filter(mut self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl FileSource for MockSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.filter.clone()
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.file_schema = Some(schema);
        Arc::new(source)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.projected_statistics = Some(statistics);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self
            .projected_statistics
            .as_ref()
            .expect("projected_statistics must be set")
            .clone())
    }

    fn projection(&self) -> Option<Vec<datafusion_physical_plan::projection::ProjectionExpr>> {
        self.projection.clone()
    }

    fn schema(&self) -> SchemaRef {
        self.file_schema
            .clone()
            .expect("file_schema must be set")
    }

    fn file_type(&self) -> &str {
        "mock"
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn try_projection_pushdown(
        &self,
        projection: &[datafusion_physical_plan::projection::ProjectionExpr],
    ) -> Result<Option<(Arc<dyn FileSource>, Option<Vec<datafusion_physical_plan::projection::ProjectionExpr>>)>> {
        // MockSource accepts all projections (pushes them all down, no remainder)
        let mut new_source = self.clone();
        new_source.projection = Some(projection.to_vec());

        // Compute and store the projected schema
        let file_schema = self.file_schema.as_ref().expect("file_schema must be set");
        let projected_fields: Vec<_> = projection
            .iter()
            .map(|p| {
                let field = p.expr.return_field(file_schema).unwrap();
                Field::new(&p.alias, field.data_type().clone(), field.is_nullable())
            })
            .collect();
        let projected_schema = Arc::new(Schema::new(projected_fields));
        new_source.file_schema = Some(Arc::clone(&projected_schema));

        // If we have a filter, check if it references columns not in the projected schema
        // If so, remove the filter (this is a simplified behavior for testing)
        if let Some(filter) = &new_source.filter {
            use datafusion_physical_expr::utils::collect_columns;
            let filter_columns = collect_columns(filter);
            let all_columns_present = filter_columns.iter().all(|col| {
                projected_schema.fields().iter().any(|f| f.name() == col.name())
            });
            if !all_columns_present {
                new_source.filter = None;
            }
        }

        Ok(Some((Arc::new(new_source), None)))
    }
}

/// Create a column expression
pub(crate) fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
}
