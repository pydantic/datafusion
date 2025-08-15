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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::sync::Arc;

use crate::avro_to_arrow::Reader as AvroReader;

use arrow::datatypes::SchemaRef;
use datafusion_common::error::Result;
use datafusion_common::Statistics;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use object_store::ObjectStore;

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Debug, Clone)]
pub struct AvroSource {
    pub config: FileScanConfig,
}

impl AvroSource {
    /// Initialize an AvroSource with default values
    pub fn new(config: FileScanConfig) -> Self {
        Self { config }
    }

    fn open<R: std::io::Read>(&self, reader: R) -> Result<AvroReader<'static, R>> {
        AvroReader::try_new(
            reader,
            Arc::clone(&self.config.file_schema),
            self.config
                .batch_size
                .expect("Batch size must set before open"),
            self.config.projected_file_column_names().clone(),
        )
    }
}

impl FileSource for AvroSource {
    fn config(&self) -> FileScanConfig {
        self.config.clone()
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(private::AvroOpener {
            config: Arc::new(self.clone()),
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut this = self.clone();
        this.config.batch_size = Some(batch_size);

        Arc::new(this)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut this = self.clone();
        this.config.file_schema = schema;

        Arc::new(this)
    }
    fn with_projected_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut this = self.clone();
        this.config.projected_statistics = Some(statistics);

        Arc::new(this)
    }

    fn with_projection(&self) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn projected_statistics(&self) -> Result<Statistics> {
        let statistics = &self.config.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "avro"
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        Ok(None)
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        let mut this = self.clone();
        this.config.schema_adapter_factory = Some(schema_adapter_factory);

        Ok(Arc::new(this))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.config.schema_adapter_factory.clone()
    }

    fn with_config(&self, config: FileScanConfig) -> Arc<dyn FileSource> {
        let mut this = self.clone();
        this.config = config;

        Arc::new(this)
    }
}

mod private {
    use super::*;

    use bytes::Buf;
    use datafusion_datasource::{
        file_meta::FileMeta, file_stream::FileOpenFuture, PartitionedFile,
    };
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(
            &self,
            file_meta: FileMeta,
            _file: PartitionedFile,
        ) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            let object_store = Arc::clone(&self.object_store);
            Ok(Box::pin(async move {
                let r = object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }
}
