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

//! Conversions between `datafusion-proto-common` types and the `ParquetSink`
//! type. Enabled by the `proto` feature.

use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_proto_common::common::proto_error;
use datafusion_proto_common::protobuf;

use crate::file_format::ParquetSink;

impl TryFrom<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        let config = value
            .config
            .as_ref()
            .ok_or_else(|| proto_error("Missing required field config in ParquetSink"))?
            .try_into()?;
        let parquet_options = value
            .parquet_options
            .as_ref()
            .ok_or_else(|| {
                proto_error("Missing required field parquet_options in ParquetSink")
            })?
            .try_into()?;
        Ok(Self::new(config, parquet_options))
    }
}

impl TryFrom<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}
