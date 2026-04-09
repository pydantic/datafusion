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

use std::sync::Arc;

use crate::file_scan_config::FileScanConfig;
use crate::file_stream::scan_state::ScanState;
use crate::file_stream::work_source::{SharedWorkSource, WorkSource};
use crate::morsel::{FileOpenerMorselizer, Morselizer};
use datafusion_common::{Result, internal_err};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};

use super::metrics::FileStreamMetrics;
use super::{FileOpener, FileStream, FileStreamState, OnError};

/// Whether this stream may reorder work across sibling `FileStream`s.
///
/// This is derived entirely from [`FileScanConfig`]. Streams that must
/// preserve file order or file-group partition boundaries are not reorderable.
enum Reorderable {
    /// This stream may reorder work using a shared queue of unopened files.
    Yes(SharedWorkSource),
    /// This stream must keep its own local file order.
    No,
}

/// Builder for constructing a [`FileStream`].
pub struct FileStreamBuilder<'a> {
    config: &'a FileScanConfig,
    partition: Option<usize>,
    morselizer: Option<Box<dyn Morselizer>>,
    metrics: Option<&'a ExecutionPlanMetricsSet>,
    on_error: OnError,
    reorderable: Reorderable,
}

impl<'a> FileStreamBuilder<'a> {
    /// Create a new builder for [`FileStream`].
    pub fn new(config: &'a FileScanConfig) -> Self {
        let reorderable = if config.preserve_order || config.partitioned_by_file_group {
            Reorderable::No
        } else {
            let shared_work_source = config
                .shared_work_source
                .get_or_init(SharedWorkSource::new)
                .clone();
            Reorderable::Yes(shared_work_source)
        };

        Self {
            config,
            partition: None,
            morselizer: None,
            metrics: None,
            on_error: OnError::Fail,
            reorderable,
        }
    }

    /// Configure the partition to scan.
    pub fn with_partition(mut self, partition: usize) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Configure the [`FileOpener`] used to open files.
    ///
    /// This will overwrite any setting from [`Self::with_morselizer`]
    pub fn with_file_opener(mut self, file_opener: Arc<dyn FileOpener>) -> Self {
        self.morselizer = Some(Box::new(FileOpenerMorselizer::new(file_opener)));
        self
    }

    /// Configure the [`Morselizer`] used to open files.
    ///
    /// This will overwrite any setting from [`Self::with_file_opener`]
    pub fn with_morselizer(mut self, morselizer: Box<dyn Morselizer>) -> Self {
        self.morselizer = Some(morselizer);
        self
    }

    /// Configure the metrics set used by the stream.
    pub fn with_metrics(mut self, metrics: &'a ExecutionPlanMetricsSet) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Configure the behavior when opening or scanning a file fails.
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Build the configured [`FileStream`].
    pub fn build(self) -> Result<FileStream> {
        let Self {
            config,
            partition,
            morselizer,
            metrics,
            on_error,
            reorderable,
        } = self;

        let Some(partition) = partition else {
            return internal_err!("FileStreamBuilder missing required partition");
        };
        let Some(morselizer) = morselizer else {
            return internal_err!("FileStreamBuilder missing required morselizer");
        };
        let Some(metrics) = metrics else {
            return internal_err!("FileStreamBuilder missing required metrics");
        };
        let projected_schema = config.projected_schema()?;
        let Some(file_group) = config.file_groups.get(partition).cloned() else {
            return internal_err!(
                "FileStreamBuilder invalid partition index: {partition}"
            );
        };
        let files = file_group.into_inner();
        let work_source = match reorderable {
            Reorderable::Yes(shared) => {
                shared.register_stream();
                shared.push_files(files);
                WorkSource::Shared(shared)
            }
            Reorderable::No => WorkSource::Local(files.into()),
        };

        let file_stream_metrics = FileStreamMetrics::new(metrics, partition);
        let scan_state = Box::new(ScanState::new(
            work_source,
            config.limit,
            morselizer,
            on_error,
            file_stream_metrics,
        ));

        Ok(FileStream {
            projected_schema,
            state: FileStreamState::Scan { scan_state },
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        })
    }
}
