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

use std::collections::VecDeque;
use std::sync::Arc;

use crate::PartitionedFile;
use crate::file_groups::FileGroup;
use crate::file_scan_config::FileScanConfig;
use parking_lot::Mutex;

/// Source of work for `ScanState`.
///
/// Streams that may share work across siblings use [`WorkSource::Shared`],
/// while streams that can not share work (e.g. because they must preserve file
/// order) use  [`WorkSource::Local`].
#[derive(Debug, Clone)]
pub(super) enum WorkSource {
    /// Files this stream will plan locally without sharing them.
    Local(VecDeque<PartitionedFile>),
    /// Files shared with sibling streams.
    Shared(SharedWorkSource),
}

impl WorkSource {
    /// Pop the next file to plan from this work source.
    pub(super) fn pop_front(&mut self) -> Option<PartitionedFile> {
        match self {
            Self::Local(files) => files.pop_front(),
            Self::Shared(shared) => shared.pop_front(),
        }
    }

    /// Return how many queued files should be counted as already processed
    /// when this stream stops early after hitting a global limit.
    pub(super) fn skipped_on_limit(&self) -> usize {
        match self {
            Self::Local(files) => files.len(),
            Self::Shared(_) => 0,
        }
    }
}

/// Shared source of work for sibling `FileStream`s
///
/// The queue is created once per execution and shared by all reorderable
/// sibling streams for that execution. Whichever stream becomes idle first may
/// take the next unopened file from the front of the queue.
///
/// It uses a [`Mutex`] internally to provide thread-safe access
/// to the shared file queue.
#[derive(Debug, Clone)]
pub(crate) struct SharedWorkSource {
    inner: Arc<SharedWorkSourceInner>,
}

#[derive(Debug, Default)]
pub(super) struct SharedWorkSourceInner {
    files: Mutex<VecDeque<PartitionedFile>>,
}

impl SharedWorkSource {
    /// Create a shared work source containing the provided unopened files.
    pub(crate) fn new(files: impl IntoIterator<Item = PartitionedFile>) -> Self {
        let files = files.into_iter().collect();
        Self {
            inner: Arc::new(SharedWorkSourceInner {
                files: Mutex::new(files),
            }),
        }
    }

    /// Create a shared work source for the unopened files in `config`.
    ///
    /// When `config.work_order_hint` is set (populated by Inexact sort
    /// pushdown), files are seeded in globally stats-sorted order so
    /// workers see best-first files first — letting dynamic filters
    /// (TopK, etc.) tighten across the whole scan. Falls back to the
    /// flat group-order iteration when no hint is present or when stats
    /// are unusable.
    pub(crate) fn from_config(config: &FileScanConfig) -> Self {
        if let Some(hint) = config.work_order_hint.as_ref()
            && let Ok(projected_schema) = config.projected_schema()
        {
            let projection_indices =
                config.file_source.projection().as_ref().and_then(|p| {
                    crate::file_scan_config::sort_pushdown::ordered_column_indices_from_projection(p)
                });
            if let Some(sorted) =
                crate::file_scan_config::sort_pushdown::sort_files_globally_by_statistics(
                    &config.file_groups,
                    hint,
                    &projected_schema,
                    projection_indices.as_deref(),
                )
            {
                return Self::new(sorted);
            }
        }
        Self::new(config.file_groups.iter().flat_map(FileGroup::iter).cloned())
    }

    /// Pop the next file from the shared work queue.
    ///
    /// Returns `None` if the queue is empty
    fn pop_front(&self) -> Option<PartitionedFile> {
        self.inner.files.lock().pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionedFile;
    use crate::file_groups::FileGroup;
    use crate::file_scan_config::FileScanConfigBuilder;
    use crate::test_util::MockSource;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use std::sync::Arc;

    use crate::table_schema::TableSchema;

    fn file_with_min_max(name: &str, min: f64, max: f64) -> PartitionedFile {
        PartitionedFile::new(name.to_string(), 1024).with_statistics(Arc::new(
            Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Exact(1024),
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
                    max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
                    ..Default::default()
                }],
            },
        ))
    }

    fn file_no_stats(name: &str) -> PartitionedFile {
        PartitionedFile::new(name.to_string(), 1024)
    }

    fn config_with_groups(
        groups: Vec<FileGroup>,
        hint: Option<LexOrdering>,
    ) -> FileScanConfig {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)]));
        let table_schema = TableSchema::new(file_schema, vec![]);
        let file_source = Arc::new(MockSource::new(table_schema));
        let mut cfg =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
                .with_file_groups(groups)
                .build();
        cfg.work_order_hint = hint;
        cfg
    }

    fn drain_names(source: SharedWorkSource) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(f) = source.pop_front() {
            out.push(f.object_meta.location.as_ref().to_string());
        }
        out
    }

    #[test]
    fn from_config_without_hint_preserves_flat_group_order() {
        let groups = vec![
            FileGroup::new(vec![
                file_with_min_max("g0_a", 30.0, 40.0),
                file_with_min_max("g0_b", 50.0, 60.0),
            ]),
            FileGroup::new(vec![
                file_with_min_max("g1_a", 10.0, 20.0),
                file_with_min_max("g1_b", 70.0, 80.0),
            ]),
        ];
        let cfg = config_with_groups(groups, None);
        let source = SharedWorkSource::from_config(&cfg);
        // No hint → today's flat order is preserved.
        assert_eq!(
            drain_names(source),
            vec!["g0_a", "g0_b", "g1_a", "g1_b"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn from_config_with_hint_seeds_globally_sorted_queue() {
        // Groups whose min values interleave: cross-group ordering is what
        // the hint is supposed to fix.
        let groups = vec![
            FileGroup::new(vec![
                file_with_min_max("g0_hi", 50.0, 60.0),
                file_with_min_max("g0_mid", 30.0, 40.0),
            ]),
            FileGroup::new(vec![
                file_with_min_max("g1_lo", 10.0, 20.0),
                file_with_min_max("g1_top", 70.0, 80.0),
            ]),
        ];
        let hint: LexOrdering =
            [PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))].into();
        let cfg = config_with_groups(groups, Some(hint));
        let source = SharedWorkSource::from_config(&cfg);
        // Sorted ascending by min: 10, 30, 50, 70.
        assert_eq!(
            drain_names(source),
            vec!["g1_lo", "g0_mid", "g0_hi", "g1_top"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn from_config_falls_back_when_stats_missing() {
        // One file without stats → global sort returns None, fallback path
        // used. Queue should match flat group-order iteration.
        let groups = vec![
            FileGroup::new(vec![file_with_min_max("g0_a", 30.0, 40.0)]),
            FileGroup::new(vec![file_no_stats("g1_no_stats")]),
        ];
        let hint: LexOrdering =
            [PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))].into();
        let cfg = config_with_groups(groups, Some(hint));
        let source = SharedWorkSource::from_config(&cfg);
        assert_eq!(
            drain_names(source),
            vec!["g0_a", "g1_no_stats"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }
}
