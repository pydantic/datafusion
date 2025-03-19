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

//! An optimizer rule that detects TopK operations that can generate dynamic filters to be pushed down into file scans

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;

/// An optimizer rule that passes a TopK as a DynamicFilterSource to DataSourceExec executors.
///
/// This optimizer looks for TopK operators in the plan and connects them to compatible
/// data sources by registering the TopK as a source of dynamic filters.
///
/// When a data source is found that can benefit from a TopK's filter, the source is
/// modified to include the TopK as a dynamic filter source via `with_dynamic_filter_source`.
/// During execution, the data source will then consult the TopK's current filter state
/// to determine which files or partitions can be skipped.
#[derive(Debug)]
pub struct TopKDynamicFilters {}

impl PhysicalOptimizerRule for TopKDynamicFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.enable_dynamic_filter_pushdown {
            plan.transform_down(|_plan| {
                // TODO: recurse only traversing approved nodes (e.g., FilterExec, ProjectionExec, SortExec, CoalesceBatchesExec, RepartitionExec; namely not aggregations, joins, etc.)
                // Collect any DataSourceExecs that can use dynamic filters and register the TopK as a source
                todo!();
            })
            .data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "TopKDynamicFilters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
