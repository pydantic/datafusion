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

use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    execution_plan::{ExecutionPlanFilterPushdownResult, FilterPushdownSupport},
    with_new_children_if_necessary, ExecutionPlan,
};

use crate::PhysicalOptimizerRule;

#[derive(Clone, Copy, Debug)]
enum FilterPushdownSupportState {
    ChildExact,
    ChildInexact,
    NoChild,
}

impl FilterPushdownSupportState {
    fn combine_with_other(
        &self,
        other: &FilterPushdownSupport,
    ) -> FilterPushdownSupportState {
        match (other, self) {
            (FilterPushdownSupport::Exact, FilterPushdownSupportState::NoChild) => {
                FilterPushdownSupportState::ChildExact
            }
            (FilterPushdownSupport::Exact, FilterPushdownSupportState::ChildInexact) => {
                FilterPushdownSupportState::ChildInexact
            }
            (FilterPushdownSupport::Inexact, FilterPushdownSupportState::NoChild) => {
                FilterPushdownSupportState::ChildInexact
            }
            (FilterPushdownSupport::Inexact, FilterPushdownSupportState::ChildExact) => {
                FilterPushdownSupportState::ChildInexact
            }
            (
                FilterPushdownSupport::Inexact,
                FilterPushdownSupportState::ChildInexact,
            ) => FilterPushdownSupportState::ChildInexact,
            (FilterPushdownSupport::Exact, FilterPushdownSupportState::ChildExact) => {
                // If both are exact, keep it as exact
                FilterPushdownSupportState::ChildExact
            }
        }
    }
}

fn pushdown_filters(
    node: &Arc<dyn ExecutionPlan>,
    parent_filters: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<ExecutionPlanFilterPushdownResult>> {
    let node_filters = node.filters_for_pushdown()?;
    let children = node.children();
    let mut new_children = Vec::with_capacity(children.len());
    let all_filters = parent_filters
        .iter()
        .chain(node_filters.iter())
        .cloned()
        .collect::<Vec<_>>();
    let mut filter_pushdown_result =
        vec![FilterPushdownSupportState::NoChild; all_filters.len()];
    for child in children {
        if child.supports_filter_pushdown() {
            if let Some(result) = pushdown_filters(child, &all_filters)? {
                new_children.push(result.inner);
                for (all_filters_idx, support) in result.support.iter().enumerate() {
                    filter_pushdown_result[all_filters_idx] = filter_pushdown_result
                        [all_filters_idx]
                        .combine_with_other(support)
                }
            } else {
                new_children.push(Arc::clone(child));
            }
        } else {
            // Reset the filters we are pushing down.
            if let Some(result) = pushdown_filters(child, &Vec::new())? {
                new_children.push(result.inner);
            } else {
                new_children.push(Arc::clone(child));
            }
        };
    }

    let mut node = with_new_children_if_necessary(Arc::clone(node), new_children)?;

    // Now update the node with the result of the pushdown of it's filters
    let pushdown_result = filter_pushdown_result[parent_filters.len()..]
        .iter()
        .map(|s| match s {
            FilterPushdownSupportState::ChildExact => FilterPushdownSupport::Exact,
            FilterPushdownSupportState::ChildInexact => FilterPushdownSupport::Inexact,
            FilterPushdownSupportState::NoChild => FilterPushdownSupport::Inexact,
        })
        .collect::<Vec<_>>();
    if let Some(new_node) =
        Arc::clone(&node).with_filter_pushdown_result(&pushdown_result)?
    {
        node = new_node;
    };

    // And check if it can absorb the remaining filters
    let remaining_filter_indexes = (0..parent_filters.len())
        .filter(|&i| match filter_pushdown_result[i] {
            FilterPushdownSupportState::ChildExact => false,
            _ => true,
        })
        .collect::<Vec<_>>();
    if !remaining_filter_indexes.is_empty() {
        let remaining_filters = remaining_filter_indexes
            .iter()
            .map(|&i| &parent_filters[i])
            .collect::<Vec<_>>();
        if let Some(result) = node.push_down_filters_from_parents(&remaining_filters)? {
            node = result.inner;
            for (parent_filter_index, support) in
                remaining_filter_indexes.iter().zip(result.support)
            {
                filter_pushdown_result[*parent_filter_index] = filter_pushdown_result
                    [*parent_filter_index]
                    .combine_with_other(&support)
            }
        }
    }
    let support = filter_pushdown_result[..parent_filters.len()]
        .iter()
        .map(|s| match s {
            FilterPushdownSupportState::ChildExact => FilterPushdownSupport::Exact,
            FilterPushdownSupportState::ChildInexact => FilterPushdownSupport::Inexact,
            FilterPushdownSupportState::NoChild => FilterPushdownSupport::Inexact,
        })
        .collect::<Vec<_>>();
    Ok(Some(ExecutionPlanFilterPushdownResult::new(node, support)))
}

#[derive(Debug)]
pub struct FilterPushdown {}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterPushdown {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for FilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(result) = pushdown_filters(&plan, &[])? {
            Ok(result.inner)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "FilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}
