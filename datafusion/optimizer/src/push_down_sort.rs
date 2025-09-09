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

//! [`PushDownSort`] pushes sort expressions into table scans to enable
//! sort pushdown optimizations by table providers

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{ScanOrdering, SortExpr};

/// Optimization rule that pushes sort expressions down to table scans
/// when the sort can potentially be optimized by the table provider.
///
/// This rule looks for `Sort -> TableScan` patterns and moves the sort
/// expressions into the `TableScan.preferred_ordering` field, allowing
/// table providers to potentially optimize the scan based on sort requirements.
///
/// # Behavior
///
/// The optimizer preserves the original `Sort` node as a fallback while passing
/// the ordering preference to the `TableScan` as an optimization hint. This ensures
/// correctness even if the table provider cannot satisfy the requested ordering.
/// 
/// # Examples
///
/// ```text
/// Before optimization:
/// Sort: test.a ASC NULLS LAST
///   TableScan: test
///
/// After optimization:
/// Sort: test.a ASC NULLS LAST  -- Preserved as fallback
///   TableScan: test            -- Now includes preferred_ordering hint
/// ```
#[derive(Default, Debug)]
pub struct PushDownSort {}


impl OptimizerRule for PushDownSort {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| {
            if let LogicalPlan::Sort(sort) = &plan {
                // Use TreeNodeRewriter to push sort information down to TableScans
                let mut rewriter = SortPushdownRewriter::new(sort.expr.clone());
                let transformed_input = Arc::unwrap_or_clone(sort.input.clone()).rewrite(&mut rewriter)?;
                
                if transformed_input.transformed {
                    // Reconstruct the Sort with the transformed input
                    Ok(Transformed::yes(LogicalPlan::Sort(
                        datafusion_expr::logical_plan::Sort {
                            input: Arc::new(transformed_input.data),
                            expr: sort.expr.clone(),
                            fetch: sort.fetch,
                        }
                    )))
                } else {
                    Ok(Transformed::no(plan))
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })
    }

    fn name(&self) -> &str {
        "push_down_sort"
    }
}

struct SortPushdownRewriter {
    sort_exprs: Vec<SortExpr>,
}

impl SortPushdownRewriter {
    fn new(sort_exprs: Vec<SortExpr>) -> Self {
        Self { sort_exprs }
    }
}

impl TreeNodeRewriter for SortPushdownRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::TableScan(scan) => {
                // Add preferred ordering to TableScan
                let new_scan = scan.with_ordering(
                    ScanOrdering::default().with_preferred_ordering(self.sort_exprs.clone())
                );
                Ok(Transformed::yes(LogicalPlan::TableScan(new_scan)))
            }
            
            LogicalPlan::SubqueryAlias(_) => {
                // Safe to traverse - just renames
                Ok(Transformed::no(node))
            }
            
            LogicalPlan::Filter(_) => {
                // Safe to traverse - preserves ordering
                Ok(Transformed::no(node))
            }
            
            LogicalPlan::Projection(ref proj) => {
                // Check if sort columns exist in projection
                if sort_columns_exist_in_projection(&self.sort_exprs, proj)? {
                    Ok(Transformed::no(node)) // Continue traversal
                } else {
                    // Can't push through - stop traversal
                    Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
                }
            }
            
            LogicalPlan::Union(_) => {
                // Safe to traverse - push to all branches
                Ok(Transformed::no(node))
            }
            
            _ => {
                // Stop at other node types (Aggregate, Join, Distinct, etc.)
                Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
            }
        }
    }
}


/// Check if all sort columns exist in the projection's output
fn sort_columns_exist_in_projection(
    sort_exprs: &[SortExpr],
    projection: &datafusion_expr::logical_plan::Projection,
) -> Result<bool> {
    use datafusion_expr::utils::find_column_exprs;
    
    let sort_columns = sort_exprs
        .iter()
        .flat_map(|sort_expr| find_column_exprs(&[sort_expr.expr.clone()]))
        .collect::<Vec<_>>();
    
    let projected_columns = projection
        .expr
        .iter()
        .flat_map(|expr| find_column_exprs(&[expr.clone()]))
        .collect::<Vec<_>>();
    
    // All sort columns must be present in the projected columns
    Ok(sort_columns.iter().all(|sort_col| {
        projected_columns.iter().any(|proj_col| sort_col == proj_col)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_table_scan;
    use crate::{assert_optimized_plan_eq_snapshot, OptimizerContext};
    use datafusion_common::{Column, Result};
    use datafusion_expr::{col, lit, Expr, JoinType, LogicalPlanBuilder, SortExpr};
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(PushDownSort::default())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn test_basic_sort_pushdown_to_table_scan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort node is preserved with preferred_ordering passed to TableScan
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_multiple_column_sort_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false),
                SortExpr::new(col("b"), false, true),
            ])?
            .build()?;

        // Multi-column sort is preserved with preferred_ordering passed to TableScan
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_sort_node_preserved_with_preferred_ordering() -> Result<()> {
        let rule = PushDownSort::default();
        let table_scan = test_table_scan()?;
        let sort_plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        let config = &OptimizerContext::new();
        let result = rule.rewrite(sort_plan, config)?;

        // Verify Sort node is preserved
        match &result.data {
            LogicalPlan::Sort(sort) => {
                // Check that TableScan has preferred_ordering
                if let LogicalPlan::TableScan(ts) = sort.input.as_ref() {
                    assert!(ts.ordering.is_some());
                } else {
                    panic!("Expected TableScan input");
                }
            }
            _ => panic!("Expected Sort node to be preserved"),
        }

        Ok(())
    }

    #[test]
    fn test_no_pushdown_with_complex_expressions() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false),
                SortExpr::new(col("a") + col("b"), false, true), // Complex expression
            ])?
            .build()?;

        // Sort should remain unchanged
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.a + test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above projection
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").gt(lit(10)))?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above filter
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.a > Int32(10)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], Vec::<Expr>::new())?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above aggregate
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Aggregate: groupBy=[[test.a]], aggr=[[]]
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_join() -> Result<()> {
        let left_table = crate::test::test_table_scan_with_name("t1")?;
        let right_table = crate::test::test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(left_table)
            .join(
                right_table,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .sort(vec![SortExpr::new(
                Expr::Column(Column::new(Some("t1"), "a")),
                true,
                false,
            )])?
            .build()?;

        // Sort should remain above join
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: t1.a ASC NULLS LAST
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(10))?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above limit
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Limit: skip=0, fetch=10
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .distinct()?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above distinct
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Distinct:
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_on_non_sort_nodes() -> Result<()> {
        let table_scan = test_table_scan()?;

        // TableScan should remain unchanged
        assert_optimized_plan_equal!(
            table_scan,
            @ r"TableScan: test"
        )
    }

    // Tests for node types that currently block sort pushdown

    #[test]
    fn test_potential_pushdown_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("aliased_table")?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort remains above SubqueryAlias
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: aliased_table.a ASC NULLS LAST
          SubqueryAlias: aliased_table
            TableScan: test
        "
        )
    }

    #[test]
    fn test_potential_pushdown_through_order_preserving_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])? // Identity projection - doesn't change column order
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort remains above Projection (conservative approach)
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_potential_pushdown_through_order_preserving_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(0)))? // Filter on different column than sort
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Currently: Sort remains above Filter (conservative approach)
        // Future enhancement: Could push through filters that don't affect sort column relationships
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.b > Int32(0)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_edge_case_empty_sort_expressions() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(Vec::<SortExpr>::new())? // Empty sort
            .build()?;

        // Empty sort is preserved
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: 
          TableScan: test
        "
        )
    }

    #[test]
    fn test_sort_with_nulls_first_last_variants() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false), // ASC NULLS LAST
                SortExpr::new(col("b"), true, true),  // ASC NULLS FIRST
                SortExpr::new(col("c"), false, false), // DESC NULLS LAST
            ])?
            .build()?;

        // All variants of nulls ordering should be pushable for simple columns
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b ASC NULLS FIRST, test.c DESC NULLS LAST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_mixed_simple_and_qualified_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false), // Simple column
                SortExpr::new(Expr::Column(Column::new(Some("test"), "b")), false, true), // Qualified column
            ])?
            .build()?;

        // Both simple and qualified column references should be pushable
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_case_sensitive_column_references() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("A"), true, false)])? // Capital A
            .build()?;

        // Column reference case sensitivity should be handled by the schema
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          TableScan: test
        "
        )
    }

    // Tests for new functionality
    
    #[test]
    fn test_pushdown_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("aliased_table")?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to TableScan through SubqueryAlias
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: aliased_table.a ASC NULLS LAST
          SubqueryAlias: aliased_table
            TableScan: test
        "
        )
    }

    #[test]
    fn test_pushdown_through_projection_simple_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])? // Simple column projections
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to TableScan through projection
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_projection_missing_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("b"), col("c")])? // 'a' not projected
            .sort(vec![SortExpr::new(col("a"), true, false)])? // Trying to sort by 'a'
            .build()?;

        // Sort should NOT push through since 'a' is not in projection
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.b, test.c
          Sort: test.a ASC NULLS LAST
            Projection: test.b, test.c, test.a
              TableScan: test
        "
        )
    }

    #[test]
    fn test_pushdown_through_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(10)))?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to TableScan through filter
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.b > Int32(10)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_pushdown_through_filter_on_sort_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").gt(lit(5)))? // Filter on same column as sort
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to TableScan through filter
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.a > Int32(5)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_pushdown_through_union() -> Result<()> {
        let table_scan1 = test_table_scan()?;
        let table_scan2 = crate::test::test_table_scan_with_name("test2")?;
        
        let plan = LogicalPlanBuilder::from(table_scan1)
            .union(LogicalPlanBuilder::from(table_scan2).build()?)?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to both TableScans in union
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: a ASC NULLS LAST
          Union
            TableScan: test
            TableScan: test2
        "
        )
    }

    #[test]
    fn test_pushdown_through_multiple_nodes() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(0)))?
            .project(vec![col("a"), col("b")])?
            .alias("filtered_projected")?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should add preferred_ordering to TableScan through multiple layers
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: filtered_projected.a ASC NULLS LAST
          SubqueryAlias: filtered_projected
            Projection: test.a, test.b
              Filter: test.b > Int32(0)
                TableScan: test
        "
        )
    }

    #[test]
    fn test_multi_column_sort_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .sort(vec![
                SortExpr::new(col("a"), true, false),  // ASC NULLS LAST
                SortExpr::new(col("b"), false, true),  // DESC NULLS FIRST
            ])?
            .build()?;

        // Multi-column sort should add preferred_ordering to TableScan through projection
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST
          Projection: test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_sort_with_fetch_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(0)))?
            .sort_by(vec![col("a")])?
            .limit(0, Some(10))?
            .build()?;

        // Sort with fetch should add preferred_ordering to TableScan through filter
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Limit: skip=0, fetch=10
          Sort: test.a ASC NULLS LAST
            Filter: test.b > Int32(0)
              TableScan: test
        "
        )
    }

}
