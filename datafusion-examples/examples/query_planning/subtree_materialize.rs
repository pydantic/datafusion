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

//! Demonstrates subtree materialization - identifying subtrees containing only
//! Projection, Filter, and TableScan nodes, materializing them to parquet files,
//! and replacing the subtree with a TableScan of the materialized data.
//!
//! This technique can be used for:
//! - Caching intermediate results for repeated queries
//! - Pre-computing expensive projections (e.g., UDFs, string operations)
//! - Creating materialized views
//!
//! See `main.rs` for how to run it.

use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::*;

/// Represents a subtree that can be materialized
struct MaterializableSubtree {
    /// The root node of the subtree (Projection, Filter, or TableScan)
    root: LogicalPlan,
    /// Unique identifier for naming the materialized file
    id: usize,
}

/// Result of materializing a subtree
struct MaterializedResult {
    /// The original subtree that was materialized
    original_subtree: LogicalPlan,
    /// The new TableScan plan to use as replacement
    replacement_scan: LogicalPlan,
}

/// Main entry point for the subtree materialization example
pub async fn subtree_materialize() -> Result<()> {
    // Setup
    let ctx = SessionContext::new();
    create_test_data(&ctx)?;

    // Create a temporary directory for materialized files
    let temp_dir = tempfile::tempdir()?;

    // Build the query - this matches what q.sql does
    // The plan will have structure:
    //   Aggregate (GROUP BY lower(group), SUM(val))
    //     Projection (lower(group), val)
    //       Filter (val > 1)
    //         TableScan (t)
    let df = ctx
        .sql(
            r#"SELECT SUM(val) as sum_val, lower("group") as grp
               FROM t
               WHERE val > 1
               GROUP BY lower("group")"#,
        )
        .await?;

    let original_plan = df.logical_plan().clone();
    print_plan("Original Plan", &original_plan);

    // Find materializable subtrees (Projection/Filter/TableScan only)
    let subtrees = find_materializable_subtrees(&original_plan);
    println!("\nFound {} materializable subtree(s)", subtrees.len());

    for subtree in &subtrees {
        print_plan(
            &format!("Materializable Subtree {}", subtree.id),
            &subtree.root,
        );
    }

    // Materialize each subtree and create a new plan
    let mut new_plan = original_plan.clone();
    for subtree in subtrees {
        let result = materialize_subtree(&ctx, &subtree, temp_dir.path()).await?;
        println!(
            "\nMaterialized subtree {} to temporary parquet file",
            subtree.id
        );

        // Replace the subtree with the new TableScan
        new_plan = replace_subtree_with_scan(
            new_plan,
            &result.original_subtree,
            result.replacement_scan,
        )?;
    }

    print_plan("Transformed Plan (using materialized data)", &new_plan);

    // Execute both plans and compare results
    println!("\n=== Original Query Results ===");
    ctx.execute_logical_plan(original_plan)
        .await?
        .show()
        .await?;

    println!("\n=== Transformed Query Results (using materialized data) ===");
    ctx.execute_logical_plan(new_plan).await?.show().await?;

    println!("\nBoth plans produce the same results!");

    Ok(())
}

/// Returns true if this node type can be part of a materializable subtree
fn is_materializable_node(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::TableScan(_)
    )
}

/// Returns true if the entire subtree rooted at this node contains only
/// Projection, Filter, and TableScan nodes
fn is_fully_materializable_subtree(plan: &LogicalPlan) -> bool {
    let mut all_materializable = true;

    // Use apply to visit all nodes in the subtree
    let _ = plan.apply(|node| {
        if !is_materializable_node(node) {
            all_materializable = false;
            return Ok(TreeNodeRecursion::Stop); // Early exit
        }
        Ok(TreeNodeRecursion::Continue)
    });

    all_materializable
}

/// Find all subtrees that contain only Projection, Filter, and TableScan nodes.
/// Returns subtrees where the root is immediately below a non-materializable node
/// (e.g., Aggregate, Join, etc.)
fn find_materializable_subtrees(plan: &LogicalPlan) -> Vec<MaterializableSubtree> {
    let results = RefCell::new(Vec::new());
    let counter = RefCell::new(0usize);

    // Walk the plan tree top-down
    let _ = plan.apply(|node| {
        // For non-materializable nodes, check if any of their inputs
        // are the roots of materializable subtrees
        if !is_materializable_node(node) {
            for input in node.inputs() {
                // Check if this input is the root of a materializable subtree
                if is_fully_materializable_subtree(input) {
                    let id = *counter.borrow();
                    *counter.borrow_mut() += 1;
                    results.borrow_mut().push(MaterializableSubtree {
                        root: input.clone(),
                        id,
                    });
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });

    results.into_inner()
}

/// Materialize a subtree by executing it and writing results to parquet
async fn materialize_subtree(
    ctx: &SessionContext,
    subtree: &MaterializableSubtree,
    output_dir: &Path,
) -> Result<MaterializedResult> {
    let table_name = format!("materialized_{}", subtree.id);
    let file_path = output_dir.join(format!("{}.parquet", table_name));

    // Find the original table name from the subtree (for schema alignment)
    let original_table_name = find_table_name(&subtree.root);

    // Create DataFrame from the subtree plan and execute it
    let df = DataFrame::new(ctx.state(), subtree.root.clone());

    // Write to parquet file
    df.write_parquet(
        file_path.to_str().unwrap(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    )
    .await?;

    // Register the parquet file as a new table
    ctx.register_parquet(
        &table_name,
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Create replacement TableScan for the materialized data
    let provider = ctx.table_provider(&table_name).await?;
    let mut builder =
        LogicalPlanBuilder::scan(&table_name, provider_as_source(provider), None)?;

    // If we found an original table name, wrap with alias to preserve column qualifiers
    // This ensures expressions like `t.val` still work after replacement
    if let Some(orig_name) = original_table_name {
        builder = builder.alias(orig_name)?;
    }

    let replacement_scan = builder.build()?;

    Ok(MaterializedResult {
        original_subtree: subtree.root.clone(),
        replacement_scan,
    })
}

/// Find the table name from a TableScan in the subtree
fn find_table_name(plan: &LogicalPlan) -> Option<String> {
    let mut table_name = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            table_name = Some(scan.table_name.table().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    table_name
}

/// Replace a subtree in the plan with a TableScan of the materialized data
fn replace_subtree_with_scan(
    plan: LogicalPlan,
    original_subtree: &LogicalPlan,
    replacement: LogicalPlan,
) -> Result<LogicalPlan> {
    let transformed = plan.transform(|node| {
        // Check if this node matches the subtree we want to replace
        // We compare by checking if the display representation matches
        // (since LogicalPlan doesn't implement Eq)
        if format!("{:?}", node) == format!("{:?}", original_subtree) {
            // Replace with the new TableScan
            Ok(Transformed::yes(replacement.clone()))
        } else {
            // Keep the node unchanged
            Ok(Transformed::no(node))
        }
    })?;

    Ok(transformed.data)
}

/// Print the logical plan in a readable format
fn print_plan(name: &str, plan: &LogicalPlan) {
    println!("\n=== {} ===", name);
    println!("{}", plan.display_indent());
}

/// Create test data: table "t" with columns "group" (String) and "val" (Int32)
fn create_test_data(ctx: &SessionContext) -> Result<()> {
    let group: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b"]));
    let val: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let batch = RecordBatch::try_from_iter(vec![("group", group), ("val", val)])?;
    ctx.register_batch("t", batch)?;
    Ok(())
}
