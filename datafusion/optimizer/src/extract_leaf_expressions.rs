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

//! [`ExtractLeafExpressions`] extracts `MoveTowardsLeafNodes` sub-expressions into projections.
//!
//! This optimizer rule normalizes the plan so that all `MoveTowardsLeafNodes` computations
//! (like field accessors) live in Projection nodes immediately above scan nodes, making them
//! eligible for pushdown by the `OptimizeProjections` rule.
//!
//! ## Algorithm
//!
//! This rule uses **TopDown** traversal with projection merging:
//!
//! 1. When encountering a projection with `MoveTowardsLeafNodes` expressions, look at its input
//! 2. If input is a Projection, **merge** the expressions through it using column replacement
//! 3. Continue until we hit a barrier node (TableScan, Join, Aggregate)
//! 4. Idempotency is natural: merged expressions no longer have column refs matching projection outputs
//!
//! ### Special Cases
//!
//! - If ALL expressions in a projection are `MoveTowardsLeafNodes`, push the entire projection down
//! - If NO expressions are `MoveTowardsLeafNodes`, return `Transformed::no`
//!
//! ### Node Classification
//!
//! **Barrier Nodes** (stop pushing, create projection above):
//! - `TableScan` - the leaf, ideal extraction point
//! - `Join` - requires routing to left/right sides
//! - `Aggregate` - changes schema semantics
//! - `SubqueryAlias` - scope boundary
//! - `Union`, `Intersect`, `Except` - schema boundaries
//!
//! **Schema-Preserving Nodes** (push through unchanged):
//! - `Filter` - passes all input columns through
//! - `Sort` - passes all input columns through
//! - `Limit` - passes all input columns through
//!
//! **Projection Nodes** (merge through):
//! - Replace column refs with underlying expressions from the child projection

use indexmap::{IndexMap, IndexSet};
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, ExpressionPlacement, Filter, Limit, Projection, Sort};

use crate::optimizer::ApplyOrder;
use crate::push_down_filter::replace_cols_by_name;
use crate::utils::{EXTRACTED_EXPR_PREFIX, has_all_column_refs};
use crate::{OptimizerConfig, OptimizerRule};

/// Extracts `MoveTowardsLeafNodes` sub-expressions from all nodes into projections.
///
/// This normalizes the plan so that all `MoveTowardsLeafNodes` computations (like field
/// accessors) live in Projection nodes, making them eligible for pushdown.
///
/// # Example
///
/// Given a filter with a struct field access:
///
/// ```text
/// Filter: user['status'] = 'active'
///   TableScan: t [user]
/// ```
///
/// This rule extracts the field access into a projection:
///
/// ```text
/// Filter: __datafusion_extracted_1 = 'active'
///   Projection: user['status'] AS __datafusion_extracted_1, user
///     TableScan: t [user]
/// ```
///
/// The `OptimizeProjections` rule can then push this projection down to the scan.
///
/// **Important:** The `PushDownFilter` rule is aware of projections created by this rule
/// and will not push filters through them. See `is_extracted_expr_projection` in utils.rs.
#[derive(Default, Debug)]
pub struct ExtractLeafExpressions {}

impl ExtractLeafExpressions {
    /// Create a new [`ExtractLeafExpressions`]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExtractLeafExpressions {
    fn name(&self) -> &str {
        "extract_leaf_expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let alias_generator = config.alias_generator();
        extract_from_plan(plan, alias_generator)
    }
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from a plan node.
///
/// With TopDown traversal, we process parent nodes first, allowing us to
/// merge expressions through child projections.
fn extract_from_plan(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    match &plan {
        // Schema-preserving nodes - extract and push down
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => {
            extract_from_schema_preserving(plan, alias_generator)
        }

        // Schema-transforming nodes need special handling
        LogicalPlan::Aggregate(_) => extract_from_aggregate(plan, alias_generator),
        LogicalPlan::Projection(_) => extract_from_projection(plan, alias_generator),
        LogicalPlan::Join(_) => extract_from_join(plan, alias_generator),

        // Everything else passes through unchanged
        _ => Ok(Transformed::no(plan)),
    }
}

/// Extracts from schema-preserving nodes (Filter, Sort, Limit).
///
/// These nodes don't change the schema, so we can extract expressions
/// and push them down to existing extracted projections or create new ones.
///
/// Uses CSE's two-level pattern:
/// 1. Inner extraction projection with ALL columns passed through
/// 2. Outer recovery projection to restore original schema
fn extract_from_schema_preserving(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    // Skip nodes with no children
    if plan.inputs().is_empty() {
        return Ok(Transformed::no(plan));
    }

    let input = plan.inputs()[0].clone();
    let input_schema = Arc::clone(input.schema());

    // Find where to place extractions (look down through schema-preserving nodes)
    let input_arc = Arc::new(input);
    let (target, path) = find_extraction_target(&input_arc);
    let target_schema = Arc::clone(target.schema());

    // Extract using target schema - this is where the projection will be placed
    let mut extractor =
        LeafExpressionExtractor::new(target_schema.as_ref(), alias_generator);

    // Transform expressions
    let transformed = plan.map_expressions(|expr| extractor.extract(expr))?;

    if !extractor.has_extractions() {
        return Ok(transformed);
    }

    let rebuilt_input = extractor.build_extraction_projection(target, path)?;

    // Create the node with new input
    let new_inputs: Vec<LogicalPlan> = std::iter::once(rebuilt_input)
        .chain(
            transformed
                .data
                .inputs()
                .iter()
                .skip(1)
                .map(|p| (*p).clone()),
        )
        .collect();

    let new_plan = transformed
        .data
        .with_new_exprs(transformed.data.expressions(), new_inputs)?;

    // Use CSE's pattern: add recovery projection to restore original schema
    let recovered = build_recover_project_plan(input_schema.as_ref(), new_plan)?;

    Ok(Transformed::yes(recovered))
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from Join nodes.
///
/// For Joins, we extract from:
/// - `on` expressions: pairs of (left_key, right_key) for equijoin
/// - `filter` expression: non-equi join conditions
///
/// Each expression is routed to the appropriate side (left or right) based on
/// which columns it references. Expressions referencing columns from both sides
/// cannot have sub-expressions extracted (they must remain in the filter).
fn extract_from_join(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Join(join) = plan else {
        return Ok(Transformed::no(plan));
    };

    let left_schema = join.left.schema();
    let right_schema = join.right.schema();

    // Create extractors for left and right sides
    // Find extraction targets for each side (look through schema-preserving nodes)
    let (left_target, left_path) = find_extraction_target(&join.left);
    let (right_target, right_path) = find_extraction_target(&join.right);

    let left_target_schema = Arc::clone(left_target.schema());
    let right_target_schema = Arc::clone(right_target.schema());

    let mut left_extractor =
        LeafExpressionExtractor::new(left_target_schema.as_ref(), alias_generator);
    let mut right_extractor =
        LeafExpressionExtractor::new(right_target_schema.as_ref(), alias_generator);

    // Build column checker to route expressions to correct side
    let mut column_checker =
        ColumnChecker::new(left_schema.as_ref(), right_schema.as_ref());

    // Extract from `on` expressions (equijoin keys)
    let mut new_on = Vec::with_capacity(join.on.len());
    let mut any_extracted = false;

    for (left_key, right_key) in &join.on {
        // Left key should reference only left columns
        let new_left = left_extractor.extract(left_key.clone())?;
        if new_left.transformed {
            any_extracted = true;
        }

        // Right key should reference only right columns
        let new_right = right_extractor.extract(right_key.clone())?;
        if new_right.transformed {
            any_extracted = true;
        }

        new_on.push((new_left.data, new_right.data));
    }

    // Extract from `filter` expression
    let new_filter = if let Some(ref filter) = join.filter {
        let extracted = extract_from_join_filter(
            filter.clone(),
            &mut column_checker,
            &mut left_extractor,
            &mut right_extractor,
        )?;
        if extracted.transformed {
            any_extracted = true;
        }
        Some(extracted.data)
    } else {
        None
    };

    if !any_extracted {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    // Save original schema before modifying inputs
    let original_schema = Arc::clone(&join.schema);

    // Build left extraction projection if needed
    let new_left = if left_extractor.has_extractions() {
        Arc::new(left_extractor.build_extraction_projection(left_target, left_path)?)
    } else {
        Arc::clone(&join.left)
    };

    // Build right extraction projection if needed
    let new_right = if right_extractor.has_extractions() {
        Arc::new(right_extractor.build_extraction_projection(right_target, right_path)?)
    } else {
        Arc::clone(&join.right)
    };

    // Create new Join with updated inputs and expressions
    let new_join = datafusion_expr::logical_plan::Join::try_new(
        new_left,
        new_right,
        new_on,
        new_filter,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?;

    // Add recovery projection to restore original schema
    // This hides the intermediate extracted expression columns
    let recovered = build_recover_project_plan(
        original_schema.as_ref(),
        LogicalPlan::Join(new_join),
    )?;

    Ok(Transformed::yes(recovered))
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from a join filter expression.
///
/// For each sub-expression, determines if it references only left, only right,
/// or both columns, and routes extractions accordingly.
fn extract_from_join_filter(
    filter: Expr,
    column_checker: &mut ColumnChecker,
    left_extractor: &mut LeafExpressionExtractor,
    right_extractor: &mut LeafExpressionExtractor,
) -> Result<Transformed<Expr>> {
    filter.transform_down(|expr| {
        // Skip expressions already aliased with extracted expression pattern
        if let Expr::Alias(alias) = &expr
            && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
        {
            return Ok(Transformed {
                data: expr,
                transformed: false,
                tnr: TreeNodeRecursion::Jump,
            });
        }

        match expr.placement() {
            ExpressionPlacement::MoveTowardsLeafNodes => {
                // Check which side this expression belongs to
                if column_checker.is_left_only(&expr) {
                    // Extract to left side
                    let col_ref = left_extractor.add_extracted(expr)?;
                    Ok(Transformed::yes(col_ref))
                } else if column_checker.is_right_only(&expr) {
                    // Extract to right side
                    let col_ref = right_extractor.add_extracted(expr)?;
                    Ok(Transformed::yes(col_ref))
                } else {
                    // References both sides - cannot extract, keep in place
                    // This shouldn't typically happen for MoveTowardsLeafNodes expressions
                    // but we handle it gracefully
                    Ok(Transformed::no(expr))
                }
            }
            ExpressionPlacement::Column => {
                // Track columns for pass-through on appropriate side
                if let Expr::Column(col) = &expr {
                    if column_checker.is_left_only(&expr) {
                        left_extractor.columns_needed.insert(col.clone());
                    } else if column_checker.is_right_only(&expr) {
                        right_extractor.columns_needed.insert(col.clone());
                    }
                }
                Ok(Transformed::no(expr))
            }
            _ => {
                // Continue recursing into children
                Ok(Transformed::no(expr))
            }
        }
    })
}

/// Evaluates the columns referenced in the given expression to see if they refer
/// only to the left or right columns of a join.
struct ColumnChecker<'a> {
    left_schema: &'a DFSchema,
    left_columns: Option<std::collections::HashSet<Column>>,
    right_schema: &'a DFSchema,
    right_columns: Option<std::collections::HashSet<Column>>,
}

impl<'a> ColumnChecker<'a> {
    fn new(left_schema: &'a DFSchema, right_schema: &'a DFSchema) -> Self {
        Self {
            left_schema,
            left_columns: None,
            right_schema,
            right_columns: None,
        }
    }

    /// Return true if the expression references only columns from the left side
    fn is_left_only(&mut self, predicate: &Expr) -> bool {
        if self.left_columns.is_none() {
            self.left_columns = Some(schema_columns(self.left_schema));
        }
        has_all_column_refs(predicate, self.left_columns.as_ref().unwrap())
    }

    /// Return true if the expression references only columns from the right side
    fn is_right_only(&mut self, predicate: &Expr) -> bool {
        if self.right_columns.is_none() {
            self.right_columns = Some(schema_columns(self.right_schema));
        }
        has_all_column_refs(predicate, self.right_columns.as_ref().unwrap())
    }
}

/// Returns all columns in the schema (both qualified and unqualified forms)
fn schema_columns(schema: &DFSchema) -> std::collections::HashSet<Column> {
    schema
        .iter()
        .flat_map(|(qualifier, field)| {
            [
                Column::new(qualifier.cloned(), field.name()),
                Column::new_unqualified(field.name()),
            ]
        })
        .collect()
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from Aggregate nodes.
///
/// For Aggregates, we extract from:
/// - Group-by expressions (full expressions or sub-expressions)
/// - Arguments inside aggregate functions (NOT the aggregate function itself)
///
/// Uses CSE's two-level pattern with NamePreserver for stable name handling.
fn extract_from_aggregate(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Aggregate(agg) = plan else {
        return Ok(Transformed::no(plan));
    };

    // Save original expression names using NamePreserver (like CSE)
    let name_preserver = NamePreserver::new_for_projection();
    let saved_group_names: Vec<_> = agg
        .group_expr
        .iter()
        .map(|e| name_preserver.save(e))
        .collect();
    let saved_aggr_names: Vec<_> = agg
        .aggr_expr
        .iter()
        .map(|e| name_preserver.save(e))
        .collect();

    // Find where to place extractions
    let (target, path) = find_extraction_target(&agg.input);
    let target_schema = Arc::clone(target.schema());

    let mut extractor =
        LeafExpressionExtractor::new(target_schema.as_ref(), alias_generator);

    // Extract from group-by expressions
    let mut new_group_by = Vec::with_capacity(agg.group_expr.len());
    let mut has_extractions = false;

    for expr in &agg.group_expr {
        let transformed = extractor.extract(expr.clone())?;
        if transformed.transformed {
            has_extractions = true;
        }
        new_group_by.push(transformed.data);
    }

    // Extract from aggregate function arguments (not the function itself)
    let mut new_aggr = Vec::with_capacity(agg.aggr_expr.len());

    for expr in &agg.aggr_expr {
        let transformed = extract_from_aggregate_args(expr.clone(), &mut extractor)?;
        if transformed.transformed {
            has_extractions = true;
        }
        new_aggr.push(transformed.data);
    }

    if !has_extractions {
        return Ok(Transformed::no(LogicalPlan::Aggregate(agg)));
    }

    let rebuilt_input = extractor.build_extraction_projection(target, path)?;

    // Restore names in group-by expressions using NamePreserver
    let restored_group_expr: Vec<Expr> = new_group_by
        .into_iter()
        .zip(saved_group_names)
        .map(|(expr, saved)| saved.restore(expr))
        .collect();

    // Restore names in aggregate expressions using NamePreserver
    let restored_aggr_expr: Vec<Expr> = new_aggr
        .into_iter()
        .zip(saved_aggr_names)
        .map(|(expr, saved)| saved.restore(expr))
        .collect();

    // Create new Aggregate with restored names
    // (no outer projection needed if names are properly preserved)
    let new_agg = datafusion_expr::logical_plan::Aggregate::try_new(
        Arc::new(rebuilt_input),
        restored_group_expr,
        restored_aggr_expr,
    )?;

    Ok(Transformed::yes(LogicalPlan::Aggregate(new_agg)))
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from Projection nodes.
///
/// Follows the same pattern as other `extract_from_*` functions:
/// 1. Find extraction target
/// 2. Extract sub-expressions using `LeafExpressionExtractor`
/// 3. Build extraction projection (merged or fresh)
/// 4. Build outer projection with remainder expressions (names restored)
fn extract_from_projection(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(proj) = plan else {
        return Ok(Transformed::no(plan));
    };

    let (target, path) = find_extraction_target(&proj.input);
    let target_schema = Arc::clone(target.schema());

    let mut extractor =
        LeafExpressionExtractor::new(target_schema.as_ref(), alias_generator);

    // Save names so we can restore them on the remainder expressions
    let name_preserver = NamePreserver::new_for_projection();
    let saved_names: Vec<_> = proj.expr.iter().map(|e| name_preserver.save(e)).collect();

    // Extract from each expression
    let mut rewritten = Vec::with_capacity(proj.expr.len());
    let mut any_extracted = false;
    for expr in &proj.expr {
        let transformed = extractor.extract(expr.clone())?;
        if transformed.transformed {
            any_extracted = true;
        }
        rewritten.push(transformed.data);
    }

    if !any_extracted {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // If the target is the same as our input AND all rewritten expressions
    // are bare columns, no extraction is needed. When some expressions are
    // partially extracted (not bare columns), we still need the extraction
    // projection even when the target hasn't changed.
    let all_columns = rewritten.iter().all(|e| matches!(e, Expr::Column(_)));
    if all_columns && Arc::ptr_eq(&target, &proj.input) {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    let pairs = extractor.extracted_pairs();
    let extraction_proj = build_extraction_projection_impl(
        &pairs,
        &extractor.columns_needed,
        &target,
        target_schema.as_ref(),
    )?;
    let rebuilt_input = rebuild_path(path, LogicalPlan::Projection(extraction_proj))?;

    // Build remainder (restore names)
    let remainder: Vec<Expr> = rewritten
        .into_iter()
        .zip(saved_names)
        .map(|(expr, saved)| saved.restore(expr))
        .collect();

    let outer = Projection::try_new(remainder, Arc::new(rebuilt_input))?;
    Ok(Transformed::yes(LogicalPlan::Projection(outer)))
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from aggregate function arguments.
///
/// This extracts from inside the aggregate (e.g., from `sum(get_field(x, 'y'))`
/// we extract `get_field(x, 'y')`), but NOT the aggregate function itself.
fn extract_from_aggregate_args(
    expr: Expr,
    extractor: &mut LeafExpressionExtractor,
) -> Result<Transformed<Expr>> {
    match expr {
        Expr::AggregateFunction(mut agg_func) => {
            // Extract from arguments, not the function itself
            let mut any_changed = false;
            let mut new_args = Vec::with_capacity(agg_func.params.args.len());

            for arg in agg_func.params.args {
                let transformed = extractor.extract(arg)?;
                if transformed.transformed {
                    any_changed = true;
                }
                new_args.push(transformed.data);
            }

            if any_changed {
                agg_func.params.args = new_args;
                Ok(Transformed::yes(Expr::AggregateFunction(agg_func)))
            } else {
                agg_func.params.args = new_args;
                Ok(Transformed::no(Expr::AggregateFunction(agg_func)))
            }
        }
        // For aliased aggregates, process the inner expression
        Expr::Alias(alias) => {
            let transformed = extract_from_aggregate_args(*alias.expr, extractor)?;
            Ok(
                transformed
                    .update_data(|e| e.alias_qualified(alias.relation, alias.name)),
            )
        }
        // For other expressions, use regular extraction
        other => extractor.extract(other),
    }
}

// =============================================================================
// Helper Functions for Extraction Targeting
// =============================================================================

/// Build a replacement map from a projection: output_column_name -> underlying_expr.
///
/// This is used to resolve column references through a renaming projection.
/// For example, if a projection has `user AS x`, this maps `x` -> `col("user")`.
fn build_projection_replace_map(projection: &Projection) -> HashMap<String, Expr> {
    projection
        .schema
        .iter()
        .zip(projection.expr.iter())
        .map(|((qualifier, field), expr)| {
            let key = Column::from((qualifier, field)).flat_name();
            (key, expr.clone().unalias())
        })
        .collect()
}

/// Traverses down through schema-preserving nodes to find where to place extractions.
///
/// Returns (target_node, path_to_rebuild) where:
/// - target_node: the node above which to create extraction projection
/// - path_to_rebuild: nodes between our input and target that must be rebuilt
///
/// Schema-preserving nodes that we can look through:
/// - Filter, Sort, Limit: pass all input columns through unchanged
/// - Passthrough projections: only column references
///
/// Barrier nodes where we stop:
/// - TableScan, Join, Aggregate: these are extraction targets
/// - Existing extracted expression projections: we merge into these
/// - Any other node type
fn find_extraction_target(
    input: &Arc<LogicalPlan>,
) -> (Arc<LogicalPlan>, Vec<Arc<LogicalPlan>>) {
    let mut current = Arc::clone(input);
    let mut path = vec![];

    loop {
        match current.as_ref() {
            // Look through schema-preserving nodes
            LogicalPlan::Filter(f) => {
                path.push(Arc::clone(&current));
                current = Arc::clone(&f.input);
            }
            LogicalPlan::Sort(s) => {
                path.push(Arc::clone(&current));
                current = Arc::clone(&s.input);
            }
            LogicalPlan::Limit(l) => {
                path.push(Arc::clone(&current));
                current = Arc::clone(&l.input);
            }
            // Hit a barrier node - create new projection here (or merge into existing)
            _ => {
                return (current, path);
            }
        }
    }
}

/// Rebuilds the path from extraction projection back up to original input.
///
/// Takes a list of nodes (in top-to-bottom order from input towards target)
/// and rebuilds them with the new bottom input.
///
/// For passthrough projections, we update them to include ALL columns from
/// the new input (including any new extracted expression columns that were merged).
fn rebuild_path(
    path: Vec<Arc<LogicalPlan>>,
    new_bottom: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut current = new_bottom;

    // Rebuild path from bottom to top (reverse order)
    for node in path.into_iter().rev() {
        current = match node.as_ref() {
            LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter::try_new(
                f.predicate.clone(),
                Arc::new(current),
            )?),
            LogicalPlan::Sort(s) => LogicalPlan::Sort(Sort {
                expr: s.expr.clone(),
                input: Arc::new(current),
                fetch: s.fetch,
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit {
                skip: l.skip.clone(),
                fetch: l.fetch.clone(),
                input: Arc::new(current),
            }),
            LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection::try_new(
                p.expr.clone(),
                Arc::new(current),
            )?),
            // Should not happen based on find_extraction_target, but handle gracefully
            other => other.with_new_exprs(other.expressions(), vec![current])?,
        };
    }

    Ok(current)
}

/// Build projection to restore original schema (like CSE's build_recover_project_plan).
///
/// This adds a projection that selects only the columns from the original schema,
/// hiding any intermediate extracted expression columns that were added during extraction.
fn build_recover_project_plan(
    schema: &DFSchema,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let col_exprs: Vec<Expr> = schema.iter().map(Expr::from).collect();
    let projection = Projection::try_new(col_exprs, Arc::new(input))?;
    Ok(LogicalPlan::Projection(projection))
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from larger expressions.
struct LeafExpressionExtractor<'a> {
    /// Extracted expressions: maps schema_name -> (original_expr, alias)
    extracted: IndexMap<String, (Expr, String)>,
    /// Columns needed for pass-through
    columns_needed: IndexSet<Column>,
    /// Input schema
    input_schema: &'a DFSchema,
    /// Alias generator
    alias_generator: &'a Arc<AliasGenerator>,
}

impl<'a> LeafExpressionExtractor<'a> {
    fn new(input_schema: &'a DFSchema, alias_generator: &'a Arc<AliasGenerator>) -> Self {
        Self {
            extracted: IndexMap::new(),
            columns_needed: IndexSet::new(),
            input_schema,
            alias_generator,
        }
    }

    /// Extracts `MoveTowardsLeafNodes` sub-expressions, returning rewritten expression.
    fn extract(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // Walk top-down to find MoveTowardsLeafNodes sub-expressions
        expr.transform_down(|e| {
            // Skip expressions already aliased with extracted expression pattern.
            // These were created by a previous extraction pass and should not be
            // extracted again. Use TreeNodeRecursion::Jump to skip children.
            if let Expr::Alias(alias) = &e
                && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
            {
                return Ok(Transformed {
                    data: e,
                    transformed: false,
                    tnr: TreeNodeRecursion::Jump,
                });
            }

            match e.placement() {
                ExpressionPlacement::MoveTowardsLeafNodes => {
                    // Extract this entire sub-tree
                    let col_ref = self.add_extracted(e)?;
                    Ok(Transformed::yes(col_ref))
                }
                ExpressionPlacement::Column => {
                    // Track columns for pass-through
                    if let Expr::Column(col) = &e {
                        self.columns_needed.insert(col.clone());
                    }
                    Ok(Transformed::no(e))
                }
                _ => {
                    // Continue recursing into children
                    Ok(Transformed::no(e))
                }
            }
        })
    }

    /// Adds an expression to extracted set, returns column reference.
    fn add_extracted(&mut self, expr: Expr) -> Result<Expr> {
        let schema_name = expr.schema_name().to_string();

        // Deduplication: reuse existing alias if same expression
        if let Some((_, alias)) = self.extracted.get(&schema_name) {
            return Ok(Expr::Column(Column::new_unqualified(alias)));
        }

        // Track columns referenced by this expression
        for col in expr.column_refs() {
            self.columns_needed.insert(col.clone());
        }

        // Generate unique alias
        let alias = self.alias_generator.next(EXTRACTED_EXPR_PREFIX);
        self.extracted.insert(schema_name, (expr, alias.clone()));

        Ok(Expr::Column(Column::new_unqualified(&alias)))
    }

    fn has_extractions(&self) -> bool {
        !self.extracted.is_empty()
    }

    /// Builds an extraction projection and rebuilds the path back up.
    ///
    /// If the target is already a `Projection`, merges into it; otherwise
    /// creates a new projection that passes through all input columns.
    /// Then rebuilds the intermediate nodes in `path` on top of the new
    /// projection.
    fn build_extraction_projection(
        &self,
        target: Arc<LogicalPlan>,
        path: Vec<Arc<LogicalPlan>>,
    ) -> Result<LogicalPlan> {
        let pairs = self.extracted_pairs();
        let extraction_proj = build_extraction_projection_impl(
            &pairs,
            &self.columns_needed,
            &target,
            self.input_schema,
        )?;
        rebuild_path(path, LogicalPlan::Projection(extraction_proj))
    }

    /// Returns the extracted expressions as (expr, alias) pairs.
    fn extracted_pairs(&self) -> Vec<(Expr, String)> {
        self.extracted.values().cloned().collect()
    }
}

/// Build an extraction projection above the target node.
///
/// If the target is an existing projection, merges into it (dedup by resolved
/// schema_name, resolve columns through rename mapping, add pass-through
/// columns_needed). Otherwise builds a fresh projection with extracted
/// expressions + ALL input schema columns.
fn build_extraction_projection_impl(
    extracted_exprs: &[(Expr, String)],
    columns_needed: &IndexSet<Column>,
    target: &Arc<LogicalPlan>,
    target_schema: &DFSchema,
) -> Result<Projection> {
    if let LogicalPlan::Projection(existing) = target.as_ref() {
        // Merge into existing projection
        let mut proj_exprs = existing.expr.clone();

        // Build a map of existing expressions (by schema_name) to their aliases
        let existing_extractions: IndexMap<String, String> = existing
            .expr
            .iter()
            .filter_map(|e| {
                if let Expr::Alias(alias) = e
                    && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
                {
                    let schema_name = alias.expr.schema_name().to_string();
                    return Some((schema_name, alias.name.clone()));
                }
                None
            })
            .collect();

        // Resolve column references through the projection's rename mapping
        let replace_map = build_projection_replace_map(existing);

        // Add new extracted expressions, resolving column refs through the projection
        for (expr, alias) in extracted_exprs {
            let resolved = replace_cols_by_name(expr.clone().alias(alias), &replace_map)?;
            let resolved_schema_name = if let Expr::Alias(a) = &resolved {
                a.expr.schema_name().to_string()
            } else {
                resolved.schema_name().to_string()
            };
            if !existing_extractions.contains_key(&resolved_schema_name) {
                proj_exprs.push(resolved);
            }
        }

        // Add any new pass-through columns that aren't already in the projection.
        // We check against existing.input.schema() (the projection's source) rather
        // than target_schema (the projection's output) because columns produced
        // by alias expressions (e.g., CSE's __common_expr_N) exist in the output but
        // not the input, and cannot be added as pass-through Column references.
        let existing_cols: IndexSet<Column> = existing
            .expr
            .iter()
            .filter_map(|e| {
                if let Expr::Column(c) = e {
                    Some(c.clone())
                } else {
                    None
                }
            })
            .collect();

        let input_schema = existing.input.schema();
        for col in columns_needed {
            let col_expr = Expr::Column(col.clone());
            let resolved = replace_cols_by_name(col_expr, &replace_map)?;
            if let Expr::Column(resolved_col) = &resolved {
                if !existing_cols.contains(resolved_col)
                    && input_schema.has_column(resolved_col)
                {
                    proj_exprs.push(Expr::Column(resolved_col.clone()));
                }
            }
            // If resolved to non-column expr, it's already computed by existing projection
        }

        Projection::try_new(proj_exprs, Arc::clone(&existing.input))
    } else {
        // Build new projection with extracted expressions + all input columns
        let mut proj_exprs = Vec::new();
        for (expr, alias) in extracted_exprs {
            proj_exprs.push(expr.clone().alias(alias));
        }
        for (qualifier, field) in target_schema.iter() {
            proj_exprs.push(Expr::from((qualifier, field)));
        }
        Projection::try_new(proj_exprs, Arc::clone(target))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::optimize_projections::OptimizeProjections;
    use crate::test::*;
    use crate::{OptimizerContext, assert_optimized_plan_eq_snapshot};
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, col, lit, logical_plan::builder::LogicalPlanBuilder,
    };

    /// A mock UDF that simulates a leaf-pushable function like `get_field`.
    /// It returns `MoveTowardsLeafNodes` when its first argument is Column or MoveTowardsLeafNodes.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockLeafFunc {
        signature: Signature,
    }

    impl MockLeafFunc {
        fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Any(2),
                    datafusion_expr::Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MockLeafFunc {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "mock_leaf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unimplemented!("This is only used for testing optimization")
        }

        fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
            // Return MoveTowardsLeafNodes if first arg is Column or MoveTowardsLeafNodes
            // (like get_field does)
            match args.first() {
                Some(ExpressionPlacement::Column)
                | Some(ExpressionPlacement::MoveTowardsLeafNodes) => {
                    ExpressionPlacement::MoveTowardsLeafNodes
                }
                _ => ExpressionPlacement::KeepInPlace,
            }
        }
    }

    fn mock_leaf(expr: Expr, name: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(MockLeafFunc::new())),
            vec![expr, lit(name)],
        ))
    }

    /// Asserts that the optimized plan matches the expected snapshot.
    ///
    /// This applies the `ExtractLeafExpressions` and `OptimizeProjections` rules
    /// to the given plan and compares the result to the expected snapshot.
    ///
    /// The use of `OptimizeProjections` gives us a bit more of a realistic scenario
    /// otherwise the optimized plans will look very different from what an actual integration
    /// test would produce.
    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(ExtractLeafExpressions::new()), Arc::new(OptimizeProjections::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Apply just the OptimizeProjections rule for testing purposes.
    /// This is essentially what the plans would look like without our extraction.
    macro_rules! assert_plan_eq_snapshot {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(OptimizeProjections::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    #[test]
    fn test_extract_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[user]
        "#)?;

        // Note: An outer projection is added to preserve the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_no_extraction_for_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1)))?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")?;

        // No extraction should happen for simple columns
        assert_optimized_plan_equal!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")
    }

    #[test]
    fn test_extract_from_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Projection expressions with MoveTowardsLeafNodes are extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_extract_from_projection_with_subexpression() -> Result<()> {
        // Extraction happens on sub-expressions within projection
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                mock_leaf(col("user"), "name")
                    .is_not_null()
                    .alias("has_name"),
            ])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)?;

        // The mock_leaf sub-expression is extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_no_extraction_for_column() -> Result<()> {
        // Projections with only columns don't need extraction
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @"TableScan: test projection=[a, b]")?;

        // No extraction needed
        assert_optimized_plan_equal!(plan, @"TableScan: test projection=[a, b]")
    }

    #[test]
    fn test_filter_with_deduplication() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field_access = mock_leaf(col("user"), "name");
        // Filter with the same expression used twice
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                field_access
                    .clone()
                    .is_not_null()
                    .and(field_access.is_null()),
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL AND mock_leaf(test.user, Utf8("name")) IS NULL
          TableScan: test projection=[user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 IS NOT NULL AND __datafusion_extracted_1 IS NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_already_leaf_expression_in_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // A bare mock_leaf expression is already MoveTowardsLeafNodes
        // When compared to a literal, the comparison is KeepInPlace so extraction happens
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "name").eq(lit("test")))?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) = Utf8("test")
          TableScan: test projection=[user]
        "#)?;

        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 = Utf8("test")
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_group_by() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![mock_leaf(col("user"), "status")], vec![count(lit(1))])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          TableScan: test projection=[user]
        "#)?;

        // Group-by expression is MoveTowardsLeafNodes, so it gets extracted
        // With NamePreserver, names are preserved directly on the aggregate
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[__datafusion_extracted_1 AS mock_leaf(test.user,Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1
            TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_args() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Use count(mock_leaf(...)) since count works with any type
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("user")],
                vec![count(mock_leaf(col("user"), "value"))],
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value")))]]
          TableScan: test projection=[user]
        "#)?;

        // Aggregate argument is MoveTowardsLeafNodes, so it gets extracted
        // With NamePreserver, names are preserved directly on the aggregate
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1) AS COUNT(mock_leaf(test.user,Utf8("value")))]]
          Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
            TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_with_filter_combined() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        // Both filter and projection extractions.
        // BottomUp order: Filter is processed first (gets __datafusion_extracted_1),
        // then Projection merges its extraction into the same extracted projection (gets __datafusion_extracted_2).
        // Both extractions end up in a single projection above the TableScan.
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Filter: __datafusion_extracted_2 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_preserves_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name").alias("username")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)?;

        // Original alias "username" should be preserved in outer projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)
    }

    /// Test: Projection with different field than Filter
    /// SELECT id, s['label'] FROM t WHERE s['value'] > 150
    /// Both s['label'] and s['value'] should be in a single extraction projection.
    #[test]
    fn test_projection_different_field_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter uses s['value']
            .filter(mock_leaf(col("user"), "value").gt(lit(150)))?
            // Projection uses s['label'] (different field)
            .project(vec![col("user"), mock_leaf(col("user"), "label")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Filter: mock_leaf(test.user, Utf8("value")) > Int32(150)
            TableScan: test projection=[user]
        "#)?;

        // BottomUp should merge both extractions into a single projection above TableScan.
        // Filter's s['value'] -> __datafusion_extracted_1
        // Projection's s['label'] -> __datafusion_extracted_2
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("label"))
          Filter: __datafusion_extracted_2 > Int32(150)
            Projection: mock_leaf(test.user, Utf8("label")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_2
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_deduplication() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field = mock_leaf(col("user"), "name");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![field.clone(), field.clone().alias("name2")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)
    }

    // =========================================================================
    // Additional tests for code coverage
    // =========================================================================

    /// Extractions push through Sort nodes to reach the TableScan.
    /// Covers: find_extraction_target Sort branch, rebuild_path Sort
    #[test]
    fn test_extract_through_sort() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection -> Sort -> TableScan
        // The projection's extraction should push through Sort
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("user").sort(true, true)])?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Extraction projection should be placed below the Sort
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)
    }

    /// Extractions push through Limit nodes to reach the TableScan.
    /// Covers: find_extraction_target Limit branch, rebuild_path Limit
    #[test]
    fn test_extract_through_limit() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection -> Limit -> TableScan
        // The projection's extraction should push through Limit
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(10))?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Extraction projection should be placed below the Limit
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Limit: skip=0, fetch=10
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }

    /// Aliased aggregate functions like count(...).alias("cnt") are handled.
    /// Covers: Expr::Alias branch in extract_from_aggregate_args
    #[test]
    fn test_extract_from_aliased_aggregate() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Use count(mock_leaf(...)).alias("cnt") to trigger Alias branch
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("user")],
                vec![count(mock_leaf(col("user"), "value")).alias("cnt")],
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value"))) AS cnt]]
          TableScan: test projection=[user]
        "#)?;

        // The aliased aggregate should have its inner expression extracted
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1) AS cnt]]
          Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
            TableScan: test projection=[user]
        "#)
    }

    /// Aggregates with no MoveTowardsLeafNodes expressions return unchanged.
    /// Covers: early return in extract_from_aggregate when no extractions
    #[test]
    fn test_aggregate_no_extraction() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan()?;
        // GROUP BY col (no MoveTowardsLeafNodes expressions)
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count(col("b"))])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")
    }

    /// Projections containing extracted expression aliases are skipped (already extracted).
    /// Covers: is_extracted_expr_projection skip in extract_from_projection
    #[test]
    fn test_skip_extracted_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Create a projection that already contains an extracted expression alias
        // This simulates what happens after extraction has already occurred
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                mock_leaf(col("user"), "name").alias("__datafusion_extracted_manual"),
                col("user"),
            ])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)?;

        // Should return unchanged because projection already contains extracted expressions
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)
    }

    /// Multiple extractions merge into a single extracted expression projection.
    /// Covers: merge_into_extracted_projection for schema-preserving nodes
    #[test]
    fn test_merge_into_existing_extracted_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Filter -> existing extracted expression Projection -> TableScan
        // We need to manually build the tree where Filter extracts
        // into an existing extracted expression projection
        let plan = LogicalPlanBuilder::from(table_scan)
            // First extraction from inner filter creates __datafusion_extracted_1
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            // Second filter extraction should merge into existing extracted projection
            .filter(mock_leaf(col("user"), "name").is_not_null())?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        // Both extractions should end up in a single extracted expression projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 IS NOT NULL
            Projection: __datafusion_extracted_1, test.user
              Filter: __datafusion_extracted_2 = Utf8("active")
                Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2
                  TableScan: test projection=[user]
        "#)
    }

    /// Extractions push through passthrough projections (columns only).
    /// Covers: passthrough projection handling in rebuild_path
    #[test]
    fn test_extract_through_passthrough_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // Projection(with extraction) -> Projection(cols only) -> TableScan
        // The passthrough projection should be rebuilt with all columns
        let plan = LogicalPlanBuilder::from(table_scan)
            // Inner passthrough projection (only column references)
            .project(vec![col("user")])?
            // Outer projection with extraction
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Extraction should push through the passthrough projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)
    }

    /// Projections with aliased columns (nothing to extract) return unchanged.
    /// Covers: is_fully_extracted early return in extract_from_projection
    #[test]
    fn test_projection_early_return_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Projection with aliased column - nothing to extract
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("x"), col("b")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")
    }

    /// Projections with arithmetic expressions but no MoveTowardsLeafNodes return unchanged.
    /// This hits the early return when has_extractions is false (after checking expressions).
    #[test]
    fn test_projection_with_arithmetic_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Projection with arithmetic expression - not is_fully_extracted
        // but also has no MoveTowardsLeafNodes expressions
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![(col("a") + col("b")).alias("sum")])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")
    }

    /// Aggregate extractions merge into existing extracted projection created by Filter.
    /// Covers: merge_into_extracted_projection call in extract_from_aggregate
    #[test]
    fn test_aggregate_merge_into_extracted_projection() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        // Filter creates extracted projection, then Aggregate merges into it
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter extracts first -> creates extracted projection
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            // Aggregate extracts -> should merge into existing extracted projection
            .aggregate(vec![mock_leaf(col("user"), "name")], vec![count(lit(1))])?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        // Both extractions should be in a single extracted projection
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[__datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Projection: __datafusion_extracted_1
            Filter: __datafusion_extracted_2 = Utf8("active")
              Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2
                TableScan: test projection=[user]
        "#)
    }

    /// Merging adds new pass-through columns not in the existing extracted projection.
    /// When second filter references different column than first, it gets added during merge.
    #[test]
    fn test_merge_with_new_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        // Filter on column 'a' creates extracted projection with column 'a'
        // Then filter on column 'b' needs to add column 'b' during merge
        let plan = LogicalPlanBuilder::from(table_scan)
            // Filter extracts from column 'a'
            .filter(mock_leaf(col("a"), "x").eq(lit(1)))?
            // Filter extracts from column 'b' - needs to add 'b' to existing projection
            .filter(mock_leaf(col("b"), "y").eq(lit(2)))?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.b, Utf8("y")) = Int32(2)
          Filter: mock_leaf(test.a, Utf8("x")) = Int32(1)
            TableScan: test projection=[a, b, c]
        "#)?;

        // Both extractions should be in a single extracted projection,
        // with both 'a' and 'b' columns passed through
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.a, test.b, test.c
          Filter: __datafusion_extracted_1 = Int32(2)
            Projection: __datafusion_extracted_1, test.a, test.b, test.c
              Filter: __datafusion_extracted_2 = Int32(1)
                Projection: mock_leaf(test.b, Utf8("y")) AS __datafusion_extracted_1, test.a, test.b, test.c, mock_leaf(test.a, Utf8("x")) AS __datafusion_extracted_2
                  TableScan: test projection=[a, b, c]
        "#)
    }

    // =========================================================================
    // Join extraction tests
    // =========================================================================

    /// Create a second table scan with struct field for join tests
    fn test_table_scan_with_struct_named(name: &str) -> Result<LogicalPlan> {
        use arrow::datatypes::Schema;
        let schema = Schema::new(test_table_scan_with_struct_fields());
        datafusion_expr::logical_plan::table_scan(Some(name), &schema, None)?.build()
    }

    /// Extraction from equijoin keys (`on` expressions).
    /// Each key expression is routed to its respective side.
    #[test]
    fn test_extract_from_join_on() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join on mock_leaf(left.user, "id") = mock_leaf(right.user, "id")
        let plan = LogicalPlanBuilder::from(left)
            .join_with_expr_keys(
                right,
                JoinType::Inner,
                (
                    vec![mock_leaf(col("user"), "id")],
                    vec![mock_leaf(col("user"), "id")],
                ),
                None,
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
          TableScan: test projection=[user]
          TableScan: right projection=[user]
        "#)?;

        // Both left and right keys should be extracted into their respective sides
        // A recovery projection is added to restore the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, right.user
          Inner Join: __datafusion_extracted_1 = __datafusion_extracted_2
            Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
            Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_2, right.user
              TableScan: right projection=[user]
        "#)
    }

    /// Extraction from non-equi join filter.
    /// Filter sub-expressions are routed based on column references.
    #[test]
    fn test_extract_from_join_filter() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with filter: mock_leaf(left.user, "status") = 'active'
        let plan = LogicalPlanBuilder::from(left)
            .join_on(
                right,
                JoinType::Inner,
                vec![
                    col("test.user").eq(col("right.user")),
                    mock_leaf(col("test.user"), "status").eq(lit("active")),
                ],
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[user]
          TableScan: right projection=[user]
        "#)?;

        // Left-side expression should be extracted to left input
        // A recovery projection is added to restore the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
            TableScan: right projection=[user]
        "#)
    }

    /// Extraction from both left and right sides of a join.
    /// Tests that expressions are correctly routed to each side.
    #[test]
    fn test_extract_from_join_both_sides() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with filters on both sides
        let plan = LogicalPlanBuilder::from(left)
            .join_on(
                right,
                JoinType::Inner,
                vec![
                    col("test.user").eq(col("right.user")),
                    mock_leaf(col("test.user"), "status").eq(lit("active")),
                    mock_leaf(col("right.user"), "role").eq(lit("admin")),
                ],
            )?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active") AND mock_leaf(right.user, Utf8("role")) = Utf8("admin")
          TableScan: test projection=[user]
          TableScan: right projection=[user]
        "#)?;

        // Each side should have its own extraction projection
        // A recovery projection is added to restore the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active") AND __datafusion_extracted_2 = Utf8("admin")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
            Projection: mock_leaf(right.user, Utf8("role")) AS __datafusion_extracted_2, right.user
              TableScan: right projection=[user]
        "#)
    }

    /// Join with no MoveTowardsLeafNodes expressions returns unchanged.
    #[test]
    fn test_extract_from_join_no_extraction() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan()?;
        let right = test_table_scan_with_name("right")?;

        // Simple equijoin on columns (no MoveTowardsLeafNodes expressions)
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a"], vec!["a"]), None)?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")
    }

    /// Join followed by filter with extraction.
    /// Tests extraction from filter above a join that also has extractions.
    #[test]
    fn test_extract_from_filter_above_join() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // Join with extraction in on clause, then filter with extraction
        let plan = LogicalPlanBuilder::from(left)
            .join_with_expr_keys(
                right,
                JoinType::Inner,
                (
                    vec![mock_leaf(col("user"), "id")],
                    vec![mock_leaf(col("user"), "id")],
                ),
                None,
            )?
            .filter(mock_leaf(col("test.user"), "status").eq(lit("active")))?
            .build()?;

        assert_plan_eq_snapshot!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
            TableScan: test projection=[user]
            TableScan: right projection=[user]
        "#)?;

        // Join keys are extracted to respective sides
        // Filter expression is extracted above the join's recovery projection
        // (The filter extraction creates its own projection above the join)
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, right.user
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user, right.user
              Inner Join: __datafusion_extracted_2 = __datafusion_extracted_3
                Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_2, test.user
                  TableScan: test projection=[user]
                Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_3, right.user
                  TableScan: right projection=[user]
        "#)
    }

    // =========================================================================
    // Column-rename through intermediate node tests
    // =========================================================================

    /// Projection with leaf expr above Filter above renaming Projection.
    /// Tests that column refs are resolved through the rename in
    /// build_extraction_projection (extract_from_projection path).
    #[test]
    fn test_extract_through_filter_with_column_rename() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(col("x").is_not_null())?
            .project(vec![mock_leaf(col("x"), "a")])?
            .build()?;
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(x,Utf8("a"))
          Filter: x IS NOT NULL
            Projection: test.user AS x, mock_leaf(test.user, Utf8("a")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }

    /// Same as above but with a partial extraction (leaf + arithmetic).
    #[test]
    fn test_extract_partial_through_filter_with_column_rename() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(col("x").is_not_null())?
            .project(vec![mock_leaf(col("x"), "a").is_not_null()])?
            .build()?;
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 IS NOT NULL AS mock_leaf(x,Utf8("a")) IS NOT NULL
          Filter: x IS NOT NULL
            Projection: test.user AS x, mock_leaf(test.user, Utf8("a")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }

    /// Tests merge_into_extracted_projection path (schema-preserving extraction)
    /// through a renaming projection.
    #[test]
    fn test_extract_from_filter_above_renaming_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("user").alias("x")])?
            .filter(mock_leaf(col("x"), "a").eq(lit("active")))?
            .build()?;
        assert_optimized_plan_equal!(plan, @r#"
        Projection: x
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: test.user AS x, mock_leaf(test.user, Utf8("a")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }
}
