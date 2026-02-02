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
//! This rule uses **BottomUp** traversal to push ALL `MoveTowardsLeafNodes` expressions
//! (like `get_field`) to projections immediately above scan nodes. This enables optimal
//! Parquet column pruning.
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
//! **Schema-Preserving Nodes** (push through):
//! - `Filter` - passes all input columns through
//! - `Sort` - passes all input columns through
//! - `Limit` - passes all input columns through
//! - Passthrough `Projection` - only column references
//!
//! ### How It Works
//!
//! 1. Process leaf nodes first (TableScan, etc.)
//! 2. When processing higher nodes, descendants are already finalized
//! 3. Push extractions DOWN through the plan, merging into existing extracted
//!    expression projections when possible

use indexmap::{IndexMap, IndexSet};
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, ExpressionPlacement, Filter, Limit, Projection, Sort};

use crate::optimizer::ApplyOrder;
use crate::utils::{EXTRACTED_EXPR_PREFIX, is_extracted_expr_projection};
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
        Some(ApplyOrder::BottomUp)
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
/// With BottomUp traversal, we process leaves first, then work up.
/// This allows us to push extractions all the way down to scan nodes.
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

    // Build extraction projection with ALL columns (CSE-style)
    let extraction_proj = if let Some(existing_proj) = get_extracted_projection(&target) {
        merge_into_extracted_projection(existing_proj, &extractor)?
    } else {
        extractor.build_projection_with_all_columns(target)?
    };

    // Rebuild the path from target back up to our node's input
    let rebuilt_input = rebuild_path(path, LogicalPlan::Projection(extraction_proj))?;

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

    // Build extraction projection with ALL columns (CSE-style)
    let extraction_proj = if let Some(existing_proj) = get_extracted_projection(&target) {
        merge_into_extracted_projection(existing_proj, &extractor)?
    } else {
        extractor.build_projection_with_all_columns(target)?
    };

    // Rebuild path from target back up
    let rebuilt_input = rebuild_path(path, LogicalPlan::Projection(extraction_proj))?;

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
/// Uses CSE's two-level pattern (outer + inner projections only):
/// - Inner projection: extraction with ALL columns passed through
/// - Outer projection: rewritten expressions with restored names
///
/// This avoids the unstable 3-level structure that gets broken by OptimizeProjections.
fn extract_from_projection(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(proj) = plan else {
        return Ok(Transformed::no(plan));
    };

    // Skip if this projection is fully extracted (only column references)
    if is_fully_extracted(&proj) {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // Skip if this is already an extracted expression projection.
    // This prevents re-extraction on subsequent optimizer passes.
    if is_extracted_expr_projection(&proj) {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // Save original expression names using NamePreserver (like CSE)
    let name_preserver = NamePreserver::new_for_projection();
    let saved_names: Vec<_> = proj.expr.iter().map(|e| name_preserver.save(e)).collect();

    // Find where to place extractions (look down through schema-preserving nodes)
    let (target, path) = find_extraction_target(&proj.input);
    let target_schema = Arc::clone(target.schema());

    let mut extractor =
        LeafExpressionExtractor::new(target_schema.as_ref(), alias_generator);

    // Extract from projection expressions
    let mut new_exprs = Vec::with_capacity(proj.expr.len());
    let mut has_extractions = false;

    for expr in &proj.expr {
        let transformed = extractor.extract(expr.clone())?;
        if transformed.transformed {
            has_extractions = true;
        }
        new_exprs.push(transformed.data);
    }

    if !has_extractions {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // Build extraction projection with ALL columns (CSE-style)
    let extraction_proj = if let Some(existing_proj) = get_extracted_projection(&target) {
        merge_into_extracted_projection(existing_proj, &extractor)?
    } else {
        extractor.build_projection_with_all_columns(target)?
    };

    // Rebuild path from target back up
    let rebuilt_input = rebuild_path(path, LogicalPlan::Projection(extraction_proj))?;

    // Create outer projection with rewritten exprs + restored names
    let final_exprs: Vec<Expr> = new_exprs
        .into_iter()
        .zip(saved_names)
        .map(|(expr, saved_name)| saved_name.restore(expr))
        .collect();

    let outer_projection = Projection::try_new(final_exprs, Arc::new(rebuilt_input))?;

    Ok(Transformed::yes(LogicalPlan::Projection(outer_projection)))
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
// Helper Functions for BottomUp Traversal
// =============================================================================

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
) -> (Arc<LogicalPlan>, Vec<LogicalPlan>) {
    let mut current = Arc::clone(input);
    let mut path = vec![];

    loop {
        match current.as_ref() {
            // Look through schema-preserving nodes
            LogicalPlan::Filter(f) => {
                path.push(current.as_ref().clone());
                current = Arc::clone(&f.input);
            }
            LogicalPlan::Sort(s) => {
                path.push(current.as_ref().clone());
                current = Arc::clone(&s.input);
            }
            LogicalPlan::Limit(l) => {
                path.push(current.as_ref().clone());
                current = Arc::clone(&l.input);
            }
            // Look through passthrough projections (only column references)
            LogicalPlan::Projection(p) if is_passthrough_projection(p) => {
                path.push(current.as_ref().clone());
                current = Arc::clone(&p.input);
            }
            // Found existing extracted expression projection - will merge into it
            LogicalPlan::Projection(p) if is_extracted_expr_projection(p) => {
                return (current, path);
            }
            // Hit a barrier node - create new projection here
            _ => {
                return (current, path);
            }
        }
    }
}

/// Returns true if the projection is a passthrough (only column references).
fn is_passthrough_projection(proj: &Projection) -> bool {
    proj.expr.iter().all(|e| matches!(e, Expr::Column(_)))
}

/// Returns true if the projection only has column references (nothing to extract).
fn is_fully_extracted(proj: &Projection) -> bool {
    proj.expr.iter().all(|e| {
        matches!(e, Expr::Column(_))
            || matches!(e, Expr::Alias(a) if matches!(a.expr.as_ref(), Expr::Column(_)))
    })
}

/// If the target is an extracted expression projection, return it for merging.
fn get_extracted_projection(target: &Arc<LogicalPlan>) -> Option<&Projection> {
    if let LogicalPlan::Projection(p) = target.as_ref()
        && is_extracted_expr_projection(p)
    {
        return Some(p);
    }
    None
}

/// Merges new extractions into an existing extracted expression projection.
fn merge_into_extracted_projection(
    existing: &Projection,
    extractor: &LeafExpressionExtractor,
) -> Result<Projection> {
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

    // Add new extracted expressions, but only if not already present
    for (schema_name, (expr, alias)) in &extractor.extracted {
        if !existing_extractions.contains_key(schema_name) {
            proj_exprs.push(expr.clone().alias(alias));
        }
    }

    // Add any new pass-through columns that aren't already in the projection
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

    for col in &extractor.columns_needed {
        if !existing_cols.contains(col) && extractor.input_schema.has_column(col) {
            proj_exprs.push(Expr::Column(col.clone()));
        }
    }

    Projection::try_new(proj_exprs, Arc::clone(&existing.input))
}

/// Rebuilds the path from extraction projection back up to original input.
///
/// Takes a list of nodes (in top-to-bottom order from input towards target)
/// and rebuilds them with the new bottom input.
///
/// For passthrough projections, we update them to include ALL columns from
/// the new input (including any new extracted expression columns that were merged).
fn rebuild_path(path: Vec<LogicalPlan>, new_bottom: LogicalPlan) -> Result<LogicalPlan> {
    let mut current = new_bottom;

    // Rebuild path from bottom to top (reverse order)
    for node in path.into_iter().rev() {
        current = match node {
            LogicalPlan::Filter(f) => {
                LogicalPlan::Filter(Filter::try_new(f.predicate, Arc::new(current))?)
            }
            LogicalPlan::Sort(s) => LogicalPlan::Sort(Sort {
                expr: s.expr,
                input: Arc::new(current),
                fetch: s.fetch,
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit {
                skip: l.skip,
                fetch: l.fetch,
                input: Arc::new(current),
            }),
            LogicalPlan::Projection(p) if is_passthrough_projection(&p) => {
                // For passthrough projections, include ALL columns from new input
                // This ensures new extracted expression columns flow through
                let new_exprs: Vec<Expr> = current
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
                LogicalPlan::Projection(Projection::try_new(
                    new_exprs,
                    Arc::new(current),
                )?)
            }
            LogicalPlan::Projection(p) => {
                LogicalPlan::Projection(Projection::try_new(p.expr, Arc::new(current))?)
            }
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

    /// Builds projection with extracted expressions + ALL input columns (CSE-style).
    ///
    /// Passes through ALL columns from the input schema. This ensures nothing
    /// gets lost during optimizer merges and produces a stable 2-level structure.
    fn build_projection_with_all_columns(
        &self,
        input: Arc<LogicalPlan>,
    ) -> Result<Projection> {
        let mut proj_exprs = Vec::new();

        // 1. Add extracted expressions with their aliases
        for (_, (expr, alias)) in &self.extracted {
            proj_exprs.push(expr.clone().alias(alias));
        }

        // 2. Add ALL columns from input schema (not just columns_needed)
        for (qualifier, field) in self.input_schema.iter() {
            proj_exprs.push(Expr::from((qualifier, field)));
        }

        Projection::try_new(proj_exprs, input)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
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

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(ExtractLeafExpressions::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan, @ $expected,)
        }};
    }

    #[test]
    fn test_extract_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .build()?;

        // Note: An outer projection is added to preserve the original schema
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
              TableScan: test
        "#)
    }

    #[test]
    fn test_no_extraction_for_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1)))?
            .build()?;

        // No extraction should happen for simple columns
        assert_optimized_plan_equal!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test
        ")
    }

    #[test]
    fn test_extract_from_projection() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        // Projection expressions with MoveTowardsLeafNodes are extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
            TableScan: test
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

        // The mock_leaf sub-expression is extracted
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 IS NOT NULL AS has_name
          Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
            TableScan: test
        "#)
    }

    #[test]
    fn test_projection_no_extraction_for_column() -> Result<()> {
        // Projections with only columns don't need extraction
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        // No extraction needed
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.b
          TableScan: test
        ")
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

        // Same expression should be extracted only once
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 IS NOT NULL AND __datafusion_extracted_1 IS NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test
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

        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_1 = Utf8("test")
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_group_by() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![mock_leaf(col("user"), "status")], vec![count(lit(1))])?
            .build()?;

        // Group-by expression is MoveTowardsLeafNodes, so it gets extracted
        // With NamePreserver, names are preserved directly on the aggregate
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[__datafusion_extracted_1 AS mock_leaf(test.user,Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
            TableScan: test
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

        // Aggregate argument is MoveTowardsLeafNodes, so it gets extracted
        // With NamePreserver, names are preserved directly on the aggregate
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1) AS COUNT(mock_leaf(test.user,Utf8("value")))]]
          Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
            TableScan: test
        "#)
    }

    #[test]
    fn test_projection_with_filter_combined() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .project(vec![mock_leaf(col("user"), "name")])?
            .build()?;

        // Both filter and projection extractions.
        // BottomUp order: Filter is processed first (gets __datafusion_extracted_1),
        // then Projection merges its extraction into the same extracted projection (gets __datafusion_extracted_2).
        // Both extractions end up in a single projection above the TableScan.
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_2 AS mock_leaf(test.user,Utf8("name"))
          Projection: __datafusion_extracted_1, test.user, __datafusion_extracted_2
            Filter: __datafusion_extracted_1 = Utf8("active")
              Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_2
                TableScan: test
        "#)
    }

    #[test]
    fn test_projection_preserves_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name").alias("username")])?
            .build()?;

        // Original alias "username" should be preserved in outer projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS username
          Projection: mock_leaf(test.user, Utf8("name")) AS username AS __datafusion_extracted_1, test.user
            TableScan: test
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

        // BottomUp should merge both extractions into a single projection above TableScan.
        // Filter's s['value'] -> __datafusion_extracted_1
        // Projection's s['label'] -> __datafusion_extracted_2
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user, __datafusion_extracted_2 AS mock_leaf(test.user,Utf8("label"))
          Projection: __datafusion_extracted_1, test.user, __datafusion_extracted_2
            Filter: __datafusion_extracted_1 > Int32(150)
              Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("label")) AS __datafusion_extracted_2
                TableScan: test
        "#)
    }

    #[test]
    fn test_projection_deduplication() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let field = mock_leaf(col("user"), "name");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![field.clone(), field.clone().alias("name2")])?
            .build()?;

        // Same expression should be extracted only once
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name")), __datafusion_extracted_2 AS name2
          Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("name")) AS name2 AS __datafusion_extracted_2, test.user
            TableScan: test
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

        // Extraction projection should be placed below the Sort
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test
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

        // Extraction projection should be placed below the Limit
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Limit: skip=0, fetch=10
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test
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

        // The aliased aggregate should have its inner expression extracted
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1) AS cnt]]
          Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
            TableScan: test
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

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test
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

        // Should return unchanged because projection already contains extracted expressions
        assert_optimized_plan_equal!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test
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

        // Both extractions should end up in a single extracted expression projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.user
          Filter: __datafusion_extracted_2 IS NOT NULL
            Projection: __datafusion_extracted_1, test.user, __datafusion_extracted_2
              Filter: __datafusion_extracted_1 = Utf8("active")
                Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_2
                  TableScan: test
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

        // Extraction should push through the passthrough projection
        assert_optimized_plan_equal!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Projection: __datafusion_extracted_1, test.user
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test
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

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test
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

        // Should return unchanged (no extraction needed)
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test
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

        // Both extractions should be in a single extracted projection
        assert_optimized_plan_equal!(plan, @r#"
        Aggregate: groupBy=[[__datafusion_extracted_2 AS mock_leaf(test.user,Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Projection: __datafusion_extracted_1, test.user, __datafusion_extracted_2
            Filter: __datafusion_extracted_1 = Utf8("active")
              Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_2
                TableScan: test
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

        // Both extractions should be in a single extracted projection,
        // with both 'a' and 'b' columns passed through
        assert_optimized_plan_equal!(plan, @r#"
        Projection: test.a, test.b, test.c
          Filter: __datafusion_extracted_2 = Int32(2)
            Projection: __datafusion_extracted_1, test.a, test.b, test.c, __datafusion_extracted_2
              Filter: __datafusion_extracted_1 = Int32(1)
                Projection: mock_leaf(test.a, Utf8("x")) AS __datafusion_extracted_1, test.a, test.b, test.c, mock_leaf(test.b, Utf8("y")) AS __datafusion_extracted_2
                  TableScan: test
        "#)
    }
}
