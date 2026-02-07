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

use indexmap::{IndexMap, IndexSet};
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, qualified_name};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, ExpressionPlacement, Projection};

use crate::optimizer::ApplyOrder;
use crate::push_down_filter::replace_cols_by_name;
use crate::utils::{EXTRACTED_EXPR_PREFIX, has_all_column_refs};
use crate::{OptimizerConfig, OptimizerRule};

/// Extracts `MoveTowardsLeafNodes` sub-expressions from non-projection nodes into projections.
///
/// This handles Filter, Sort, Limit, Aggregate, and Join nodes. For Projection nodes,
/// extraction and pushdown are handled by [`PushDownLeafProjections`].
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
/// Works for any number of inputs (0, 1, 2, …N). For multi-input nodes
/// like Join, each extracted sub-expression is routed to the correct input
/// by checking which input's schema contains all of the expression's column
/// references.
fn extract_from_plan(
    plan: LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    // Only extract from plan types whose output schema is predictable after
    // expression rewriting.  Nodes like Window derive column names from
    // their expressions, so rewriting `get_field` inside a window function
    // changes the output schema and breaks the recovery projection.
    if !matches!(
        &plan,
        LogicalPlan::Aggregate(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Join(_)
    ) {
        return Ok(Transformed::no(plan));
    }

    let inputs = plan.inputs();
    if inputs.is_empty() {
        return Ok(Transformed::no(plan));
    }

    // Save original output schema before any transformation
    let original_schema = Arc::clone(plan.schema());

    // Clone inputs upfront (before plan is consumed by map_expressions)
    let owned_inputs: Vec<LogicalPlan> = inputs.into_iter().cloned().collect();

    // Build per-input schemas (kept alive for extractor borrows)
    let input_schemas: Vec<Arc<DFSchema>> = owned_inputs
        .iter()
        .map(|i| Arc::clone(i.schema()))
        .collect();

    // Build per-input extractors
    let mut extractors: Vec<LeafExpressionExtractor> = input_schemas
        .iter()
        .map(|schema| LeafExpressionExtractor::new(schema.as_ref(), alias_generator))
        .collect();

    // Build per-input column sets for routing expressions to the correct input
    let input_column_sets: Vec<std::collections::HashSet<Column>> = input_schemas
        .iter()
        .map(|schema| schema_columns(schema.as_ref()))
        .collect();

    // Transform expressions via map_expressions with routing
    let transformed = plan.map_expressions(|expr| {
        routing_extract(expr, &mut extractors, &input_column_sets)
    })?;

    // Check if any extractor has extractions
    let any_extracted = extractors.iter().any(|e| e.has_extractions());
    if !any_extracted {
        assert!(!transformed.transformed);
        return Ok(transformed);
    }

    // Build per-input extraction projections
    let new_inputs: Vec<LogicalPlan> = owned_inputs
        .iter()
        .zip(extractors.iter())
        .map(|(input, extractor)| {
            if extractor.has_extractions() {
                let input_arc = Arc::new(input.clone());
                extractor.build_extraction_projection(&input_arc)
            } else {
                Ok(input.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Rebuild and add recovery projection if schema changed
    let new_plan = transformed
        .data
        .with_new_exprs(transformed.data.expressions(), new_inputs)?;

    // Add recovery projection if the output schema changed
    let recovered = build_recovery_projection(original_schema.as_ref(), new_plan)?;

    Ok(Transformed::yes(recovered))
}

/// Given an expression, returns the index of the input whose columns fully
/// cover the expression's column references.
/// Returns `None` if the expression references columns from multiple inputs.
fn find_owning_input(
    expr: &Expr,
    input_column_sets: &[std::collections::HashSet<Column>],
) -> Option<usize> {
    input_column_sets
        .iter()
        .position(|cols| has_all_column_refs(expr, cols))
}

/// Walks an expression tree top-down, extracting `MoveTowardsLeafNodes`
/// sub-expressions and routing each to the correct per-input extractor.
fn routing_extract(
    expr: Expr,
    extractors: &mut [LeafExpressionExtractor],
    input_column_sets: &[std::collections::HashSet<Column>],
) -> Result<Transformed<Expr>> {
    expr.transform_down(|e| {
        // Skip expressions already aliased with extracted expression pattern
        if let Expr::Alias(alias) = &e
            && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
        {
            return Ok(Transformed {
                data: e,
                transformed: false,
                tnr: TreeNodeRecursion::Jump,
            });
        }

        // Don't extract Alias nodes directly — preserve the alias and let
        // transform_down recurse into the inner expression
        if matches!(&e, Expr::Alias(_)) {
            return Ok(Transformed::no(e));
        }

        match e.placement() {
            ExpressionPlacement::MoveTowardsLeafNodes => {
                if let Some(idx) = find_owning_input(&e, input_column_sets) {
                    let col_ref = extractors[idx].add_extracted(e)?;
                    Ok(Transformed::yes(col_ref))
                } else {
                    // References columns from multiple inputs — cannot extract
                    Ok(Transformed::no(e))
                }
            }
            ExpressionPlacement::Column => {
                if let Expr::Column(col) = &e
                    && let Some(idx) = find_owning_input(&e, input_column_sets)
                {
                    extractors[idx].columns_needed.insert(col.clone());
                }
                Ok(Transformed::no(e))
            }
            _ => Ok(Transformed::no(e)),
        }
    })
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

/// Build a recovery projection to restore the original output schema.
///
/// Handles two cases:
/// - **Schema-preserving nodes** (Filter/Sort/Limit): new schema has extra extraction
///   columns. Original columns still exist by name → select them to hide extras.
/// - **Schema-defining nodes** (Projection/Aggregate): same number of columns but
///   names may differ. Map positionally, aliasing where names changed.
/// - **Schemas identical** → no recovery projection needed.
fn build_recovery_projection(
    original_schema: &DFSchema,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let new_schema = input.schema();
    let orig_len = original_schema.fields().len();
    let new_len = new_schema.fields().len();

    if orig_len == new_len {
        // Same number of fields — check if schemas are identical
        let schemas_match = original_schema.iter().zip(new_schema.iter()).all(
            |((orig_q, orig_f), (new_q, new_f))| {
                orig_f.name() == new_f.name() && orig_q == new_q
            },
        );
        if schemas_match {
            return Ok(input);
        }

        // Schema-defining nodes (Projection, Aggregate): names may differ at some positions.
        // Map positionally, aliasing where the name changed.
        let mut proj_exprs = Vec::with_capacity(orig_len);
        for (i, (orig_qualifier, orig_field)) in original_schema.iter().enumerate() {
            let (new_qualifier, new_field) = new_schema.qualified_field(i);
            if orig_field.name() == new_field.name() && orig_qualifier == new_qualifier {
                proj_exprs.push(Expr::from((orig_qualifier, orig_field)));
            } else {
                let new_col = Expr::Column(Column::from((new_qualifier, new_field)));
                proj_exprs.push(
                    new_col.alias_qualified(orig_qualifier.cloned(), orig_field.name()),
                );
            }
        }
        let projection = Projection::try_new(proj_exprs, Arc::new(input))?;
        Ok(LogicalPlan::Projection(projection))
    } else {
        // Schema-preserving nodes: new schema has extra extraction columns.
        // Original columns still exist by name; select them to hide extras.
        let col_exprs: Vec<Expr> = original_schema.iter().map(Expr::from).collect();
        let projection = Projection::try_new(col_exprs, Arc::new(input))?;
        Ok(LogicalPlan::Projection(projection))
    }
}

/// Extracts `MoveTowardsLeafNodes` sub-expressions from larger expressions.
struct LeafExpressionExtractor<'a> {
    /// Extracted expressions: maps expression -> alias
    extracted: IndexMap<Expr, String>,
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

    /// Adds an expression to extracted set, returns column reference.
    fn add_extracted(&mut self, expr: Expr) -> Result<Expr> {
        // Deduplication: reuse existing alias if same expression
        if let Some(alias) = self.extracted.get(&expr) {
            return Ok(Expr::Column(Column::new_unqualified(alias)));
        }

        // Track columns referenced by this expression
        for col in expr.column_refs() {
            self.columns_needed.insert(col.clone());
        }

        // Generate unique alias
        let alias = self.alias_generator.next(EXTRACTED_EXPR_PREFIX);
        self.extracted.insert(expr, alias.clone());

        Ok(Expr::Column(Column::new_unqualified(&alias)))
    }

    fn has_extractions(&self) -> bool {
        !self.extracted.is_empty()
    }

    /// Builds a fresh extraction projection above the given input.
    ///
    /// Creates a new projection that includes extracted expressions (aliased)
    /// plus all input schema columns for pass-through.
    fn build_extraction_projection(
        &self,
        input: &Arc<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let mut proj_exprs = Vec::new();
        for (expr, alias) in self.extracted.iter() {
            proj_exprs.push(expr.clone().alias(alias));
        }
        for (qualifier, field) in self.input_schema.iter() {
            proj_exprs.push(Expr::from((qualifier, field)));
        }
        Ok(LogicalPlan::Projection(Projection::try_new(
            proj_exprs,
            Arc::clone(input),
        )?))
    }
}

/// Build an extraction projection above the target node.
///
/// If the target is an existing projection, merges into it (dedup by resolved
/// expression equality, resolve columns through rename mapping, add pass-through
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

        // Build a map of existing expressions (by Expr equality) to their aliases
        let existing_extractions: IndexMap<Expr, String> = existing
            .expr
            .iter()
            .filter_map(|e| {
                if let Expr::Alias(alias) = e
                    && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
                {
                    return Some((*alias.expr.clone(), alias.name.clone()));
                }
                None
            })
            .collect();

        // Resolve column references through the projection's rename mapping
        let replace_map = build_projection_replace_map(existing);

        // Add new extracted expressions, resolving column refs through the projection
        for (expr, alias) in extracted_exprs {
            let resolved = replace_cols_by_name(expr.clone().alias(alias), &replace_map)?;
            let resolved_inner = if let Expr::Alias(a) = &resolved {
                a.expr.as_ref()
            } else {
                &resolved
            };
            if let Some(existing_alias) = existing_extractions.get(resolved_inner) {
                // Same expression already extracted under a different alias —
                // add the expression with the new alias so both names are
                // available in the output. We can't reference the existing alias
                // as a column within the same projection, so we duplicate the
                // computation.
                if existing_alias != alias {
                    proj_exprs.push(resolved);
                }
            } else {
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
            if let Expr::Column(resolved_col) = &resolved
                && !existing_cols.contains(resolved_col)
                && input_schema.has_column(resolved_col)
            {
                proj_exprs.push(Expr::Column(resolved_col.clone()));
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

// =============================================================================
// Pass 2: PushDownLeafProjections
// =============================================================================

/// Pushes extraction projections down through schema-preserving nodes towards leaf nodes.
///
/// Handles two types of projections:
/// - **Pure extraction projections** (all `__datafusion_extracted` aliases + columns):
///   pushes through Filter/Sort/Limit, merges into existing projections, or routes
///   into multi-input node inputs (Join, SubqueryAlias, etc.)
/// - **Mixed projections** (containing `MoveTowardsLeafNodes` sub-expressions):
///   splits into a recovery projection + pure extraction projection, then recursively
///   pushes the extraction projection down.
///
/// This is the second pass of a two-pass extraction pipeline:
/// 1. [`ExtractLeafExpressions`] extracts sub-expressions from non-projection nodes
/// 2. [`PushDownLeafProjections`] handles projection splitting/pushing and pushes
///    extraction projections down through schema-preserving nodes
#[derive(Default, Debug)]
pub struct PushDownLeafProjections {}

impl PushDownLeafProjections {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownLeafProjections {
    fn name(&self) -> &str {
        "push_down_leaf_projections"
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
        match try_push_input(&plan, alias_generator)? {
            Some(new_plan) => Ok(Transformed::yes(new_plan)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// Attempts to push a projection's extractable expressions further down.
///
/// Returns `Some(new_subtree)` if the projection was pushed down or merged,
/// `None` if there is nothing to push or the projection sits above a barrier.
fn try_push_input(
    input: &LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(proj) = input else {
        return Ok(None);
    };
    split_and_push_projection(proj, alias_generator)
}

/// Unified function that splits a projection into extractable pieces, pushes
/// them towards leaf nodes, and adds a recovery projection if needed.
///
/// Handles both:
/// - **Pure extraction projections** (all `__extracted` aliases + columns)
/// - **Mixed projections** (containing `MoveTowardsLeafNodes` sub-expressions)
///
/// Returns `Some(new_subtree)` if extractions were pushed down,
/// `None` if there is nothing to extract or push.
fn split_and_push_projection(
    proj: &Projection,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Option<LogicalPlan>> {
    let input = &proj.input;
    let input_schema = input.schema();

    // ── Phase 1: Split ──────────────────────────────────────────────────
    // For each projection expression, collect extraction pairs and build
    // recovery expressions.

    // Manual pairs/columns from __extracted aliases (pre-handled before routing_extract)
    let mut manual_pairs: Vec<(Expr, String)> = Vec::new();
    let mut manual_columns: IndexSet<Column> = IndexSet::new();

    // Extractor for everything else (via routing_extract)
    let mut extractors = vec![LeafExpressionExtractor::new(
        input_schema.as_ref(),
        alias_generator,
    )];
    let input_column_sets = vec![schema_columns(input_schema.as_ref())];

    let original_schema = proj.schema.as_ref();
    let mut recovery_exprs: Vec<Expr> = Vec::with_capacity(proj.expr.len());
    let mut needs_recovery = false;

    for (expr, (qualifier, field)) in proj.expr.iter().zip(original_schema.iter()) {
        if let Expr::Alias(alias) = expr
            && alias.name.starts_with(EXTRACTED_EXPR_PREFIX)
        {
            // Pre-handle __extracted aliases: add inner expr to manual pairs,
            // recovery just references the extracted alias as a column.
            let inner = *alias.expr.clone();
            let alias_name = alias.name.clone();

            // Track columns referenced by the inner expression
            for col_ref in inner.column_refs() {
                manual_columns.insert(col_ref.clone());
            }

            manual_pairs.push((inner, alias_name.clone()));
            recovery_exprs.push(Expr::Column(Column::new_unqualified(&alias_name)));
        } else if let Expr::Column(col) = expr {
            // Plain column pass-through — track it and use as-is for recovery
            manual_columns.insert(col.clone());
            recovery_exprs.push(expr.clone());
        } else {
            // Everything else: run through routing_extract
            let transformed =
                routing_extract(expr.clone(), &mut extractors, &input_column_sets)?;
            let transformed_expr = transformed.data;

            // Build recovery expression, aliasing back to original name if needed
            let original_name = field.name();
            let needs_alias = if let Expr::Column(col) = &transformed_expr {
                col.name.as_str() != original_name
            } else {
                let expr_name = transformed_expr.schema_name().to_string();
                original_name != &expr_name
            };
            let recovery_expr = if needs_alias {
                needs_recovery = true;
                transformed_expr
                    .clone()
                    .alias_qualified(qualifier.cloned(), original_name)
            } else {
                transformed_expr.clone()
            };

            // If the expression was transformed (i.e., has extracted sub-parts),
            // it differs from what the pushed projection outputs → needs recovery.
            // Also, any non-column, non-__extracted expression needs recovery
            // because the pushed extraction projection won't output it directly.
            if transformed.transformed || !matches!(expr, Expr::Column(_)) {
                needs_recovery = true;
            }

            recovery_exprs.push(recovery_expr);
        }
    }

    // Merge manual pairs/columns with extractor's pairs/columns
    let extractor = &extractors[0];
    let mut pairs: Vec<(Expr, String)> = manual_pairs;
    let mut columns_needed: IndexSet<Column> = manual_columns;

    for (expr, alias) in extractor.extracted.iter() {
        pairs.push((expr.clone(), alias.clone()));
    }
    for col in &extractor.columns_needed {
        columns_needed.insert(col.clone());
    }

    // If no extractions found, nothing to do
    if pairs.is_empty() {
        return Ok(None);
    }

    // ── Phase 2: Push down ──────────────────────────────────────────────
    let proj_input = Arc::clone(&proj.input);
    let pushed = push_extraction_pairs(
        &pairs,
        &columns_needed,
        proj,
        &proj_input,
        alias_generator,
    )?;

    // ── Phase 3: Recovery ───────────────────────────────────────────────
    match (pushed, needs_recovery) {
        (Some(pushed_plan), true) => {
            // Wrap with recovery projection
            let recovery = LogicalPlan::Projection(Projection::try_new(
                recovery_exprs,
                Arc::new(pushed_plan),
            )?);
            Ok(Some(recovery))
        }
        (Some(pushed_plan), false) => {
            // No recovery needed (pure extraction projection)
            Ok(Some(pushed_plan))
        }
        (None, true) => {
            // Push returned None but we still have extractions to apply.
            // Build the extraction projection in-place (not pushed) using
            // ALL pairs (manual + extractor) so the recovery can resolve
            // both __extracted aliases and newly extracted expressions.
            if !extractor.has_extractions() {
                // Only manual pairs (all __extracted + columns) but push failed.
                // The original projection is already an extraction projection,
                // and we couldn't push it further. Return None.
                return Ok(None);
            }
            let input_arc = Arc::clone(input);
            let extraction = build_extraction_projection_impl(
                &pairs,
                &columns_needed,
                &input_arc,
                input_schema.as_ref(),
            )?;
            let extraction_plan = LogicalPlan::Projection(extraction);
            let recovery = LogicalPlan::Projection(Projection::try_new(
                recovery_exprs,
                Arc::new(extraction_plan),
            )?);
            Ok(Some(recovery))
        }
        (None, false) => {
            // No extractions could be pushed and no recovery needed
            Ok(None)
        }
    }
}

/// Returns true if the plan is a Projection where ALL expressions are either
/// `Alias(EXTRACTED_EXPR_PREFIX, ...)` or `Column`, with at least one extraction.
/// Such projections can safely be pushed further without re-extraction.
fn is_pure_extraction_projection(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Projection(proj) = plan else {
        return false;
    };
    let mut has_extraction = false;
    for expr in &proj.expr {
        match expr {
            Expr::Alias(alias) if alias.name.starts_with(EXTRACTED_EXPR_PREFIX) => {
                has_extraction = true;
            }
            Expr::Column(_) => {}
            _ => return false,
        }
    }
    has_extraction
}

/// Pushes extraction pairs down through the projection's input node.
///
/// This contains the match arms from the former `try_push_pure_extraction`,
/// dispatching to the appropriate handler based on the input node type.
fn push_extraction_pairs(
    pairs: &[(Expr, String)],
    columns_needed: &IndexSet<Column>,
    proj: &Projection,
    proj_input: &Arc<LogicalPlan>,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Option<LogicalPlan>> {
    match proj_input.as_ref() {
        // Merge into existing projection, then try to push the result further down.
        // Only merge when all outer expressions are captured (pairs + columns).
        // Uncaptured expressions (e.g. `col AS __common_expr_1`) would be lost
        // during the merge since build_extraction_projection_impl only knows
        // about the captured pairs and columns.
        LogicalPlan::Projection(_)
            if pairs.len() + columns_needed.len() == proj.expr.len() =>
        {
            let target_schema = Arc::clone(proj_input.schema());
            let merged = build_extraction_projection_impl(
                pairs,
                columns_needed,
                proj_input,
                target_schema.as_ref(),
            )?;
            let merged_plan = LogicalPlan::Projection(merged);

            // After merging, try to push the result further down, but ONLY
            // if the merged result is still a pure extraction projection
            // (all __extracted aliases + columns). If the merge inherited
            // bare MoveTowardsLeafNodes expressions from the inner projection,
            // pushing would re-extract them into new aliases and fail when
            // the (None, true) fallback can't find the original aliases.
            // This handles: Extraction → Recovery(cols) → Filter → ... → TableScan
            // by pushing through the recovery projection AND the filter in one pass.
            if is_pure_extraction_projection(&merged_plan)
                && let Some(pushed) = try_push_input(&merged_plan, alias_generator)?
            {
                return Ok(Some(pushed));
            }
            Ok(Some(merged_plan))
        }
        // Generic: handles Filter/Sort/Limit (via recursion),
        // SubqueryAlias (with qualifier remap in try_push_into_inputs),
        // Join, and anything else.
        // Safely bails out for nodes that don't pass through extracted
        // columns (Aggregate, Window) via the output schema check.
        _ => try_push_into_inputs(
            pairs,
            columns_needed,
            proj_input.as_ref(),
            alias_generator,
        ),
    }
}

/// Pushes extraction expressions into a node's inputs by routing each
/// expression to the input that owns all of its column references.
///
/// Works for any number of inputs (1, 2, …N). For single-input nodes,
/// all expressions trivially route to that input. For multi-input nodes
/// (Join, etc.), each expression is routed to the side that owns its columns.
///
/// Returns `Some(new_node)` if all expressions could be routed AND the
/// rebuilt node's output schema contains all extracted aliases.
/// Returns `None` if any expression references columns from multiple inputs
/// or the node doesn't pass through the extracted columns.
fn try_push_into_inputs(
    pairs: &[(Expr, String)],
    columns_needed: &IndexSet<Column>,
    node: &LogicalPlan,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Option<LogicalPlan>> {
    let inputs = node.inputs();
    if inputs.is_empty() {
        return Ok(None);
    }

    // SubqueryAlias remaps qualifiers between input and output.
    // Rewrite pairs/columns from alias-space to input-space before routing.
    let (pairs, columns_needed) = if let LogicalPlan::SubqueryAlias(sa) = node {
        let mut replace_map = HashMap::new();
        for ((input_q, input_f), (alias_q, alias_f)) in
            sa.input.schema().iter().zip(sa.schema.iter())
        {
            replace_map.insert(
                qualified_name(alias_q, alias_f.name()),
                Expr::Column(Column::new(input_q.cloned(), input_f.name())),
            );
        }
        let remapped_pairs: Vec<(Expr, String)> = pairs
            .iter()
            .map(|(expr, alias)| {
                Ok((
                    replace_cols_by_name(expr.clone(), &replace_map)?,
                    alias.clone(),
                ))
            })
            .collect::<Result<_>>()?;
        let remapped_columns: IndexSet<Column> = columns_needed
            .iter()
            .filter_map(|col| {
                let rewritten =
                    replace_cols_by_name(Expr::Column(col.clone()), &replace_map).ok()?;
                if let Expr::Column(c) = rewritten {
                    Some(c)
                } else {
                    Some(col.clone())
                }
            })
            .collect();
        (remapped_pairs, remapped_columns)
    } else {
        (pairs.to_vec(), columns_needed.clone())
    };
    let pairs = &pairs[..];
    let columns_needed = &columns_needed;

    let num_inputs = inputs.len();

    // Build per-input column sets using existing schema_columns()
    let input_schemas: Vec<Arc<DFSchema>> =
        inputs.iter().map(|i| Arc::clone(i.schema())).collect();
    let input_column_sets: Vec<std::collections::HashSet<Column>> =
        input_schemas.iter().map(|s| schema_columns(s)).collect();

    // Route pairs and columns to inputs.
    // Union: all inputs share the same schema, so broadcast to every branch.
    // Everything else (Join, single-input nodes): columns are disjoint across
    // inputs, so route each expression to its owning input.
    let broadcast = matches!(node, LogicalPlan::Union(_));

    let mut per_input_pairs: Vec<Vec<(Expr, String)>> = vec![vec![]; num_inputs];
    let mut per_input_columns: Vec<IndexSet<Column>> = vec![IndexSet::new(); num_inputs];

    if broadcast {
        // Union output schema and each input schema have the same fields by
        // index but may differ in qualifiers (e.g. output `s` vs input
        // `simple_struct.s`). Remap pairs/columns to each input's space.
        let union_schema = node.schema();
        for (idx, input_schema) in input_schemas.iter().enumerate() {
            let mut remap = HashMap::new();
            for ((out_q, out_f), (in_q, in_f)) in
                union_schema.iter().zip(input_schema.iter())
            {
                remap.insert(
                    qualified_name(out_q, out_f.name()),
                    Expr::Column(Column::new(in_q.cloned(), in_f.name())),
                );
            }
            per_input_pairs[idx] = pairs
                .iter()
                .map(|(expr, alias)| {
                    Ok((replace_cols_by_name(expr.clone(), &remap)?, alias.clone()))
                })
                .collect::<Result<_>>()?;
            per_input_columns[idx] = columns_needed
                .iter()
                .filter_map(|col| {
                    let rewritten =
                        replace_cols_by_name(Expr::Column(col.clone()), &remap).ok()?;
                    if let Expr::Column(c) = rewritten {
                        Some(c)
                    } else {
                        Some(col.clone())
                    }
                })
                .collect();
        }
    } else {
        for (expr, alias) in pairs {
            match find_owning_input(expr, &input_column_sets) {
                Some(idx) => per_input_pairs[idx].push((expr.clone(), alias.clone())),
                None => return Ok(None), // Cross-input expression — bail out
            }
        }
        for col in columns_needed {
            let col_expr = Expr::Column(col.clone());
            match find_owning_input(&col_expr, &input_column_sets) {
                Some(idx) => {
                    per_input_columns[idx].insert(col.clone());
                }
                None => return Ok(None), // Ambiguous column — bail out
            }
        }
    }

    // Check at least one input has extractions to push
    if per_input_pairs.iter().all(|p| p.is_empty()) {
        return Ok(None);
    }

    // Build per-input extraction projections and push them as far as possible
    // immediately. This is critical because map_children preserves cached schemas,
    // so if the TopDown pass later pushes a child further (changing its output
    // schema), the parent node's schema becomes stale.
    let mut new_inputs: Vec<LogicalPlan> = Vec::with_capacity(num_inputs);
    for (idx, input) in inputs.into_iter().enumerate() {
        if per_input_pairs[idx].is_empty() {
            new_inputs.push(input.clone());
        } else {
            let input_arc = Arc::new(input.clone());
            let target_schema = Arc::clone(input.schema());
            let proj = build_extraction_projection_impl(
                &per_input_pairs[idx],
                &per_input_columns[idx],
                &input_arc,
                target_schema.as_ref(),
            )?;
            // Verify all requested aliases appear in the projection's output.
            // A merge may deduplicate if the same expression already exists
            // under a different alias, leaving the requested alias missing.
            let proj_schema = proj.schema.as_ref();
            for (_expr, alias) in &per_input_pairs[idx] {
                if !proj_schema.fields().iter().any(|f| f.name() == alias) {
                    return Ok(None);
                }
            }
            let proj_plan = LogicalPlan::Projection(proj);
            // Try to push the extraction projection further down within
            // this input (e.g., through Filter → existing extraction projection).
            // This ensures the input's output schema is stable and won't change
            // when the TopDown pass later visits children.
            match try_push_input(&proj_plan, alias_generator)? {
                Some(pushed) => new_inputs.push(pushed),
                None => new_inputs.push(proj_plan),
            }
        }
    }

    // Rebuild the node with new inputs
    let new_node = node.with_new_exprs(node.expressions(), new_inputs)?;

    // Safety check: verify all extracted aliases appear in the rebuilt
    // node's output schema. Nodes like Aggregate define their own output
    // and won't pass through extracted columns — bail out for those.
    let output_schema = new_node.schema();
    for (_expr, alias) in pairs {
        if !output_schema.fields().iter().any(|f| f.name() == alias) {
            return Ok(None);
        }
    }

    Ok(Some(new_node))
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

    // =========================================================================
    // Test assertion macros - 4 stages of the optimization pipeline
    // All stages run OptimizeProjections first to match the actual rule layout.
    // =========================================================================

    /// Stage 1: Original plan with OptimizeProjections (baseline without extraction).
    /// This shows the plan as it would be without our extraction rules.
    macro_rules! assert_original_plan {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(OptimizeProjections::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 2: OptimizeProjections + ExtractLeafExpressions (shows extraction projections).
    macro_rules! assert_after_extract {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 3: OptimizeProjections + Extract + PushDown (extraction pushed through schema-preserving nodes).
    macro_rules! assert_after_pushdown {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
                Arc::new(PushDownLeafProjections::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    /// Stage 4: Full pipeline - OptimizeProjections + Extract + PushDown + OptimizeProjections (final).
    macro_rules! assert_optimized {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(OptimizeProjections::new()),
                Arc::new(ExtractLeafExpressions::new()),
                Arc::new(PushDownLeafProjections::new()),
                Arc::new(OptimizeProjections::new()),
            ];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan.clone(), @ $expected,)
        }};
    }

    #[test]
    fn test_extract_from_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(mock_leaf(col("user"), "status").eq(lit("active")))?
            .select(vec![
                table_scan
                    .schema()
                    .index_of_column_by_name(None, "id")
                    .unwrap(),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: test.id
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id
          Projection: test.id, test.user
            Filter: __datafusion_extracted_1 = Utf8("active")
              Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user
                TableScan: test projection=[id, user]
        "#)?;

        // Note: An outer projection is added to preserve the original schema
        assert_optimized!(plan, @r#"
        Projection: test.id
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id
              TableScan: test projection=[id, user]
        "#)
    }

    #[test]
    fn test_no_extraction_for_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").eq(lit(1)))?
            .build()?;

        assert_original_plan!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")?;

        assert_after_extract!(plan, @r"
        Filter: test.a = Int32(1)
          TableScan: test projection=[a, b, c]
        ")?;

        // No extraction should happen for simple columns
        assert_optimized!(plan, @r"
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Projection expressions with MoveTowardsLeafNodes are extracted
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS mock_leaf(test.user,Utf8("name"))
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) IS NOT NULL AS has_name
          TableScan: test projection=[user]
        "#)?;

        // The mock_leaf sub-expression is extracted
        assert_optimized!(plan, @r#"
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

        assert_original_plan!(plan, @"TableScan: test projection=[a, b]")?;

        assert_after_extract!(plan, @"TableScan: test projection=[a, b]")?;

        // No extraction needed
        assert_optimized!(plan, @"TableScan: test projection=[a, b]")
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

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL AND mock_leaf(test.user, Utf8("name")) IS NULL
          TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 IS NOT NULL AND __datafusion_extracted_1 IS NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 IS NOT NULL AND __datafusion_extracted_1 IS NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
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

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) = Utf8("test")
          TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 = Utf8("test")
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
        "#)?;

        assert_optimized!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 = Utf8("test")
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
        "#)
    }

    #[test]
    fn test_extract_from_aggregate_group_by() -> Result<()> {
        use datafusion_expr::test::function_stub::count;

        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![mock_leaf(col("user"), "status")], vec![count(lit(1))])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("status"))]], aggr=[[COUNT(Int32(1))]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("status")), COUNT(Int32(1))
          Aggregate: groupBy=[[__datafusion_extracted_1]], aggr=[[COUNT(Int32(1))]]
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)?;

        // Group-by expression is MoveTowardsLeafNodes, so it gets extracted
        // Recovery projection restores original schema on top
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("status")), COUNT(Int32(1))
          Aggregate: groupBy=[[__datafusion_extracted_1]], aggr=[[COUNT(Int32(1))]]
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

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value")))]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.user, COUNT(__datafusion_extracted_1) AS COUNT(mock_leaf(test.user,Utf8("value")))
          Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1)]]
            Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)?;

        // Aggregate argument is MoveTowardsLeafNodes, so it gets extracted
        // Recovery projection restores original schema on top
        assert_optimized!(plan, @r#"
        Projection: test.user, COUNT(__datafusion_extracted_1) AS COUNT(mock_leaf(test.user,Utf8("value")))
          Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1)]]
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Projection: test.user
            Filter: __datafusion_extracted_1 = Utf8("active")
              Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.user
                TableScan: test projection=[user]
        "#)?;

        // Both filter and projection extractions are pushed to a single
        // extraction projection above the TableScan.
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_2 AS mock_leaf(test.user,Utf8("name"))
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_2
              TableScan: test projection=[user]
        "#)
    }

    #[test]
    fn test_projection_preserves_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![mock_leaf(col("user"), "name").alias("username")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS username
          TableScan: test projection=[user]
        "#)?;

        // Original alias "username" should be preserved in outer projection
        assert_optimized!(plan, @r#"
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

        assert_original_plan!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Filter: mock_leaf(test.user, Utf8("value")) > Int32(150)
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.user, mock_leaf(test.user, Utf8("label"))
          Projection: test.user
            Filter: __datafusion_extracted_1 > Int32(150)
              Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
                TableScan: test projection=[user]
        "#)?;

        // Both extractions merge into a single projection above TableScan.
        assert_optimized!(plan, @r#"
        Projection: test.user, __datafusion_extracted_2 AS mock_leaf(test.user,Utf8("label"))
          Filter: __datafusion_extracted_1 > Int32(150)
            Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user, mock_leaf(test.user, Utf8("label")) AS __datafusion_extracted_2
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
          TableScan: test projection=[user]
        "#)?;

        // Same expression should be extracted only once
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS mock_leaf(test.user,Utf8("name")), mock_leaf(test.user, Utf8("name")) AS name2
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

        // Stage 1: Baseline (no extraction rules)
        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Stage 2: After extraction - projection untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            TableScan: test projection=[user]
        "#)?;

        // Stage 3: After pushdown - extraction pushed through Sort
        assert_after_pushdown!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Sort: test.user ASC NULLS FIRST
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)?;

        // Stage 4: Final optimized - projection columns resolved
        assert_optimized!(plan, @r#"
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

        // Stage 1: Baseline (no extraction rules)
        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Stage 2: After extraction - projection untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          Limit: skip=0, fetch=10
            TableScan: test projection=[user]
        "#)?;

        // Stage 3: After pushdown - extraction pushed through Limit
        assert_after_pushdown!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name"))
          Limit: skip=0, fetch=10
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              TableScan: test projection=[user]
        "#)?;

        // Stage 4: Final optimized - projection columns resolved
        assert_optimized!(plan, @r#"
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

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(mock_leaf(test.user, Utf8("value"))) AS cnt]]
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Aggregate: groupBy=[[test.user]], aggr=[[COUNT(__datafusion_extracted_1) AS cnt]]
          Projection: mock_leaf(test.user, Utf8("value")) AS __datafusion_extracted_1, test.user
            TableScan: test projection=[user]
        "#)?;

        // The aliased aggregate should have its inner expression extracted
        assert_optimized!(plan, @r#"
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

        assert_original_plan!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[COUNT(test.b)]]
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_manual, test.user
          TableScan: test projection=[user]
        "#)?;

        // Should return unchanged because projection already contains extracted expressions
        assert_optimized!(plan, @r#"
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

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("name")) IS NOT NULL
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 IS NOT NULL
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.id, test.user
              Projection: test.id, test.user
                Filter: __datafusion_extracted_2 = Utf8("active")
                  Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2, test.id, test.user
                    TableScan: test projection=[id, user]
        "#)?;

        // Both extractions end up merged into a single extraction projection above the TableScan
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user
          Filter: __datafusion_extracted_1 IS NOT NULL
            Projection: test.id, test.user, __datafusion_extracted_1
              Filter: __datafusion_extracted_2 = Utf8("active")
                Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2, test.id, test.user, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1
                  TableScan: test projection=[id, user]
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

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name"))
          TableScan: test projection=[user]
        "#)?;

        // Extraction should push through the passthrough projection
        assert_optimized!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("name")) AS mock_leaf(test.user,Utf8("name"))
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

        assert_original_plan!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Projection: test.a AS x, test.b
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
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

        assert_original_plan!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        Projection: test.a + test.b AS sum
          TableScan: test projection=[a, b]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
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

        assert_original_plan!(plan, @r#"
        Aggregate: groupBy=[[mock_leaf(test.user, Utf8("name"))]], aggr=[[COUNT(Int32(1))]]
          Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name")), COUNT(Int32(1))
          Aggregate: groupBy=[[__datafusion_extracted_1]], aggr=[[COUNT(Int32(1))]]
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1, test.user
              Projection: test.user
                Filter: __datafusion_extracted_2 = Utf8("active")
                  Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2, test.user
                    TableScan: test projection=[user]
        "#)?;

        // Both extractions should be in a single extracted projection
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("name")), COUNT(Int32(1))
          Aggregate: groupBy=[[__datafusion_extracted_1]], aggr=[[COUNT(Int32(1))]]
            Projection: __datafusion_extracted_1
              Filter: __datafusion_extracted_2 = Utf8("active")
                Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_2, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1
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

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.b, Utf8("y")) = Int32(2)
          Filter: mock_leaf(test.a, Utf8("x")) = Int32(1)
            TableScan: test projection=[a, b, c]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.a, test.b, test.c
          Filter: __datafusion_extracted_1 = Int32(2)
            Projection: mock_leaf(test.b, Utf8("y")) AS __datafusion_extracted_1, test.a, test.b, test.c
              Projection: test.a, test.b, test.c
                Filter: __datafusion_extracted_2 = Int32(1)
                  Projection: mock_leaf(test.a, Utf8("x")) AS __datafusion_extracted_2, test.a, test.b, test.c
                    TableScan: test projection=[a, b, c]
        "#)?;

        // Both extractions should be in a single extracted projection,
        // with both 'a' and 'b' columns passed through
        assert_optimized!(plan, @r#"
        Projection: test.a, test.b, test.c
          Filter: __datafusion_extracted_1 = Int32(2)
            Projection: test.a, test.b, test.c, __datafusion_extracted_1
              Filter: __datafusion_extracted_2 = Int32(1)
                Projection: mock_leaf(test.a, Utf8("x")) AS __datafusion_extracted_2, test.a, test.b, test.c, mock_leaf(test.b, Utf8("y")) AS __datafusion_extracted_1
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

        assert_original_plan!(plan, @r#"
        Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join: __datafusion_extracted_1 = __datafusion_extracted_2
            Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_2, right.id, right.user
              TableScan: right projection=[id, user]
        "#)?;

        // Both left and right keys should be extracted into their respective sides
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join: __datafusion_extracted_1 = __datafusion_extracted_2
            Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_2, right.id, right.user
              TableScan: right projection=[id, user]
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

        assert_original_plan!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // Left-side expression should be extracted to left input
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
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

        assert_original_plan!(plan, @r#"
        Inner Join:  Filter: test.user = right.user AND mock_leaf(test.user, Utf8("status")) = Utf8("active") AND mock_leaf(right.user, Utf8("role")) = Utf8("admin")
          TableScan: test projection=[id, user]
          TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active") AND __datafusion_extracted_2 = Utf8("admin")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            Projection: mock_leaf(right.user, Utf8("role")) AS __datafusion_extracted_2, right.id, right.user
              TableScan: right projection=[id, user]
        "#)?;

        // Each side should have its own extraction projection
        // A recovery projection is added to restore the original schema
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Inner Join:  Filter: test.user = right.user AND __datafusion_extracted_1 = Utf8("active") AND __datafusion_extracted_2 = Utf8("admin")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user
              TableScan: test projection=[id, user]
            Projection: mock_leaf(right.user, Utf8("role")) AS __datafusion_extracted_2, right.id, right.user
              TableScan: right projection=[id, user]
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

        assert_original_plan!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")?;

        assert_after_extract!(plan, @r"
        Inner Join: test.a = right.a
          TableScan: test projection=[a, b, c]
          TableScan: right projection=[a, b, c]
        ")?;

        // Should return unchanged (no extraction needed)
        assert_optimized!(plan, @r"
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

        assert_original_plan!(plan, @r#"
        Filter: mock_leaf(test.user, Utf8("status")) = Utf8("active")
          Inner Join: mock_leaf(test.user, Utf8("id")) = mock_leaf(right.user, Utf8("id"))
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id, test.user, right.id, right.user
              Projection: test.id, test.user, right.id, right.user
                Inner Join: __datafusion_extracted_2 = __datafusion_extracted_3
                  Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_2, test.id, test.user
                    TableScan: test projection=[id, user]
                  Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_3, right.id, right.user
                    TableScan: right projection=[id, user]
        "#)?;

        // Join keys are extracted to respective sides
        // Filter expression is now pushed through the Join into the left input
        // (merges with the existing extraction projection on that side)
        assert_optimized!(plan, @r#"
        Projection: test.id, test.user, right.id, right.user
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: test.id, test.user, __datafusion_extracted_1, right.id, right.user
              Inner Join: __datafusion_extracted_2 = __datafusion_extracted_3
                Projection: mock_leaf(test.user, Utf8("id")) AS __datafusion_extracted_2, test.id, test.user, mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1
                  TableScan: test projection=[id, user]
                Projection: mock_leaf(right.user, Utf8("id")) AS __datafusion_extracted_3, right.id, right.user
                  TableScan: right projection=[id, user]
        "#)
    }

    /// Extraction projection (get_field in SELECT) above a Join pushes into
    /// the correct input side.
    #[test]
    fn test_extract_projection_above_join() -> Result<()> {
        use datafusion_expr::JoinType;

        let left = test_table_scan_with_struct()?;
        let right = test_table_scan_with_struct_named("right")?;

        // SELECT mock_leaf(test.user, "status"), mock_leaf(right.user, "role")
        // FROM test JOIN right ON test.id = right.id
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["id"], vec!["id"]), None)?
            .project(vec![
                mock_leaf(col("test.user"), "status"),
                mock_leaf(col("right.user"), "role"),
            ])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("status")), mock_leaf(right.user, Utf8("role"))
          Inner Join: test.id = right.id
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // After extraction, projection is untouched (extraction no longer handles projections)
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(test.user, Utf8("status")), mock_leaf(right.user, Utf8("role"))
          Inner Join: test.id = right.id
            TableScan: test projection=[id, user]
            TableScan: right projection=[id, user]
        "#)?;

        // After optimization, extraction projections push through the Join
        // into respective input sides (only id needed as passthrough for join key)
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(test.user,Utf8("status")), __datafusion_extracted_2 AS mock_leaf(right.user,Utf8("role"))
          Inner Join: test.id = right.id
            Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, test.id
              TableScan: test projection=[id, user]
            Projection: mock_leaf(right.user, Utf8("role")) AS __datafusion_extracted_2, right.id
              TableScan: right projection=[id, user]
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
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a"))
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
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
        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(x, Utf8("a")) IS NOT NULL
          Filter: x IS NOT NULL
            Projection: test.user AS x
              TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
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
        assert_after_extract!(plan, @r#"
        Projection: x
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: mock_leaf(x, Utf8("a")) AS __datafusion_extracted_1, x
              Projection: test.user AS x
                TableScan: test projection=[user]
        "#)?;

        assert_optimized!(plan, @r#"
        Projection: x
          Filter: __datafusion_extracted_1 = Utf8("active")
            Projection: test.user AS x, mock_leaf(test.user, Utf8("a")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }

    // =========================================================================
    // SubqueryAlias extraction tests
    // =========================================================================

    /// Extraction projection pushes through SubqueryAlias by remapping
    /// alias-qualified column refs to input-space.
    #[test]
    fn test_extract_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        // SELECT mock_leaf(sub.user, 'name') FROM (SELECT * FROM test) AS sub
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .project(vec![mock_leaf(col("sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          SubqueryAlias: sub
            TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          SubqueryAlias: sub
            TableScan: test projection=[user]
        "#)?;

        // Extraction projection should be pushed below SubqueryAlias
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(sub.user,Utf8("name"))
          SubqueryAlias: sub
            Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1
              TableScan: test projection=[user]
        "#)
    }

    /// Extraction projection pushes through SubqueryAlias + Filter.
    #[test]
    fn test_extract_through_subquery_alias_with_filter() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .filter(mock_leaf(col("sub.user"), "status").eq(lit("active")))?
            .project(vec![mock_leaf(col("sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          Filter: mock_leaf(sub.user, Utf8("status")) = Utf8("active")
            SubqueryAlias: sub
              TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(sub.user, Utf8("name"))
          Projection: sub.user
            Filter: __datafusion_extracted_1 = Utf8("active")
              Projection: mock_leaf(sub.user, Utf8("status")) AS __datafusion_extracted_1, sub.user
                SubqueryAlias: sub
                  TableScan: test projection=[user]
        "#)?;

        // Both extractions should push below SubqueryAlias
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_2 AS mock_leaf(sub.user,Utf8("name"))
          Filter: __datafusion_extracted_1 = Utf8("active")
            SubqueryAlias: sub
              Projection: mock_leaf(test.user, Utf8("status")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_2
                TableScan: test projection=[user]
        "#)
    }

    /// Two layers of SubqueryAlias: extraction pushes through both.
    #[test]
    fn test_extract_through_nested_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("inner_sub")?
            .alias("outer_sub")?
            .project(vec![mock_leaf(col("outer_sub.user"), "name")])?
            .build()?;

        assert_original_plan!(plan, @r#"
        Projection: mock_leaf(outer_sub.user, Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              TableScan: test projection=[user]
        "#)?;

        assert_after_extract!(plan, @r#"
        Projection: mock_leaf(outer_sub.user, Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              TableScan: test projection=[user]
        "#)?;

        // Extraction should push through both SubqueryAlias layers
        assert_optimized!(plan, @r#"
        Projection: __datafusion_extracted_1 AS mock_leaf(outer_sub.user,Utf8("name"))
          SubqueryAlias: outer_sub
            SubqueryAlias: inner_sub
              Projection: mock_leaf(test.user, Utf8("name")) AS __datafusion_extracted_1
                TableScan: test projection=[user]
        "#)
    }

    /// Plain columns through SubqueryAlias — no extraction needed.
    #[test]
    fn test_subquery_alias_no_extraction() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("sub")?
            .project(vec![col("sub.a"), col("sub.b")])?
            .build()?;

        assert_original_plan!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")?;

        assert_after_extract!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")?;

        // No extraction should happen for plain columns
        assert_optimized!(plan, @r"
        SubqueryAlias: sub
          TableScan: test projection=[a, b]
        ")
    }

    /// A variant of MockLeafFunc with the same `name()` but a different concrete type.
    /// Used to verify that deduplication uses `Expr` equality, not `schema_name`.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockLeafFuncVariant {
        signature: Signature,
    }

    impl MockLeafFuncVariant {
        fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Any(2),
                    datafusion_expr::Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MockLeafFuncVariant {
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
            match args.first() {
                Some(ExpressionPlacement::Column)
                | Some(ExpressionPlacement::MoveTowardsLeafNodes) => {
                    ExpressionPlacement::MoveTowardsLeafNodes
                }
                _ => ExpressionPlacement::KeepInPlace,
            }
        }
    }

    /// Two UDFs with the same `name()` but different concrete types should NOT be
    /// deduplicated — they are semantically different expressions that happen to
    /// collide on `schema_name()`. Before the fix (schema_name-based dedup), both
    /// would collapse into one alias; with Expr-equality dedup they get two aliases.
    #[test]
    fn test_different_udfs_same_schema_name_not_deduplicated() -> Result<()> {
        let udf_a = Arc::new(ScalarUDF::new_from_impl(MockLeafFunc::new()));
        let udf_b = Arc::new(ScalarUDF::new_from_impl(MockLeafFuncVariant::new()));

        let expr_a = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf_a,
            vec![col("user"), lit("field")],
        ));
        let expr_b = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf_b,
            vec![col("user"), lit("field")],
        ));

        // Verify preconditions: same schema_name but different Expr
        assert_eq!(
            expr_a.schema_name().to_string(),
            expr_b.schema_name().to_string(),
            "Both expressions should have the same schema_name"
        );
        assert_ne!(
            expr_a, expr_b,
            "Expressions should NOT be equal (different UDF instances)"
        );

        // Use both expressions in a filter so they get extracted.
        // With schema_name dedup, both would collapse into one alias since
        // they have the same schema_name. With Expr-equality, each gets
        // its own extraction alias.
        let table_scan = test_table_scan_with_struct()?;
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(expr_a.clone().eq(lit("a")).and(expr_b.clone().eq(lit("b"))))?
            .select(vec![
                table_scan
                    .schema()
                    .index_of_column_by_name(None, "id")
                    .unwrap(),
            ])?
            .build()?;

        // After extraction, both expressions should get separate aliases
        assert_after_extract!(plan, @r#"
        Projection: test.id
          Projection: test.id, test.user
            Filter: __datafusion_extracted_1 = Utf8("a") AND __datafusion_extracted_2 = Utf8("b")
              Projection: mock_leaf(test.user, Utf8("field")) AS __datafusion_extracted_1, mock_leaf(test.user, Utf8("field")) AS __datafusion_extracted_2, test.id, test.user
                TableScan: test projection=[id, user]
        "#)
    }
}
