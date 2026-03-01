# PR B: Consolidate Filter Classification into Physical Planner

## Context

This is part of splitting PR #20061 "Consolidate filters and projections onto TableScan" into two smaller PRs. This is the **second PR** that builds on PR A (projection-expressions).

**Prerequisite:** PR A (projection-expressions) must be merged first
**Target branch to create:** `filter-consolidation` (from `projection-expressions`)

## Goal

Move ALL filter expressions to `TableScan.filters` during logical optimization, deferring the classification (Exact/Inexact/Unsupported) to the physical planner.

**Current behavior (after PR A):**
- Optimizer calls `supports_filters_pushdown()` to classify filters
- Exact/Inexact filters go to `TableScan.filters`
- Unsupported filters stay as `Filter` nodes above the scan
- Physical planner just passes filters to source

**Target behavior (after this PR):**
- Optimizer moves ALL filters to `TableScan.filters` (no classification)
- Physical planner calls `supports_filters_pushdown()` to classify
- Physical planner creates `FilterExec` for post-scan filters

---

## Why This Change

1. **Cleaner separation**: Logical plan doesn't need to know provider capabilities
2. **Better physical planning**: Physical planner has access to session state for smarter decisions
3. **Simpler optimizer**: No need to query TableSource during optimization

---

## Files to Modify

### 1. `datafusion/optimizer/src/push_down_filter.rs`

**Major changes - simplify filter handling:**

Find the `LogicalPlan::TableScan(scan)` case (around line 1124-1170).

**Current code (after PR A):**
```rust
LogicalPlan::TableScan(scan) => {
    let filter_predicates = split_conjunction(&filter.predicate);

    let (volatile_filters, non_volatile_filters): (Vec<&Expr>, Vec<&Expr>) =
        filter_predicates.into_iter().partition(|pred| pred.is_volatile());

    // Check which non-volatile filters are supported by source
    let supported_filters = scan.source
        .supports_filters_pushdown(non_volatile_filters.as_slice())?;

    // Compose scan filters from supported ones
    let zip = non_volatile_filters.into_iter().zip(supported_filters);
    let new_scan_filters = zip.clone()
        .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
        .map(|(pred, _)| pred);

    // Add new scan filters
    let new_scan_filters: Vec<Expr> = scan.filters.iter()
        .chain(new_scan_filters)
        .unique()
        .cloned()
        .collect();

    // Create Filter node for unsupported/inexact filters
    let new_predicate: Vec<Expr> = zip
        .filter(|(_, res)| res != &TableProviderFilterPushDown::Exact)
        .map(|(pred, _)| pred)
        .chain(volatile_filters)
        .cloned()
        .collect();

    let new_scan = LogicalPlan::TableScan(TableScan {
        filters: new_scan_filters,
        ..scan
    });

    Transformed::yes(new_scan).transform_data(|new_scan| {
        if let Some(predicate) = conjunction(new_predicate) {
            make_filter(predicate, Arc::new(new_scan)).map(Transformed::yes)
        } else {
            Ok(Transformed::no(new_scan))
        }
    })
}
```

**New code (simplified):**
```rust
LogicalPlan::TableScan(scan) => {
    // Move ALL filters to the TableScan.
    // Exclude scalar subqueries - they are essentially a fork in the logical
    // plan tree that should not be processed by TableScan nodes.
    let filter_predicates = split_conjunction(&filter.predicate);

    // Partition predicates: scalar subqueries must stay in a Filter node
    let (scalar_subquery_filters, pushable_filters): (Vec<_>, Vec<_>) =
        filter_predicates.iter().partition(|pred| {
            pred.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_)))).unwrap()
        });

    // Combine existing scan filters with pushable filter predicates
    let new_scan_filters: Vec<Expr> = scan.filters.iter()
        .chain(pushable_filters)
        .unique()
        .cloned()
        .collect();

    let new_scan = LogicalPlan::TableScan(TableScan {
        filters: new_scan_filters,
        ..scan
    });

    // Keep scalar subquery filters in a Filter node above the TableScan
    let remaining_predicates: Vec<Expr> =
        scalar_subquery_filters.into_iter().cloned().collect();

    if let Some(predicate) = conjunction(remaining_predicates) {
        make_filter(predicate, Arc::new(new_scan)).map(Transformed::yes)
    } else {
        Ok(Transformed::yes(new_scan))
    }
}
```

**Key changes:**
1. Remove `supports_filters_pushdown()` call
2. Remove filter classification logic
3. Move ALL filters (except scalar subqueries) to `TableScan.filters`
4. Only keep `Filter` nodes for scalar subquery predicates

**Also remove unused imports:**
```rust
// Remove these if no longer used:
use datafusion_expr::TableProviderFilterPushDown;
// Remove assert_eq_or_internal_err if only used for filter count check
```

### 2. `datafusion/core/src/physical_planner.rs`

**Major changes - add filter classification:**

Update `plan_table_scan()` to classify filters and create `FilterExec`:

```rust
async fn plan_table_scan(
    &self,
    scan: &TableScan,
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion_expr::TableProviderFilterPushDown;

    let provider = source_as_provider(&scan.source)?;
    let source_schema = scan.source.schema();

    // Remove all qualifiers from filters as the provider doesn't know
    // (nor should care) how the relation was referred to in the query
    let filters: Vec<Expr> = unnormalize_cols(scan.filters.iter().cloned());

    // Separate volatile filters (they should never be pushed down)
    let (volatile_filters, non_volatile_filters): (Vec<Expr>, Vec<Expr>) = filters
        .into_iter()
        .partition(|pred: &Expr| pred.is_volatile());

    // Classify filters using supports_filters_pushdown
    let filter_refs: Vec<&Expr> = non_volatile_filters.iter().collect();
    let supported = provider.supports_filters_pushdown(&filter_refs)?;

    assert_eq!(
        non_volatile_filters.len(),
        supported.len(),
        "supports_filters_pushdown returned {} results for {} filters",
        supported.len(),
        non_volatile_filters.len()
    );

    // Separate filters into:
    // - pushable_filters: Exact or Inexact filters to pass to the provider
    // - post_scan_filters: Inexact, Unsupported, and volatile filters for FilterExec
    let mut pushable_filters = Vec::new();
    let mut post_scan_filters = Vec::new();

    for (filter, support) in non_volatile_filters.into_iter().zip(supported.iter()) {
        match support {
            TableProviderFilterPushDown::Exact => {
                pushable_filters.push(filter);
            }
            TableProviderFilterPushDown::Inexact => {
                pushable_filters.push(filter.clone());
                post_scan_filters.push(filter);
            }
            TableProviderFilterPushDown::Unsupported => {
                post_scan_filters.push(filter);
            }
        }
    }

    // Add volatile filters to post_scan_filters
    post_scan_filters.extend(volatile_filters);

    // Compute required column indices for the scan
    // We need columns from BOTH projection expressions AND post-scan filters
    let scan_projection = self.compute_scan_projection(
        &scan.projection,
        &post_scan_filters,
        &source_schema,
    )?;

    // Check if we have inexact filters - if so, we can't push limit
    let has_inexact = supported.contains(&TableProviderFilterPushDown::Inexact);
    let scan_limit = if has_inexact || !post_scan_filters.is_empty() {
        None // Can't push limit when post-filtering is needed
    } else {
        scan.fetch
    };

    // Create the scan
    let scan_args = ScanArgs::default()
        .with_projection(scan_projection.as_deref())
        .with_filters(if pushable_filters.is_empty() {
            None
        } else {
            Some(&pushable_filters)
        })
        .with_limit(scan_limit);

    let scan_result = provider.scan_with_args(session_state, scan_args).await?;
    let mut plan: Arc<dyn ExecutionPlan> = Arc::clone(scan_result.plan());

    // Create a DFSchema from the scan output for filter and projection creation
    let scan_output_schema = plan.schema();
    let scan_df_schema = DFSchema::try_from(scan_output_schema.as_ref().clone())?;

    // Wrap with FilterExec if needed
    if !post_scan_filters.is_empty() {
        if let Some(filter_expr) = conjunction(post_scan_filters) {
            let num_scan_columns = scan_output_schema.fields().len();
            plan = self.create_filter_exec(
                &filter_expr,
                plan,
                &scan_df_schema,
                session_state,
                num_scan_columns,
            )?;
        }
    }

    // Wrap with ProjectionExec if projection is present and differs from scan output
    if let Some(ref proj_exprs) = scan.projection {
        let needs_projection = !self.is_identity_column_projection(proj_exprs, &source_schema)
            || scan_output_schema.fields().len() != proj_exprs.len();

        if needs_projection {
            let unnormalized_proj_exprs = unnormalize_cols(proj_exprs.iter().cloned());
            plan = self.create_projection_exec(
                &unnormalized_proj_exprs,
                plan,
                &scan_df_schema,
                session_state,
            )?;
        }
    }

    // Apply limit if it wasn't pushed to scan
    if let Some(fetch) = scan.fetch {
        if scan_limit.is_none() {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(fetch)));
        }
    }

    Ok(plan)
}
```

**Add compute_scan_projection helper:**
```rust
/// Compute the column indices needed for the scan based on projection
/// expressions and post-scan filters.
fn compute_scan_projection(
    &self,
    projection: &Option<Vec<Expr>>,
    post_filters: &[Expr],
    source_schema: &Schema,
) -> Result<Option<Vec<usize>>> {
    use std::collections::HashSet;

    // Collect all columns needed
    let mut required_columns = HashSet::new();

    // Add columns from projection expressions
    if let Some(exprs) = projection {
        for expr in exprs {
            expr.apply(|e| {
                if let Expr::Column(col) = e {
                    required_columns.insert(col.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
    }

    // Add columns from post-scan filters
    for filter in post_filters {
        filter.apply(|e| {
            if let Expr::Column(col) = e {
                required_columns.insert(col.name().to_string());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }

    // If no projection specified and no filters, return None (all columns)
    if projection.is_none() && post_filters.is_empty() {
        return Ok(None);
    }

    // If projection is None but we have filters, we need all columns
    if projection.is_none() {
        return Ok(None);
    }

    // Convert column names to indices
    let indices: Vec<usize> = required_columns
        .iter()
        .filter_map(|name| source_schema.index_of(name).ok())
        .sorted()
        .collect();

    if indices.is_empty() {
        Ok(None)
    } else {
        Ok(Some(indices))
    }
}
```

**Add create_filter_exec helper** (handles async UDFs):
```rust
fn create_filter_exec(
    &self,
    predicate: &Expr,
    input: Arc<dyn ExecutionPlan>,
    input_dfschema: &DFSchema,
    session_state: &SessionState,
    num_input_columns: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let runtime_expr = self.create_physical_expr(predicate, input_dfschema, session_state)?;
    let input_schema = input.schema();

    let filter = match self.try_plan_async_exprs(
        num_input_columns,
        PlannedExprResult::Expr(vec![runtime_expr]),
        input_schema.as_ref(),
    )? {
        PlanAsyncExpr::Sync(PlannedExprResult::Expr(runtime_expr)) => {
            FilterExecBuilder::new(Arc::clone(&runtime_expr[0]), input)
                .with_batch_size(session_state.config().batch_size())
                .build()?
        }
        PlanAsyncExpr::Async(async_map, PlannedExprResult::Expr(runtime_expr)) => {
            let async_exec = AsyncFuncExec::try_new(async_map.async_exprs, input)?;
            FilterExecBuilder::new(Arc::clone(&runtime_expr[0]), Arc::new(async_exec))
                .apply_projection(Some((0..num_input_columns).collect()))?
                .with_batch_size(session_state.config().batch_size())
                .build()?
        }
        _ => return internal_err!("Unexpected result from try_plan_async_exprs"),
    };

    let selectivity = session_state
        .config()
        .options()
        .optimizer
        .default_filter_selectivity;

    Ok(Arc::new(filter.with_default_selectivity(selectivity)?))
}
```

**Also update extract_dml_filters** to include TableScan filters:
```rust
fn extract_dml_filters(input: &Arc<LogicalPlan>) -> Result<Vec<Expr>> {
    let mut filters = Vec::new();

    input.apply(|node| {
        match node {
            LogicalPlan::Filter(filter) => {
                filters.extend(split_conjunction(&filter.predicate).into_iter().cloned());
            }
            LogicalPlan::TableScan(scan) => {
                // Also extract filters from TableScan (where they may be pushed down)
                filters.extend(scan.filters.iter().cloned());
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(filters)
}
```

### 3. Update imports in physical_planner.rs

Add:
```rust
use std::collections::HashSet;
use datafusion_expr::TableProviderFilterPushDown;
use datafusion_expr::utils::conjunction;
```

### 4. Test Files (`.slt` files)

Update expected plans to show:
- Filters on TableScan instead of separate Filter nodes
- No separate `Filter:` nodes for pushable filters

**Example change:**
```
# Before (after PR A):
physical_plan
01)ProjectionExec: ...
02)--FilterExec: a > 10
03)----DataSourceExec: ...

# After (this PR):
physical_plan
01)ProjectionExec: ...
02)--DataSourceExec: ..., filters=[a > 10]
```

For Unsupported/Inexact filters, `FilterExec` will still appear but created by physical planner.

---

## Edge Cases to Handle

### 1. Scalar Subqueries
Scalar subquery expressions must stay as `Filter` nodes (handled in push_down_filter).

### 2. Volatile Expressions
Functions like `random()` should never be pushed to the source. They go to `FilterExec`.

### 3. Async UDFs
The `create_filter_exec` helper handles async UDFs by wrapping with `AsyncFuncExec`.

### 4. Limit Pushdown
If there are post-scan filters (Inexact/Unsupported), limit cannot be pushed to scan.

---

## Implementation Steps

1. Create branch from projection-expressions:
   ```bash
   git checkout projection-expressions
   git pull origin projection-expressions
   git checkout -b filter-consolidation
   ```

2. Simplify push_down_filter.rs (remove classification)

3. Enhance physical_planner.rs with filter classification

4. Add compute_scan_projection and create_filter_exec helpers

5. Update extract_dml_filters

6. Update test expected outputs

7. Run tests:
   ```bash
   cargo test -p datafusion-optimizer
   cargo test -p datafusion-core
   cargo test -p datafusion-sqllogictest
   ```

8. Create PR:
   ```bash
   git add -A
   git commit -m "Consolidate filter classification into physical planner"
   git push -u origin filter-consolidation
   ```

---

## PR Description

```markdown
## Summary
Moves all filter expressions to `TableScan.filters` during logical optimization,
deferring the classification (Exact/Inexact/Unsupported) to the physical planner.

## Related Issues
Closes #19894. Helps with #19387.

## Changes
- Simplified `push_down_filter` to move all filters to TableScan
- Physical planner now calls `supports_filters_pushdown` and creates `FilterExec`
- Updated `compute_scan_projection` to include columns needed by post-scan filters
- Handles async UDFs in filters via `create_filter_exec` helper

## Why This Change
- Cleaner separation: logical plan doesn't need to know provider capabilities
- Enables better physical planning decisions at execution time
- Physical planner has access to session state for smarter decisions

## Test Plan
- All existing tests pass with updated expected outputs
- Verified filter pushdown works correctly with Exact/Inexact/Unsupported filters
```

---

## Key Differences from Original PR #20061

1. **Builds on PR A** - assumes projection expressions are already done
2. **Focused scope** - only filter consolidation changes
3. **Smaller diff** - no projection-related changes

## Reference: Original PR #20061 Commits

The original PR has these commits:
- `0166985f9 Consolidate filters and projections onto TableScan` - main commit
- `e61159874 handle async udfs` - async UDF fix
- `fc6807c8c fix tests by blocking on subquery expressions` - subquery fix

The async UDF handling and scalar subquery blocking are included in this plan.
