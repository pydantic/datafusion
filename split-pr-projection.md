# PR A: Change TableScan.projection to Use Expressions

## Context

This is part of splitting PR #20061 "Consolidate filters and projections onto TableScan" into two smaller PRs. This is the **first PR** that must be completed before PR B (filter consolidation).

**Current branch:** `move-filters` contains the combined changes
**Target branch to create:** `projection-expressions` (from `main`)

## Goal

Change `TableScan.projection` from `Option<Vec<usize>>` (column indices) to `Option<Vec<Expr>>` (expressions), enabling projection expressions to be represented directly on table scans.

**Important:** This PR should NOT include filter consolidation changes. Filter pushdown must continue to work as it does today (optimizer classifies filters, creates `Filter` nodes for unsupported filters).

---

## Current State (main branch)

### TableScan struct (`datafusion/expr/src/logical_plan/plan.rs`)
```rust
pub struct TableScan {
    pub table_name: TableReference,
    pub source: Arc<dyn TableSource>,
    pub projection: Option<Vec<usize>>,  // Column INDICES
    pub projected_schema: DFSchemaRef,
    pub filters: Vec<Expr>,
    pub fetch: Option<usize>,
}
```

### How projections work today
1. SQL parser creates `TableScan` with column indices
2. `optimize_projections` optimizer rule works with indices
3. Physical planner receives indices and passes them directly to `scan_with_args()`
4. No `ProjectionExec` is created for table scans (projection is pushed to source)

### How filters work today (KEEP THIS BEHAVIOR)
1. `push_down_filter` optimizer calls `supports_filters_pushdown()` on the source
2. Filters are classified as Exact/Inexact/Unsupported
3. Exact/Inexact filters go to `TableScan.filters`
4. Unsupported filters stay as `Filter` nodes above the scan
5. Physical planner just passes filters to source, doesn't create `FilterExec`

---

## Target State (after this PR)

### TableScan struct
```rust
pub struct TableScan {
    pub table_name: TableReference,
    pub source: Arc<dyn TableSource>,
    pub projection: Option<Vec<Expr>>,  // Column EXPRESSIONS
    pub projected_schema: DFSchemaRef,
    pub filters: Vec<Expr>,  // No change to filter handling
    pub fetch: Option<usize>,
}
```

### New TableScanBuilder
```rust
pub struct TableScanBuilder {
    table_name: TableReference,
    table_source: Arc<dyn TableSource>,
    projection: Option<Vec<Expr>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
}
```

---

## Files to Modify

### 1. `datafusion/expr/src/logical_plan/plan.rs`

**Changes needed:**

1. **Change TableScan.projection type** (around line 2688):
   ```rust
   // FROM:
   pub projection: Option<Vec<usize>>,

   // TO:
   pub projection: Option<Vec<Expr>>,
   ```

2. **Update TableScan::try_new()** for backward compatibility - convert indices to column expressions:
   ```rust
   pub fn try_new(
       table_name: impl Into<TableReference>,
       table_source: Arc<dyn TableSource>,
       projection: Option<Vec<usize>>,  // Keep this signature for compat
       filters: Vec<Expr>,
       fetch: Option<usize>,
   ) -> Result<Self> {
       // ... validation ...
       let schema = table_source.schema();

       // Convert indices to column expressions
       let projection_exprs = projection.as_ref().map(|indices| {
           indices.iter().map(|&i| {
               let field = schema.field(i);
               Expr::Column(Column::new_unqualified(field.name()))
           }).collect::<Vec<_>>()
       });

       // Build projected_schema from expressions...
   }
   ```

3. **Add TableScanBuilder** (new struct):
   ```rust
   pub struct TableScanBuilder {
       table_name: TableReference,
       table_source: Arc<dyn TableSource>,
       projection: Option<Vec<Expr>>,
       filters: Vec<Expr>,
       fetch: Option<usize>,
   }

   impl TableScanBuilder {
       pub fn new(table_name: impl Into<TableReference>, table_source: Arc<dyn TableSource>) -> Self { ... }
       pub fn with_projection(mut self, projection: Option<Vec<Expr>>) -> Self { ... }
       pub fn with_filters(mut self, filters: Vec<Expr>) -> Self { ... }
       pub fn with_fetch(mut self, fetch: Option<usize>) -> Self { ... }
       pub fn build(self) -> Result<TableScan> { ... }
   }
   ```

4. **Update Display impl** to show expression names instead of indices:
   ```rust
   // In the display code for TableScan
   let names: Vec<String> = exprs.iter().map(|e| {
       if let Expr::Column(col) = e {
           col.name.clone()
       } else {
           e.schema_name().to_string()
       }
   }).collect();
   ```

5. **Update Hash and PartialOrd impls** to handle `Vec<Expr>` instead of `Vec<usize>`

6. **Add helper function** in `datafusion/expr/src/utils.rs`:
   ```rust
   /// Extract column indices from projection expressions that are simple column references
   pub fn projection_indices_from_exprs(exprs: &[Expr], schema: &Schema) -> Option<Vec<usize>> {
       exprs.iter().map(|e| {
           if let Expr::Column(col) = e {
               schema.index_of(col.name()).ok()
           } else {
               None
           }
       }).collect()
   }
   ```

### 2. `datafusion/optimizer/src/optimize_projections/mod.rs`

**Changes needed:**

The optimizer rule works with required column indices. Update it to convert indices to expressions when updating TableScan.

Find the TableScan handling (around line 255-299):
```rust
// Current code works with indices
let projection = match &projection {
    Some(projection) => indices.into_mapped_indices(|idx| projection[idx]),
    None => indices.into_inner(),
};

// Change to work with expressions
let new_projection = match &projection {
    Some(proj_exprs) => {
        let new_exprs: Vec<Expr> = required_indices
            .iter()
            .filter_map(|&idx| proj_exprs.get(idx).cloned())
            .collect();
        Some(new_exprs)
    }
    None => {
        let new_exprs: Vec<Expr> = required_indices
            .iter()
            .map(|&idx| {
                let field = source_schema.field(idx);
                Expr::Column(Column::new_unqualified(field.name()))
            })
            .collect();
        Some(new_exprs)
    }
};
```

### 3. `datafusion/optimizer/src/optimize_projections/required_indices.rs`

Remove or update any index-based helpers that are no longer needed.

### 4. `datafusion/core/src/physical_planner.rs`

**Changes needed:**

Create a new `plan_table_scan()` method that handles projection expressions.

```rust
async fn plan_table_scan(
    &self,
    scan: &TableScan,
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    let provider = source_as_provider(&scan.source)?;
    let source_schema = scan.source.schema();

    // Convert projection expressions to indices for the source
    // (sources still expect indices)
    let projection_indices = scan.projection.as_ref().and_then(|exprs| {
        projection_indices_from_exprs(exprs, &source_schema)
    });

    // Unnormalize filters (existing behavior)
    let filters = unnormalize_cols(scan.filters.iter().cloned());
    let filters_vec: Vec<Expr> = filters.into_iter().collect();

    // Create scan with indices
    let scan_args = ScanArgs::default()
        .with_projection(projection_indices.as_deref())
        .with_filters(if filters_vec.is_empty() { None } else { Some(&filters_vec) })
        .with_limit(scan.fetch);

    let scan_result = provider.scan_with_args(session_state, scan_args).await?;
    let mut plan: Arc<dyn ExecutionPlan> = Arc::clone(scan_result.plan());

    // If projection has non-column expressions, wrap with ProjectionExec
    if let Some(ref proj_exprs) = scan.projection {
        if !self.is_identity_column_projection(proj_exprs, &source_schema) {
            plan = self.create_projection_exec(proj_exprs, plan, session_state)?;
        }
    }

    Ok(plan)
}

fn is_identity_column_projection(&self, exprs: &[Expr], schema: &Schema) -> bool {
    if exprs.len() != schema.fields().len() {
        return false;
    }
    exprs.iter().enumerate().all(|(i, expr)| {
        if let Expr::Column(col) = expr {
            schema.index_of(col.name()).ok() == Some(i)
        } else {
            false
        }
    })
}
```

**IMPORTANT:** Do NOT add filter classification logic. The existing filter handling (filters passed to source, Filter nodes for unsupported) should remain unchanged.

### 5. `datafusion/expr/src/logical_plan/builder.rs`

Update any scan building code to work with the new projection type.

### 6. `datafusion/proto/src/logical_plan/mod.rs`

Update serialization/deserialization for the new projection type.

### 7. `datafusion/sql/src/unparser/plan.rs` and `utils.rs`

Update SQL unparsing to handle expression-based projections.

### 8. `datafusion/substrait/`

Update Substrait conversion in:
- `src/logical_plan/consumer/rel/read_rel.rs`
- `src/logical_plan/producer/rel/read_rel.rs`

### 9. `datafusion/optimizer/src/push_down_filter.rs`

**Minimal changes only** - just update test helpers to use new projection type:

```rust
// In test helper functions, convert projection indices to expressions
let projection_exprs = projection.map(|indices| {
    indices.into_iter().map(|i| {
        let field = schema.field(i);
        Expr::Column(Column::new(Some("test"), field.name()))
    }).collect::<Vec<_>>()
});
```

**DO NOT** change the filter classification logic. Keep `supports_filters_pushdown()` call and Filter node creation.

### 10. Test Files (`.slt` files)

Update expected plans to show projection expressions on TableScan. The key change in plan output:
- Before: `TableScan: test projection=[0, 1]`
- After: `TableScan: test projection=[a, b]`

**Keep Filter nodes in expected outputs** - filter consolidation happens in PR B.

---

## Implementation Steps

1. Create branch from main:
   ```bash
   git checkout main
   git pull origin main
   git checkout -b projection-expressions
   ```

2. Implement TableScan struct changes and TableScanBuilder

3. Update optimize_projections to work with expressions

4. Update physical planner with simplified plan_table_scan

5. Update serialization (proto, substrait)

6. Update SQL unparser

7. Fix push_down_filter test helpers only

8. Update test expected outputs

9. Run tests:
   ```bash
   cargo test -p datafusion-expr
   cargo test -p datafusion-optimizer
   cargo test -p datafusion-core
   cargo test -p datafusion-sqllogictest
   ```

10. Create PR:
    ```bash
    git add -A
    git commit -m "Change TableScan.projection to use expressions instead of column indices"
    git push -u origin projection-expressions
    ```

---

## PR Description

```markdown
## Summary
Changes `TableScan.projection` from `Option<Vec<usize>>` to `Option<Vec<Expr>>`,
allowing projection expressions to be represented directly on the TableScan node.

## Changes
- Modified `TableScan` struct to use expression-based projections
- Added `TableScanBuilder` for constructing scans with expression projections
- Updated `optimize_projections` to work with expressions
- Physical planner creates `ProjectionExec` when projection isn't identity
- Backward compatible: `TableScan::try_new()` still accepts indices

## Why This Change
This is preparation for richer scan optimizations and enables representing
computed columns directly on table scans.

## Test Plan
- All existing tests pass with updated expected outputs
- Verified projection expressions work correctly with various scan patterns
```

---

## Key Differences from Original PR #20061

1. **No filter consolidation** - filters stay in optimizer, not deferred to physical planner
2. **No compute_scan_projection()** that considers filters - not needed since filters are Filter nodes
3. **No FilterExec creation** in plan_table_scan - existing behavior preserved
4. **Keep Filter nodes in test outputs** - they're not removed until PR B
