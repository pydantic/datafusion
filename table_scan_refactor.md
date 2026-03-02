# TableScan Physical Planning Refactor

## Goal

Refactor TableScan physical planning so that **all** filter and projection handling happens during physical planning, not during logical optimization. This eliminates the need for separate `Filter` and `Projection` logical nodes above `TableScan`.

### Current Structure
```
Projection(exprs)                    →  ProjectionExec
  └─ Filter(predicate)               →  FilterExec
       └─ TableScan(filters=[...])   →  <scan from provider>
```

### Target Structure
```
TableScan(projection=[exprs], filters=[all_filters])
    ↓ physical planning
ProjectionExec
  └─ FilterExec (for Inexact/Unsupported filters)
       └─ <scan from provider>
```

---

## Current Implementation

### 1. TableScan Struct

**File:** `datafusion/expr/src/logical_plan/plan.rs:2680-2693`

```rust
pub struct TableScan {
    pub table_name: TableReference,
    pub source: Arc<dyn TableSource>,
    pub projection: Option<Vec<usize>>,  // Column indices
    pub projected_schema: DFSchemaRef,
    pub filters: Vec<Expr>,              // Only Exact+Inexact filters (not Unsupported)
    pub fetch: Option<usize>,
}
```

### 2. Filter Pushdown (Optimizer Phase)

**File:** `datafusion/optimizer/src/push_down_filter.rs:1128-1185`

Currently, the `PushDownFilter` optimizer rule:
1. Calls `source.supports_filters_pushdown()` to check each filter
2. Moves Exact+Inexact filters into `TableScan.filters`
3. Keeps Inexact+Unsupported filters in a `Filter` node above

```rust
LogicalPlan::TableScan(scan) => {
    let filter_predicates = split_conjunction(&filter.predicate);

    // Check which filters are supported
    let supported_filters = scan.source
        .supports_filters_pushdown(non_volatile_filters.as_slice())?;

    // Exact + Inexact → go to TableScan.filters
    let new_scan_filters = zip.clone()
        .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
        .map(|(pred, _)| pred);

    // Inexact + Unsupported → stay in Filter node above (DUPLICATES Inexact!)
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

    // Add Filter node above if there are remaining predicates
    if let Some(predicate) = conjunction(new_predicate) {
        make_filter(predicate, Arc::new(new_scan))
    } else {
        new_scan
    }
}
```

**Nit with current state:** Inexact filters are duplicated - they appear in both `TableScan.filters` AND the `Filter` node above.

### 3. Physical Planning (Current)

**File:** `datafusion/core/src/physical_planner.rs:458-477`

TableScan conversion just passes filters through to the provider:

```rust
LogicalPlan::TableScan(TableScan {
    source,
    projection,
    filters,
    fetch,
    ..
}) => {
    let source = source_as_provider(source)?;
    let filters = unnormalize_cols(filters.iter().cloned());
    let filters_vec = filters.into_iter().collect::<Vec<_>>();
    let opts = ScanArgs::default()
        .with_projection(projection.as_deref())
        .with_filters(Some(&filters_vec))
        .with_limit(*fetch);
    let res = source.scan_with_args(session_state, opts).await?;
    Arc::clone(res.plan())
}
```

**File:** `datafusion/core/src/physical_planner.rs:949-1005`

Filter nodes are converted separately to FilterExec:

```rust
LogicalPlan::Filter(Filter { predicate, input, .. }) => {
    let physical_input = children.one()?;
    let runtime_expr = self.create_physical_expr(predicate, input_dfschema, session_state)?;
    FilterExecBuilder::new(Arc::clone(&runtime_expr[0]), physical_input)
        .with_batch_size(session_state.config().batch_size())
        .build()?
}
```

### 4. TableProvider Trait

**File:** `datafusion/catalog/src/table.rs:285-293`

```rust
fn supports_filters_pushdown(
    &self,
    filters: &[&Expr],
) -> Result<Vec<TableProviderFilterPushDown>> {
    // Default: all filters unsupported
    Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
}
```

**File:** `datafusion/expr/src/table_source.rs:37-51`

```rust
pub enum TableProviderFilterPushDown {
    Unsupported,  // Cannot handle, need FilterExec above
    Inexact,      // Can handle but may return extra rows, need FilterExec above
    Exact,        // Guarantees correct filtering, no FilterExec needed
}
```

---

## Proposed Changes

### 1. Modify TableScan Struct

**File:** `datafusion/expr/src/logical_plan/plan.rs`

Change `projection` from `Vec<usize>` to `Vec<Expr>`:

```rust
pub struct TableScan {
    pub table_name: TableReference,
    pub source: Arc<dyn TableSource>,
    pub projection: Option<Vec<Expr>>,   // CHANGED: Expression projections
    pub projected_schema: DFSchemaRef,
    pub filters: Vec<Expr>,              // Now contains ALL filters (Exact+Inexact+Unsupported)
    pub fetch: Option<usize>,
}
```

### 2. Add TableScanBuilder with Expression Projections

**File:** `datafusion/expr/src/logical_plan/plan.rs:2767-2815`

To support building with expression projections, and instead of adding a new constructor with many parameters, add a builder struct:

```rust
pub struct TableScanBuilder {
    table_name: TableReference,
    table_source: Arc<dyn TableSource>,
    projection: Option<Vec<Expr>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
}

impl TableScanBuilder {
    pub fn new(
        table_name: impl Into<TableReference>,
        table_source: Arc<dyn TableSource>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            table_source,
            projection: None,
            filters: vec![],
            fetch: None,
        }
    }

    pub fn with_projection(mut self, projection: Vec<Expr>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_fetch(mut self, fetch: usize) -> Self {
        self.fetch = Some(fetch);
        self
    }

    pub fn build(self) -> Result<TableScan> {
        ensure_valid_table_name(&self.table_name)?;

        let schema = self.table_source.schema();

        // Compute projected_schema from expressions
        let projected_schema = match &self.projection {
            Some(exprs) => {
                let source_dfschema = DFSchema::try_from_qualified_schema(
                    self.table_name.clone(),
                    &schema
                )?;
                // Use exprlist_to_fields to compute output schema from expressions
                let fields = exprlist_to_fields(exprs, &source_dfschema)?;
                DFSchema::new_with_metadata(fields, schema.metadata.clone())?
            }
            None => {
                DFSchema::try_from_qualified_schema(self.table_name.clone(), &schema)?
            }
        };

        Ok(TableScan {
            table_name: self.table_name,
            source: self.table_source,
            projection: self.projection,
            projected_schema: Arc::new(projected_schema),
            filters: self.filters,
            fetch: self.fetch,
        })
    }
}
```

Update `try_new()` to use the builder:

```rust
/// Create TableScan with column indices (for backward compatibility)
pub fn try_new(
    table_name: impl Into<TableReference>,
    table_source: Arc<dyn TableSource>,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
) -> Result<Self> {
    let schema = table_source.schema();

    // Convert indices to column expressions
    let projection_exprs = projection.map(|indices| {
        indices.iter().map(|&idx| {
            let field = schema.field(idx);
            col(Column::new_unqualified(field.name()))
        }).collect()
    });

    let mut builder = TableScanBuilder::new(table_name, table_source)
        .with_filters(filters)
        .with_fetch(fetch.unwrap_or_default());
    if let Some(exprs) = projection_exprs {
        builder = builder.with_projection(exprs);
    }
    builder.build()
}
```

### 4. Refactor Physical Planner

**File:** `datafusion/core/src/physical_planner.rs:458-477`

Replace the current TableScan handling with:

```rust
LogicalPlan::TableScan(TableScan {
    table_name,
    source,
    projection,
    projected_schema,
    filters,
    fetch,
}) => {
    let source = source_as_provider(source)?;
    let source_schema = source.schema();

    // === STEP 1: Determine filter pushdown support ===
    let filter_refs: Vec<&Expr> = filters.iter().collect();
    let pushdown_support = source.supports_filters_pushdown(&filter_refs)?;

    // Filters to pass to provider (Exact + Inexact)
    let provider_filters: Vec<Expr> = filters.iter()
        .zip(pushdown_support.iter())
        .filter(|(_, support)| **support != TableProviderFilterPushDown::Unsupported)
        .map(|(f, _)| f.clone())
        .collect();

    // Filters that need FilterExec above (Inexact + Unsupported)
    let post_scan_filters: Vec<Expr> = filters.iter()
        .zip(pushdown_support.iter())
        .filter(|(_, support)| **support != TableProviderFilterPushDown::Exact)
        .map(|(f, _)| f.clone())
        .collect();

    // === STEP 2: Compute column indices needed from source ===
    // Union of columns referenced by projection exprs + post_scan_filters
    let source_indices = compute_required_columns(
        projection.as_ref(),
        &post_scan_filters,
        &source_schema,
    );

    // === STEP 3: Call provider's scan ===
    let provider_filters_unnorm = unnormalize_cols(provider_filters.into_iter());
    let provider_filters_vec: Vec<Expr> = provider_filters_unnorm.collect();

    let opts = ScanArgs::default()
        .with_projection(Some(&source_indices))
        .with_filters(Some(&provider_filters_vec))
        .with_limit(*fetch);

    let scan_result = source.scan_with_args(session_state, opts).await?;
    let mut result: Arc<dyn ExecutionPlan> = Arc::clone(scan_result.plan());

    // === STEP 4: Wrap with FilterExec if needed ===
    if !post_scan_filters.is_empty() {
        let post_filters_unnorm: Vec<Expr> = unnormalize_cols(post_scan_filters.into_iter()).collect();
        let predicate = conjunction(post_filters_unnorm).unwrap();

        // Create physical expression for the filter
        // Note: Need to use the scan output schema here
        let scan_schema = result.schema();
        let physical_predicate = self.create_physical_expr(
            &predicate,
            &DFSchema::try_from(scan_schema.as_ref().clone())?,
            session_state,
        )?;

        result = Arc::new(
            FilterExecBuilder::new(physical_predicate, result)
                .with_batch_size(session_state.config().batch_size())
                .build()?
        );
    }

    // === STEP 5: Wrap with ProjectionExec if needed ===
    if let Some(exprs) = &projection {
        // Check if projection is just column references matching scan output
        if !is_identity_projection(exprs, result.schema()) {
            let physical_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = exprs.iter()
                .map(|e| {
                    let phys = self.create_physical_expr(
                        e,
                        projected_schema.as_ref(),
                        session_state,
                    )?;
                    let name = e.schema_name().to_string();
                    Ok((phys, name))
                })
                .collect::<Result<Vec<_>>>()?;

            result = Arc::new(ProjectionExec::try_new(physical_exprs, result)?);
        }
    }

    result
}
```

### 5. Helper Functions to Add

**File:** `datafusion/core/src/physical_planner.rs` (or a new utils module)

```rust
/// Extract all column references from projection expressions and filters.
/// Returns sorted unique column indices needed from source schema.
fn compute_required_columns(
    projection: Option<&Vec<Expr>>,
    filters: &[Expr],
    source_schema: &SchemaRef,
) -> Vec<usize> {
    let mut columns = BTreeSet::new();

    // Collect columns from projection expressions
    if let Some(exprs) = projection {
        for expr in exprs {
            expr.apply(|e| {
                if let Expr::Column(col) = e {
                    if let Ok(idx) = source_schema.index_of(col.name()) {
                        columns.insert(idx);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }).ok();
        }
    } else {
        // No projection = all columns
        columns.extend(0..source_schema.fields().len());
    }

    // Collect columns from filters (all filters, not just pushed ones)
    for expr in filters {
        expr.apply(|e| {
            if let Expr::Column(col) = e {
                if let Ok(idx) = source_schema.index_of(col.name()) {
                    columns.insert(idx);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        }).ok();
    }

    columns.into_iter().collect()
}

/// Check if projection is just selecting columns in order (identity)
fn is_identity_projection(exprs: &[Expr], schema: &SchemaRef) -> bool {
    if exprs.len() != schema.fields().len() {
        return false;
    }

    exprs.iter().enumerate().all(|(i, expr)| {
        matches!(expr, Expr::Column(col) if col.name() == schema.field(i).name())
    })
}
```

### 6. Update PushDownFilter Optimizer

**File:** `datafusion/optimizer/src/push_down_filter.rs:1128-1185`

Simplify to just move ALL filters into TableScan without checking pushdown support:

```rust
LogicalPlan::TableScan(scan) => {
    let filter_predicates = split_conjunction(&filter.predicate);

    // Move ALL filters to TableScan.filters (pushdown decision deferred to physical planning)
    let new_scan_filters: Vec<Expr> = scan.filters
        .iter()
        .chain(filter_predicates.iter().cloned())
        .unique()
        .cloned()
        .collect();

    // No Filter node above - all filters now in TableScan
    Transformed::yes(LogicalPlan::TableScan(TableScan {
        filters: new_scan_filters,
        ..scan
    }))
}
```

**Note:** This removes the call to `supports_filters_pushdown()` from the optimizer. That call now happens in the physical planner.

### 7. Update OptimizeProjections

**File:** `datafusion/optimizer/src/optimize_projections/mod.rs:255-274`

Update to merge Projection nodes into TableScan:

```rust
LogicalPlan::Projection(projection) => {
    if let LogicalPlan::TableScan(scan) = projection.input.as_ref() {
        // TODO: Merge existing scan.projection with new projection.expr
        // E.g. if TableScan::projection = [col("b") + col("c") as "bc"]
        // and Projection::expr = [col("bc") * 2 as "bc2"]
        // then we need to rewrite the projection into TableScan as:
        // [ (col("b") + col("c")) * 2 as "bc2" ]
        let merged_projection = todo!("Merge projection expressions into TableScan")

        let new_scan = TableScanBuilder::new(
            scan.table_name.clone(),
            Arc::clone(&scan.source),
        )
        .with_projection(merged_projection)
        .with_filters(scan.filters.clone())
        .with_fetch(scan.fetch)
        .build()?;

        return Ok(Transformed::yes(LogicalPlan::TableScan(new_scan)));
    }
    // ... existing projection handling for non-TableScan inputs
}

LogicalPlan::TableScan(table_scan) => {
    // Update projection to only include required columns/expressions
    let new_projection = match &table_scan.projection {
        Some(exprs) => {
            Some(indices.iter().map(|&idx| exprs[idx].clone()).collect())
        }
        None => {
            let schema = table_scan.source.schema();
            Some(indices.iter().map(|&idx| {
                let field = schema.field(idx);
                col(Column::new(Some(table_scan.table_name.clone()), field.name()))
            }).collect())
        }
    };

    let new_scan = TableScan::try_new(
        table_scan.table_name.clone(),
        Arc::clone(&table_scan.source),
        new_projection,
        table_scan.filters.clone(),
        table_scan.fetch,
    )?;

    Ok(Transformed::yes(LogicalPlan::TableScan(new_scan)))
}
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `datafusion/expr/src/logical_plan/plan.rs` | Change `projection: Option<Vec<usize>>` to `Option<Vec<Expr>>`, update `try_new()`, add `try_new_with_indices()` |
| `datafusion/core/src/physical_planner.rs` | Rewrite TableScan handling to add FilterExec + ProjectionExec, add helper functions |
| `datafusion/optimizer/src/push_down_filter.rs` | Simplify to move all filters to TableScan without checking pushdown |
| `datafusion/optimizer/src/optimize_projections/mod.rs` | Update to handle `Vec<Expr>` projections, merge Projection into TableScan |
| `datafusion/expr/src/logical_plan/builder.rs` | Update builder methods to use new constructors |

---

## Testing Strategy

1. **Unit tests for new helper functions:**
   - `compute_required_columns()`
   - `is_identity_projection()`

2. **Integration tests:**
   - Query with filter that is `Exact` → no FilterExec above scan
   - Query with filter that is `Inexact` → FilterExec above scan
   - Query with filter that is `Unsupported` → FilterExec above scan, filter not passed to provider
   - Query with expression projection → ProjectionExec above scan
   - Query with column-only projection → no ProjectionExec (identity)

3. **Existing tests:**
   - Run `cargo test -p datafusion-core`
   - Run `cargo test -p datafusion-optimizer`
   - Many tests will need updating due to changed plan structure

---

## Migration Notes

- The `Filter` logical node will still exist for filters above non-TableScan nodes
- Existing `TableProvider` implementations don't need changes - they still receive filters via `ScanArgs`
- The `supports_filters_pushdown()` method is now called during physical planning instead of optimization
