# Handle complex projections in ordering validation

## Problem

`get_projected_output_ordering()` uses `ordered_column_indices_from_projection()` to build a
mapping from projected column indices → table-schema indices, needed so `MinMaxStatistics` can
look up file-level statistics. But `ordered_column_indices_from_projection` is **all-or-nothing**:
if *any* expression in the projection isn't a simple `Column`, it returns `None` for the entire
projection — even if the sort columns themselves are simple column refs.

With projection pushdown (`try_merge`), complex expressions in the file source's `ProjectionExprs`
are common. For example:

```sql
SELECT a + 1 AS x, b, c FROM t ORDER BY b
```

col1min,col1max
a,a -> RecordBatch 2 row
b,b -> RecordBatch 8092 rows
b,b -> RecordBatch 8092 rows

a,b -> RowGroup 1
b,b -> RowGroup 2



After pushdown the projection is `[BinaryExpr(a+1), col(b), col(c)]`. The ordering on `b`
(projected index 1) maps cleanly to a simple `Column`, but `ordered_column_indices_from_projection`
fails on index 0 and returns `None`. With multi-file groups, this causes valid orderings to be
unnecessarily dropped.

## Key insight

`MinMaxStatistics::new_from_files` only indexes the projection at sort-column positions
(`statistics.rs:138-140`):

```rust
let i = projection
    .map(|p| p[c.index()])       // only accesses sort-column positions
    .unwrap_or_else(|| c.index());
```

We don't need a full projection mapping — just the entries at sort-column positions.

## Files to modify

- `datafusion/datasource/src/file_scan_config.rs`

## Changes

### 1. Replace `ordered_column_indices_from_projection` with per-ordering resolution

Replace the current all-or-nothing function:

```rust
fn ordered_column_indices_from_projection(
    projection: &ProjectionExprs,
) -> Option<Vec<usize>> {
    projection
        .expr_iter()
        .map(|e| {
            let index = e.as_any().downcast_ref::<Column>()?.index();
            Some(index)
        })
        .collect::<Option<Vec<usize>>>()
}
```

With a function that resolves projection indices only for the columns referenced by a
specific ordering:

```rust
/// For each sort column in `ordering`, look up its position in `projection` and
/// try to resolve it to a table-schema column index. Returns `Some(indices)` if
/// every sort column maps to a simple `Column` in the projection, `None` otherwise.
fn resolve_sort_column_projection(
    ordering: &LexOrdering,
    projection: &ProjectionExprs,
) -> Option<Vec<usize>> {
    ordering
        .iter()
        .map(|sort_expr| {
            let col = sort_expr.expr.as_any().downcast_ref::<Column>()?;
            let proj_expr = projection.expr_at(col.index())?;
            proj_expr.as_any().downcast_ref::<Column>().map(|c| c.index())
        })
        .collect()
}
```

Note: this requires a way to get a projection expression by index. `ProjectionExprs` already
exposes `expr_iter()` — check if there's an indexed accessor, or use `expr_iter().nth(idx)`.

### 2. Refactor `get_projected_output_ordering` to validate per-ordering

Instead of pre-computing a single projection mapping and branching on `Some(Some(_))` /
`None` / `Some(None)`, move the resolution inside `validate_orderings` (or into a new
variant) so each ordering is independently resolved:

```rust
fn get_projected_output_ordering(
    base_config: &FileScanConfig,
    projected_schema: &SchemaRef,
) -> Vec<LexOrdering> {
    let projected_orderings =
        project_orderings(&base_config.output_ordering, projected_schema);

    let projection = base_config.file_source.projection();

    projected_orderings
        .into_iter()
        .filter(|ordering| {
            match projection.as_ref() {
                None => {
                    // No projection — validate directly with statistics
                    is_ordering_valid_for_file_groups(
                        &base_config.file_groups, ordering, projected_schema, None,
                    )
                }
                Some(proj) => {
                    match resolve_sort_column_projection(ordering, proj) {
                        Some(indices) => {
                            // All sort columns resolved — validate with statistics
                            is_ordering_valid_for_file_groups(
                                &base_config.file_groups,
                                ordering,
                                projected_schema,
                                Some(&indices),
                            )
                        }
                        None => {
                            // Some sort column is a complex expression — can't
                            // look up statistics. Fall back to single-file check.
                            base_config.file_groups.iter().all(|g| g.len() <= 1)
                        }
                    }
                }
            }
        })
        .collect()
}
```

This collapses the three match arms into a clean per-ordering pipeline. The `Some(None)` case
(complex sort column) now only applies to orderings that actually reference non-Column
expressions — other orderings in the same scan that reference simple columns still get
validated with statistics.

### 3. Remove `ordered_column_indices_from_projection`

It's no longer called anywhere after step 2. Delete it.

### 4. Check `ProjectionExprs` accessor

`resolve_sort_column_projection` needs to access a projection expression by index.
Check whether `ProjectionExprs` has an indexed accessor (e.g. `expr_at(idx)`,
`Index` impl, or similar). If not, `expr_iter().nth(idx)` works but is O(n) per call.
If needed, add a small `pub fn expr_at(&self, idx: usize) -> Option<&dyn PhysicalExpr>`
method to `ProjectionExprs` in `datafusion/physical-expr/src/projection.rs`.

## Verification

1. Existing tests must still pass:
   ```
   cargo test -p datafusion-datasource
   ```

2. The SLT sorted-statistics tests:
   ```
   cargo test -p datafusion-sqllogictest --test sqllogictests -- parquet_sorted_statistics
   ```

3. Consider adding a test case with a mixed projection (some expressions, some columns)
   and multi-file groups, verifying the ordering on a simple column is retained while
   an ordering on a computed expression is dropped.
