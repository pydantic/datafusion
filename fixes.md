# Review Status: `extract_leaf_expressions.rs` (PR #20117)

Checked against current state of `get-field-pushdown-try-3` branch (3rd pass).

## Fixed (15 of 17)

| # | Issue | How it was resolved |
|---|-------|---------------------|
| 2 | `try_push_into_inputs` was monolithic (~177 lines) | Split into `push_extraction_pairs` (~53 lines), `route_to_inputs` (~46 lines), `try_push_into_inputs` (~93 lines). SubqueryAlias/Union remapping extracted into shared `remap_pairs_and_columns` helper (line 298). |
| 3 | Complex 4-way match `(pushed, needs_recovery)` | Refactored into 2-step pattern: `match pushed` for `Some`/`None` (lines 867-887) then `if needs_recovery` (lines 889-898). Reads linearly now. |
| 4 | `find_owning_input` used `position()` — wrong input for ambiguous unqualified columns | Replaced with explicit loop returning `None` on ambiguity (lines 217-222). Dedicated unit test `test_find_owning_input_ambiguous_unqualified_column` (line 2344) verifies unqualified columns matching both sides return `None`, while qualified columns resolve correctly. |
| 5 | `build_recovery_projection` positional mapping had undocumented invariant | Comment (lines 408-410) now documents that `with_new_exprs` on all supported node types preserves column order. Added `debug_assert!` (lines 411-414) as safety net. |
| 6 | `(None, false)` case comment was misleading | 4-way match eliminated (see #3); the `None` path is now self-documenting. |
| 8 | `transformed.data.with_new_exprs(...)` was uncommented | Comment at lines 196-197: "Rebuild the plan keeping its rewritten expressions but replacing inputs with the new extraction projections." |
| 9 | Comment inconsistency: `__extracted_N` vs `__datafusion_extracted` | All comments now consistently use the full prefix `__datafusion_extracted` / `__datafusion_extracted_N`. |
| 10 | `pairs.len() + columns_needed.len() == proj.expr.len()` could overcount | Completely reworked. A dedicated `proj_exprs_captured` counter (line 762) tracks only Case A (`__datafusion_extracted` aliases) and Case B (standalone `Expr::Column`) expressions. The merge guard is now `proj_exprs_captured == proj.expr.len()` (line 939). Additionally, `standalone_columns` (line 765) detects when `columns_needed` has entries from extracted alias internals that aren't standalone projection columns, triggering `needs_recovery` (line 850). Regression test at line 2929. |
| 11 | No Join tests | 9 Join tests: equijoin keys, non-equi filter, both-sides extraction, no-extraction, filter-above-join, projection-above-join, qualified right side, cross-input expression, left join. |
| 12 | No test for `(None, true)` recovery fallback | `test_projection_with_leaf_expr_above_aggregate` (line 1954) explicitly tests this: mixed projection with leaf expressions above Aggregate, which blocks pushdown — fires the in-place extraction + recovery path. |
| 13 | No test for cross-input expressions | `test_extract_from_join_cross_input_expression` (line 2390) tests a filter where each side of `=` references a different Join input. Confirms each expression is extracted and routed independently. |
| 14 | `test_extract_from_projection` optimized plan had spurious self-alias | Optimized plan now shows `Projection: leaf_udf(test.user, Utf8("name"))` without alias (line 1340). |
| 15 | `extract_from_plan` cloned all inputs before checking if transformation occurred | Inputs cloned only after `transformed.transformed` check (lines 168-179). Wrap directly in `Arc`, consume with `into_iter()` (line 183), recover without clone via `Arc::try_unwrap` (line 190). |
| 15b | Double-clone when extraction succeeds | Fixed. `Arc` wrapping happens once (line 178); `into_iter()` consumes the vec (line 183); `Arc::try_unwrap` (line 190) avoids a second clone when the refcount is 1. |

## Partially Fixed (1 of 17)

| # | Issue | What changed | What remains |
|---|-------|-------------|--------------|
| 1 | Two separate projection-building paths (`build_extraction_projection` vs `build_extraction_projection_impl`) | `build_extraction_projection` documents delegation (lines 490-492). `build_extraction_projection_impl` doc says "shared by both passes" (line 517). | `_impl` is called directly from `split_and_push_projection` (line 879) and `try_push_into_inputs` (line 1114), bypassing the wrapper. This is intentional — those callers already have pairs/columns extracted separately — but the two entry points with similar names can confuse new readers. |

## Still Present (Low Severity, 1 of 17)

| # | Issue | Severity | Details |
|---|-------|----------|---------|
| 7 | `routing_extract` only tracks `Expr::Column` variants in `columns_needed` | Low | Only `Expr::Column` nodes trigger pass-through tracking (line 271). The `ExpressionPlacement::Column` match arm (line 265) has an `if let Expr::Column` guard. In DataFusion today, `ExpressionPlacement::Column` is only returned by `Expr::Column`, so this is correct. If a future expression variant returned `Column` placement without being `Expr::Column`, it would be silently skipped. |

## By-Design / Not Issues

| # | Original issue | Why it's fine |
|---|---------------|---------------|
| 16 | `schema_columns` allocates both qualified and unqualified forms | By design — enables `has_all_column_refs` to work with both qualified and unqualified column references. The ambiguity this creates in multi-input routing is now handled by `find_owning_input` returning `None` on ambiguity (#4 fix). |
| 17 | Recursive `try_push_input` calls with no explicit depth limit | By design — recursion at lines 959 and 1134 is bounded by plan tree depth and guard conditions (`is_pure_extraction_projection`, `None` on barriers). Each call pushes the projection exactly one level deeper, and the plan tree is finite. |
