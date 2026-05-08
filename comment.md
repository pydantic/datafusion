One concern about the public API surface here: this changes the signature of `create_aggregate_expr_with_name_and_maybe_filter` (a `pub fn`) by adding a new `human_display_alias: Option<String>` parameter and changing `human_display` from `String` to `Option<String>`. That's a silent break for any downstream code that builds aggregate physical expressions through this entry point — and it isn't called out in `docs/source/library-user-guide/upgrading/54.0.0.md`.

Stepping back: I think the existence of `create_aggregate_expr_with_name_and_maybe_filter` *and* `create_aggregate_expr_and_maybe_filter` as parallel public free functions is already a smell. They're really one thing — "lower a logical aggregate `Expr` into a physical `AggregateFunctionExpr` plus its filter and order-by" — split awkwardly across two signatures. We already have `AggregateExprBuilder` for the pure physical-construction half. What's missing is a builder for the logical→physical lowering half.

Could we kill two birds with one stone in this PR by introducing a `LoweredAggregateBuilder`?

```rust
// datafusion-physical-expr/src/aggregate.rs
pub struct LoweredAggregateBuilder<'a> { /* expr + schemas + execution_props + overrides */ }

pub struct LoweredAggregate {
    pub aggregate: Arc<AggregateFunctionExpr>,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
    pub order_bys: Vec<PhysicalSortExpr>,
}

impl<'a> LoweredAggregateBuilder<'a> {
    pub fn new(expr: &'a Expr, logical_schema: &'a DFSchema, physical_schema: &'a Schema, execution_props: &'a ExecutionProps) -> Self;
    pub fn with_name(mut self, name: impl Into<String>) -> Self;
    pub fn build(self) -> Result<LoweredAggregate>;
}
```

Internally, `build()` does what these two functions do today: unwrap aliases, derive `name` / `human_display` / `human_display_alias`, lower args / filter / order-by via the existing `create_physical_*` helpers, then hand off to `AggregateExprBuilder`. All the new aliased-display logic from this PR lives here.

The proposed migration shape:

1. Add `LoweredAggregateBuilder` next to `AggregateExprBuilder` in `datafusion-physical-expr/src/aggregate.rs` (the `create_physical_*` helpers it needs already live in that crate, so no new dependencies).
2. **Revert the signature change** to `create_aggregate_expr_with_name_and_maybe_filter`. Keep both free functions on their pre-PR signatures.
3. Mark them `#[deprecated(note = "use LoweredAggregateBuilder")]`. They become thin delegations — the deprecated `_with_name_` variant passes its single `human_display` straight through with no alias, matching pre-PR behavior. The new aliased-display path is only reachable via `LoweredAggregateBuilder`.
4. Migrate the planner's own call site (the only in-tree caller) over to `LoweredAggregateBuilder` in this same PR, so deprecation warnings don't fire on internal code.

Why I'm suggesting this *in this PR* rather than a follow-up: this PR is *already* breaking the public surface. If we land it as-is and clean up later, downstream users pay the migration cost twice — once now for the new parameter, once again when we deprecate the function. Doing it in one shot means:

- Zero public signature breaks (`AggregateFunctionExpr::human_display() -> Option<&str>` is still a break and still needs the upgrade-guide entry, but the free-function break disappears).
- The upgrade guide gets a "prefer `LoweredAggregateBuilder`" pointer instead of a parameter-list diff.
- Future additions to aggregate construction stop touching public function signatures.

I realize this enlarges the PR and is partly scope creep on what was supposed to be an EXPLAIN fix — happy to help with the refactor (or pair with a committer who can) if it would be useful. What do you think?
