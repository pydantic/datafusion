# Proposal: split `datafusion-proto` and move serialization onto `PhysicalExpr`

## Problem

`datafusion-proto` serializes every built-in `PhysicalExpr` through a single ~300-line `downcast_ref` chain in [`serialize_physical_expr_with_converter`](https://github.com/apache/datafusion/blob/main/datafusion/proto/src/physical_plan/to_proto.rs#L255), with a symmetric `match` on `ExprType` in `from_proto.rs`. Because the serializer lives outside the crate where each expression is defined, **every piece of internal state an expression wants to round-trip has to be `pub`**.

The concrete incident that motivated this issue was https://github.com/apache/datafusion/pull/21807, where supporting proto round-tripping of `DynamicFilterPhysicalExpr` required exposing `pub struct Inner`, `pub fn inner()`, `pub fn from_parts()`, `pub fn original_children()`, `pub fn remapped_children()` — all marked "warning: not stable; proto-only" in the docstrings. See discussion at https://github.com/apache/datafusion/pull/21807#discussion_r3138256061 / https://github.com/apache/datafusion/pull/21807#discussion_r3139926953.

The underlying issue is not specific to `DynamicFilterPhysicalExpr`. Any stateful expression (one whose round-trip state isn't expressible through its normal public constructor) will hit the same wall. It also means:

- Built-in expressions use a big central switch; third-party expressions go through `PhysicalExtensionCodec::try_encode_expr`. Two different shapes for the same thing.
- Every new built-in `PhysicalExpr` requires editing `datafusion-proto`, which many contributors don't touch for their "real" change.
- The `.proto` schema for each expression lives far away from the expression itself.

## Proposal

Two changes, in sequence.

### 1. Extract `datafusion-proto-models`

Mirror the existing `datafusion-proto-common` split, but for the physical/logical plan schemas. The new crate contains only:

- the `.proto` file(s) and the `prost`-generated Rust types,
- optional `pbjson`/`serde` derives behind a `json` feature,
- zero datafusion deps beyond `datafusion-proto-common`.

`datafusion-proto` keeps its current public API by re-exporting from `datafusion-proto-models`. Downstream consumers (`datafusion-ffi`, `datafusion-examples`, `benchmarks`) need no changes.

This is a pure refactor — no semantic change, no behavior change.

### 2. Add `PhysicalExpr::to_proto` (feature-gated)

Add a method to the `PhysicalExpr` trait, gated on a new `proto` feature:

```rust
#[cfg(feature = "proto")]
fn to_proto(
    &self,
    ctx: &dyn PhysicalExprEncoder,
) -> Result<Option<PhysicalExprNode>> {
    Ok(None)
}
```

- Default returns `Ok(None)` → "fall through to the existing codec path" (matches today's behavior for extension expressions).
- Explicit `Ok(Some(...))` → the expression has serialized itself.
- Expressions that should serialize but don't implement this override the default with `internal_err!("{typename} does not implement to_proto")`.

`PhysicalExprEncoder` is a small trait defined in `datafusion-proto-models`, wrapping the existing `PhysicalExtensionCodec` + `PhysicalProtoConverterExtension` plumbing, with helpers for `encode_child`, `encode_udf`, `encode_udaf`, `encode_udwf`. This keeps `physical-expr-common` free of `datafusion-proto` as a dep.

The existing `serialize_physical_expr_with_converter` tries `expr.to_proto(ctx)` first; if it returns `Ok(None)`, it falls through to the current downcast chain. This lets expressions migrate one at a time without breaking anything.

### What this unlocks

- **Private state stays private.** `DynamicFilterPhysicalExpr::to_proto` reads the `RwLock` directly — no more `pub struct Inner`. Once migrated, #21807's "pub for proto" scaffolding (`Inner`, `from_parts`, `inner()`, `original_children`, `remapped_children`) can be reverted.
- **One-file changes.** Adding serialization for a new expression is a method impl next to the expression, not a round trip through `datafusion-proto`.
- **Parity between built-in and third-party.** Everyone uses the same hook shape.

## Decode side (follow-up)

The encode-side win is clear. The decode side is still a central `match` on `ExprType`, which is fine — `oneof ExprType` gives us an exhaustive Rust enum, a strict improvement over today's runtime downcast chain.

As a follow-up (probably best after the encode migrations land), we can push decoding back to the expressions too, via associated fns like:

```rust
impl BinaryExpr {
    fn from_proto(
        proto: &PhysicalBinaryExprNode,
        ctx: &ProtoDecodeCtx<'_>,
    ) -> Result<Arc<Self>> { ... }
}
```

The central match becomes a dispatch table. The main payoff is that **public god-constructors for private state go away**: `DynamicFilterPhysicalExpr::from_proto` reads the proto fields and builds `Inner` internally, so we can fully delete the `from_parts`/`Inner`-as-pub scaffolding.

## Alternatives considered

1. **Bytes-level `try_encode_self` hook, no `prost` in `physical-expr`.** Each expression encodes into an opaque `Vec<u8>`. Preserves privacy without pulling `prost` into more crates. Downside: you lose the single `.proto` file as the schema source of truth — cross-language debuggability and interop suffer. Rejected.
2. **Keep the central switch, mark unstable accessors `#[doc(hidden)]`.** Cheap. Doesn't fix the third-party/built-in asymmetry or the central-edit-per-new-expression problem. Reasonable as a stopgap, not a solution.
3. **Separate `PhysicalExprProto` trait with a blanket fallback impl.** Would avoid adding to `PhysicalExpr`. Requires a runtime "does it implement this" check (narrow `Any` cast). Slightly more complex than option (2) above and delivers the same thing.

## Scope

This issue proposes the change only for `PhysicalExpr`. `ExecutionPlan`, `LogicalPlan`, and logical `Expr` have the same shape and would benefit from the same treatment, but each is its own migration (~40 `ExecutionPlan` impls alone). Worth tackling after the PhysicalExpr path is proven.

`datafusion-substrait` is unaffected — it operates independently and does not share code with `datafusion-proto`.

## Open questions

- Should `PhysicalExprEncoder` live in `datafusion-proto-models` or be a narrow sub-trait that `PhysicalExtensionCodec` re-implements? Leaning toward the former for minimum dep surface.
- Window / aggregate expressions are serialized through a separate code path in `to_proto.rs` and are not regular `PhysicalExpr` impls. Probably get a parallel `WindowExpr::to_proto` hook, or stay central. Defer until the core `PhysicalExpr` migration is done.
- `proto` feature default. I'd default it off on `physical-expr-common`/`physical-expr`/`physical-plan` (so developers who don't care pay nothing) and flip it on from `datafusion-proto`. CI would test with the feature on; off-mode is a "it still compiles" smoke test.

## Related

- #17713 (remove `datafusion` dependency from `datafusion-proto`) — this refactor is consistent with that direction.
- #21807 (the PR that motivated this discussion).
- #20418 (serialize+dedupe dynamic filters — the underlying feature work #21807 addresses).
