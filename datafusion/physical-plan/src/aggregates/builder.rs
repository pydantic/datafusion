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

//! [`AggregateExecBuilder`]: build and rewrite [`AggregateExec`] nodes

use std::sync::Arc;

use super::{
    AggrDynFilter, AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
    create_schema, topk_types_supported,
};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::{ExecutionPlan, InputOrderMode, PlanProperties};

use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err, plan_err};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;

/// Builder for [`AggregateExec`].
///
/// This is the recommended way to create an [`AggregateExec`], and the only
/// supported way to derive a new [`AggregateExec`] from an existing one (see
/// [`AggregateExec::to_builder`]).
///
/// Compared to calling [`AggregateExec::try_new`] and then mutating individual
/// fields, the builder:
///
/// 1. Names every argument, so `input` / `input_schema` and `aggr_expr` /
///    `filter_expr` can't be transposed by accident.
/// 2. Defaults `filter_expr` to "no filter for each aggregate", which is what
///    the vast majority of callers want and removes a common source of
///    length-mismatch panics.
/// 3. Validates the plan once, at the end, so combinations that would panic or
///    return an internal error during execution (for example a limit pushed
///    into an aggregate that cannot execute it) are rejected up front.
/// 4. Keeps derived state (output schema, plan properties, ordering
///    requirements, dynamic filter) consistent when rewriting an existing
///    node, instead of asking every caller to copy the fields by hand.
///
/// # Example: creating a new aggregate
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_physical_plan::aggregates::{
/// #     AggregateExec, AggregateMode, PhysicalGroupBy,
/// # };
/// # use datafusion_physical_plan::empty::EmptyExec;
/// # use datafusion_physical_expr::expressions::col;
/// # fn main() -> datafusion_common::Result<()> {
/// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
/// let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
/// let group_by =
///     PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
///
/// let exec = AggregateExec::builder(AggregateMode::Single, input)
///     .with_group_by(group_by)
///     .build()?;
/// assert_eq!(exec.mode(), &AggregateMode::Single);
/// # Ok(())
/// # }
/// ```
///
/// # Example: rewriting an existing aggregate
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_physical_plan::aggregates::{
/// #     AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
/// # };
/// # use datafusion_physical_plan::{ExecutionPlan, empty::EmptyExec};
/// # use datafusion_physical_expr::expressions::col;
/// # fn main() -> datafusion_common::Result<()> {
/// # let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
/// # let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
/// # let group_by =
/// #     PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
/// # let exec = AggregateExec::builder(AggregateMode::Single, input)
/// #     .with_group_by(group_by)
/// #     .build()?;
/// // push a limit into an existing `SELECT DISTINCT a`-style aggregate
/// let limited = exec
///     .to_builder()
///     .with_limit_options(LimitOptions::new(10))
///     .build()?;
/// assert_eq!(limited.limit_options().map(|o| o.limit()), Some(10));
/// // the rewritten node keeps the original output schema
/// assert_eq!(limited.schema(), exec.schema());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct AggregateExecBuilder {
    mode: AggregateMode,
    group_by: Arc<PhysicalGroupBy>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// `None` means "no filter for any aggregate expression"
    filter_expr: Option<Vec<Option<Arc<dyn PhysicalExpr>>>>,
    input: Arc<dyn ExecutionPlan>,
    /// `None` means "the schema of `input`"
    input_schema: Option<SchemaRef>,
    limit_options: Option<LimitOptions>,
    /// Output schema explicitly supplied by the caller. Always honored, see
    /// [`AggregateExecBuilder::with_output_schema`].
    output_schema: Option<SchemaRef>,
    /// State carried over from the [`AggregateExec`] this builder was derived
    /// from. Dropped as soon as a field it depends on changes, see
    /// [`AggregateExecBuilder::invalidate_derived`].
    derived: Option<DerivedState>,
    /// Whether the aggregate expressions were replaced. Only used to decide
    /// whether a derived output schema still has to be checked.
    aggr_expr_replaced: bool,
}

/// State of an [`AggregateExec`] that is computed from its inputs, and which is
/// preserved verbatim when a node is rewritten without touching the inputs it
/// depends on.
#[derive(Debug, Clone)]
struct DerivedState {
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    required_input_ordering: Option<OrderingRequirements>,
    input_order_mode: InputOrderMode,
    dynamic_filter: Option<Arc<AggrDynFilter>>,
}

impl AggregateExecBuilder {
    /// Create a builder for an aggregate over `input`.
    ///
    /// Unless overridden, the aggregate has no group by expressions, no
    /// aggregate expressions, no filters, no limit, and uses the schema of
    /// `input` as its [input schema](AggregateExec::input_schema).
    pub fn new(mode: AggregateMode, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            mode,
            group_by: Arc::new(PhysicalGroupBy::default()),
            aggr_expr: vec![],
            filter_expr: None,
            input,
            input_schema: None,
            limit_options: None,
            output_schema: None,
            derived: None,
            aggr_expr_replaced: false,
        }
    }

    /// Create a builder pre-populated from `exec`.
    ///
    /// The derived state of `exec` (output schema, plan properties, ordering
    /// requirements and dynamic filter) is reused unless a field it depends on
    /// is replaced. Execution metrics are always reset, since `build` returns a
    /// new plan node.
    pub(crate) fn from_exec(exec: &AggregateExec) -> Self {
        Self {
            mode: exec.mode,
            group_by: Arc::clone(&exec.group_by),
            aggr_expr: exec.aggr_expr.to_vec(),
            filter_expr: Some(exec.filter_expr.to_vec()),
            input: Arc::clone(&exec.input),
            input_schema: Some(Arc::clone(&exec.input_schema)),
            limit_options: exec.limit_options,
            output_schema: None,
            derived: Some(DerivedState {
                schema: Arc::clone(&exec.schema),
                cache: Arc::clone(&exec.cache),
                required_input_ordering: exec.required_input_ordering.clone(),
                input_order_mode: exec.input_order_mode.clone(),
                dynamic_filter: exec.dynamic_filter.clone(),
            }),
            aggr_expr_replaced: false,
        }
    }

    /// Set the [`AggregateMode`].
    pub fn with_mode(mut self, mode: AggregateMode) -> Self {
        self.mode = mode;
        self.invalidate_derived()
    }

    /// Set the group by expressions.
    pub fn with_group_by(mut self, group_by: impl Into<Arc<PhysicalGroupBy>>) -> Self {
        self.group_by = group_by.into();
        self.invalidate_derived()
    }

    /// Set the aggregate expressions.
    ///
    /// When the builder was created with [`AggregateExec::to_builder`], the
    /// output schema of the original node is kept, so that rewriting the
    /// aggregate expressions (for example reversing them in
    /// `OptimizeAggregateOrder`) cannot change output field names. `build`
    /// verifies that the new expressions still produce a compatible schema.
    pub fn with_aggr_exprs(
        mut self,
        aggr_expr: impl IntoIterator<Item = Arc<AggregateFunctionExpr>>,
    ) -> Self {
        self.aggr_expr = aggr_expr.into_iter().collect();
        self.aggr_expr_replaced = true;
        self
    }

    /// Set the `FILTER` expression of each aggregate expression.
    ///
    /// Must have the same length as the aggregate expressions; `build` returns
    /// an error otherwise. If never called, no aggregate is filtered.
    pub fn with_filter_exprs(
        mut self,
        filter_expr: impl IntoIterator<Item = Option<Arc<dyn PhysicalExpr>>>,
    ) -> Self {
        self.filter_expr = Some(filter_expr.into_iter().collect());
        self.invalidate_derived()
    }

    /// Set the input plan.
    pub fn with_input(mut self, input: Arc<dyn ExecutionPlan>) -> Self {
        self.input = input;
        self.invalidate_derived()
    }

    /// Set the [input schema](AggregateExec::input_schema): the schema of the
    /// data *before* any aggregation is applied.
    ///
    /// For `Partial` and `Single` aggregates this is the schema of the input
    /// plan (the default). For `Final` and `FinalPartitioned` aggregates it is
    /// the input schema of the matching partial aggregate, which is *not* the
    /// schema of the input plan.
    pub fn with_input_schema(mut self, input_schema: SchemaRef) -> Self {
        self.input_schema = Some(input_schema);
        self
    }

    /// Set the limit pushed down into this aggregate, or `None` to remove it.
    ///
    /// The limit is a hint: operators above the aggregate still enforce it.
    /// `build` rejects limits this aggregate cannot execute, rather than
    /// letting them fail (or be silently ignored) at execution time.
    ///
    /// Accepts both `LimitOptions` and `Option<LimitOptions>`.
    pub fn with_limit_options(
        mut self,
        limit_options: impl Into<Option<LimitOptions>>,
    ) -> Self {
        self.limit_options = limit_options.into();
        self
    }

    /// Use `schema` as the output schema instead of computing it.
    ///
    /// Used when decoding a serialized plan, where the output schema is part of
    /// the message and must be preserved exactly. The caller is responsible for
    /// the schema matching the aggregate; prefer letting `build` compute it.
    #[cfg_attr(not(feature = "proto"), allow(dead_code))]
    pub(crate) fn with_output_schema(mut self, schema: SchemaRef) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Drop state derived from the node this builder came from, because a field
    /// it is computed from was replaced.
    fn invalidate_derived(mut self) -> Self {
        self.derived = None;
        self
    }

    /// Build the [`AggregateExec`], validating it.
    pub fn build(self) -> Result<AggregateExec> {
        let Self {
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
            limit_options,
            output_schema,
            derived,
            aggr_expr_replaced,
        } = self;

        let input_schema = input_schema.unwrap_or_else(|| input.schema());
        let filter_expr = filter_expr
            .unwrap_or_else(|| std::iter::repeat_n(None, aggr_expr.len()).collect());

        let mut exec = match (output_schema, derived) {
            // An explicitly supplied output schema is honored as is, everything
            // else is computed from the inputs.
            (Some(schema), _) => AggregateExec::try_new_with_schema(
                mode,
                group_by,
                aggr_expr,
                filter_expr,
                input,
                input_schema,
                schema,
            )?,
            // Nothing the derived state depends on changed: clone the node this
            // builder came from with the new values instead of recomputing. In
            // particular its output schema is kept, so a rewrite of the
            // aggregate expressions cannot rename output fields.
            (None, Some(derived)) => {
                assert_eq_or_internal_err!(
                    aggr_expr.len(),
                    filter_expr.len(),
                    "Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match",
                    aggr_expr,
                    filter_expr
                );
                if aggr_expr_replaced {
                    check_schema_compatible(
                        &derived.schema,
                        &input,
                        &group_by,
                        &aggr_expr,
                        mode,
                    )?;
                }
                AggregateExec {
                    mode,
                    group_by,
                    aggr_expr: aggr_expr.into(),
                    filter_expr: filter_expr.into(),
                    input,
                    schema: derived.schema,
                    input_schema,
                    metrics: ExecutionPlanMetricsSet::new(),
                    required_input_ordering: derived.required_input_ordering,
                    input_order_mode: derived.input_order_mode,
                    cache: derived.cache,
                    limit_options: None,
                    dynamic_filter: derived.dynamic_filter,
                }
            }
            (None, None) => AggregateExec::try_new(
                mode,
                group_by,
                aggr_expr,
                filter_expr,
                input,
                input_schema,
            )?,
        };

        exec.limit_options = limit_options;
        validate_limit_options(&exec)?;
        Ok(exec)
    }
}

/// Verify that `schema`, carried over from the node a builder was derived from,
/// still describes the output of the (replaced) aggregate expressions.
///
/// Field *names* are allowed to differ: preserving them is the whole point of
/// carrying the schema over. Anything else means the rewrite produced a
/// different aggregate and the schema must not be reused.
fn check_schema_compatible(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
    group_by: &PhysicalGroupBy,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    mode: AggregateMode,
) -> Result<()> {
    let computed = create_schema(&input.schema(), group_by, aggr_expr, mode)?;
    let compatible =
        computed.fields().len() == schema.fields().len()
            && computed.fields().iter().zip(schema.fields()).all(
                |(computed, existing)| {
                    computed.data_type() == existing.data_type()
                        && computed.is_nullable() == existing.is_nullable()
                },
            );
    if !compatible {
        return internal_err!(
            "New aggregate expressions are not compatible with the output schema of the \
             aggregate they replace.\nExpected: {schema}\nGot: {computed}"
        );
    }
    Ok(())
}

/// Reject [`LimitOptions`] that `exec` cannot execute.
///
/// A limit is only pushed into an aggregate by the optimizer, but nothing stops
/// another rule (or an external consumer) from copying one onto an aggregate
/// with a different shape. Without this check such a plan builds fine and then
/// panics, errors, or silently drops `FILTER` clauses when it is executed, so
/// the conditions the limited execution paths rely on are checked here, once.
fn validate_limit_options(exec: &AggregateExec) -> Result<()> {
    let Some(limit_options) = exec.limit_options else {
        return Ok(());
    };

    // Aggregates without a group by produce a single row: the limit is a no-op
    // and is ignored by `AggregateStream`.
    if exec.group_by.is_true_no_grouping() {
        return Ok(());
    }

    // A soft limit on a distinct-style aggregate: the hash streams stop
    // accumulating new groups once they have enough, no further requirements.
    if exec.is_unordered_unfiltered_group_by_distinct() {
        return Ok(());
    }

    // Everything else is executed by `GroupedTopKAggregateStream`, which keeps
    // a bounded priority queue of `(group key, min/max value)` pairs.
    let group_exprs = exec.group_by.expr();
    if group_exprs.len() != 1 || exec.group_by.has_grouping_set() {
        return plan_err!(
            "Aggregate with a limit of {} requires exactly one group by expression, found {}",
            limit_options.limit(),
            group_exprs.len()
        );
    }
    // `GroupedTopKAggregateStream` evaluates the aggregate arguments directly
    // and would silently ignore the filters.
    if exec.filter_expr.iter().any(|filter| filter.is_some()) {
        return plan_err!(
            "Aggregate with a limit of {} cannot have FILTER expressions",
            limit_options.limit()
        );
    }

    let minmax_desc = exec.get_minmax_desc();
    if minmax_desc.is_none() && !exec.aggr_expr.is_empty() {
        return plan_err!(
            "Aggregate with a limit of {} supports a single MIN/MAX aggregate expression \
             or no aggregate expressions at all",
            limit_options.limit()
        );
    }
    // Without a MIN/MAX aggregate to order by, the priority queue is ordered by
    // the group key and the direction has to come from the limit itself.
    if minmax_desc.is_none() && limit_options.descending().is_none() {
        return plan_err!(
            "Aggregate with a limit of {} and no MIN/MAX aggregate expression requires an \
             ordering direction, use `LimitOptions::new_with_order`",
            limit_options.limit()
        );
    }

    let (group_expr, _) = &group_exprs[0];
    let key_type = group_expr.data_type(&exec.input.schema())?;
    let value_type = match &minmax_desc {
        Some((field, _)) => field.data_type().clone(),
        None => key_type.clone(),
    };
    if !topk_types_supported(&key_type, &value_type) {
        return plan_err!(
            "Aggregate with a limit of {} does not support group key type {key_type} with \
             value type {value_type}",
            limit_options.limit()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;
    use crate::test::TestMemoryExec;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn test_input(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::clone(schema)))
    }

    fn group_by_a(schema: &SchemaRef) -> Result<PhysicalGroupBy> {
        Ok(PhysicalGroupBy::new_single(vec![(
            col("a", schema)?,
            "a".to_string(),
        )]))
    }

    fn min_b(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(min_udaf(), vec![col("b", schema)?])
                .schema(Arc::clone(schema))
                .alias("min_b")
                .build()?,
        ))
    }

    fn count_b(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("b", schema)?])
                .schema(Arc::clone(schema))
                .alias("count_b")
                .build()?,
        ))
    }

    /// `filter_expr` defaults to "no filter" instead of having to be a vector of
    /// `None`s of exactly the right length.
    #[test]
    fn filter_exprs_default_to_none() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?, count_b(&schema)?])
            .build()?;
        assert_eq!(exec.filter_expr(), &[None, None]);
        Ok(())
    }

    #[test]
    fn mismatched_filter_exprs_are_rejected() -> Result<()> {
        let schema = test_schema();
        let err = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .with_filter_exprs(vec![])
            .build()
            .unwrap_err();
        assert!(
            err.message().contains("their size should match"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    /// Rewriting a node keeps the output schema and plan properties of the node
    /// it was derived from, and resets its metrics.
    #[test]
    fn rewriting_preserves_derived_state() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .build()?;

        let limited = exec
            .to_builder()
            .with_limit_options(LimitOptions::new(10))
            .build()?;

        assert_eq!(limited.limit_options(), Some(LimitOptions::new(10)));
        assert_eq!(limited.schema(), exec.schema());
        assert_eq!(limited.input_schema(), exec.input_schema());
        assert_eq!(limited.mode(), exec.mode());
        // the plan properties were reused rather than recomputed
        assert!(Arc::ptr_eq(&limited.cache, &exec.cache));
        // but the metrics of the original node were not carried over
        assert_eq!(limited.metrics().unwrap().iter().count(), 0);
        Ok(())
    }

    /// Changing the mode is a structural change: the output schema of a
    /// `Partial` aggregate holds intermediate state, so it must be recomputed.
    #[test]
    fn changing_the_mode_recomputes_the_schema() -> Result<()> {
        let schema = test_schema();
        let partial = AggregateExec::builder(AggregateMode::Partial, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![count_b(&schema)?])
            .build()?;
        let single = partial
            .to_builder()
            .with_mode(AggregateMode::Single)
            .build()?;

        // `Partial` emits the accumulator state, `Single` the final count
        assert_eq!(partial.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(single.schema().field(1).data_type(), &DataType::Int64);
        assert_ne!(
            partial.schema().field(1).name(),
            single.schema().field(1).name()
        );
        Ok(())
    }

    /// Replacing the aggregate expressions keeps the original output field
    /// names, but expressions that would change the output are rejected rather
    /// than silently producing a node whose schema lies about its output.
    #[test]
    fn incompatible_aggr_exprs_are_rejected() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .build()?;

        // same output type, different name: allowed, the name is preserved
        let renamed = exec
            .to_builder()
            .with_aggr_exprs(vec![Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("something_else")
                    .build()?,
            )])
            .build()?;
        assert_eq!(renamed.schema(), exec.schema());

        // different output field: rejected (`count` is not nullable)
        let err = exec
            .to_builder()
            .with_aggr_exprs(vec![count_b(&schema)?])
            .build()
            .unwrap_err();
        assert!(
            err.message()
                .contains("not compatible with the output schema"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    /// A limit is executed as a soft limit by the hash streams for
    /// `SELECT DISTINCT`-style aggregates, and by the TopK stream for a single
    /// MIN/MAX aggregate. Anything else cannot execute it.
    #[test]
    fn limit_is_validated_against_the_aggregate() -> Result<()> {
        let schema = test_schema();

        // distinct-style aggregate: a plain soft limit is fine
        AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_limit_options(LimitOptions::new(10))
            .build()?;

        // single MIN/MAX aggregate: ordered by the aggregate value
        AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .with_limit_options(LimitOptions::new(10))
            .build()?;

        // any other aggregate cannot execute a limit
        let err = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![count_b(&schema)?])
            .with_limit_options(LimitOptions::new(10))
            .build()
            .unwrap_err();
        assert!(
            err.message()
                .contains("single MIN/MAX aggregate expression"),
            "unexpected error: {err}"
        );

        // a FILTER would be ignored by the TopK stream
        let err = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .with_filter_exprs(vec![Some(col("a", &schema)?)])
            .with_limit_options(LimitOptions::new(10))
            .build()
            .unwrap_err();
        assert!(
            err.message().contains("cannot have FILTER expressions"),
            "unexpected error: {err}"
        );

        // more than one group by expression is not supported by the TopK stream
        let err = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(PhysicalGroupBy::new_single(vec![
                (col("a", &schema)?, "a".to_string()),
                (col("b", &schema)?, "b".to_string()),
            ]))
            .with_aggr_exprs(vec![min_b(&schema)?])
            .with_limit_options(LimitOptions::new(10))
            .build()
            .unwrap_err();
        assert!(
            err.message()
                .contains("requires exactly one group by expression"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    /// Without a MIN/MAX aggregate to order by, the TopK stream orders by the
    /// group key and needs a direction. Previously this built a plan that
    /// failed with an internal error at execution time.
    #[test]
    fn limit_without_ordering_on_an_ordered_aggregate_is_rejected() -> Result<()> {
        let schema = test_schema();
        let batch = arrow::record_batch::RecordBatch::new_empty(Arc::clone(&schema));
        let sort_key =
            datafusion_physical_expr::PhysicalSortExpr::new_default(col("a", &schema)?);
        let ordering =
            datafusion_physical_expr_common::sort_expr::LexOrdering::new([sort_key])
                .unwrap();
        // an input with an ordering makes the aggregate produce an ordered
        // output, so it is not an "unordered unfiltered group by distinct"
        let input = TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![ordering])?;
        let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)))
            as Arc<dyn ExecutionPlan>;

        let err = AggregateExec::builder(AggregateMode::Single, input)
            .with_group_by(group_by_a(&schema)?)
            .with_limit_options(LimitOptions::new(10))
            .build()
            .unwrap_err();
        assert!(
            err.message().contains("requires an ordering direction"),
            "unexpected error: {err}"
        );
        Ok(())
    }
}
