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

use arrow::array::{ArrayRef, new_null_array};
use datafusion_common::pruning::PruningStatistics;
use datafusion_expr::Expr;
use std::collections::{HashMap, HashSet};

use datafusion_common::error::DataFusionError;

/// A source of runtime statistical information for pruning.
///
/// This trait accepts a set of [`Expr`] expressions and returns
/// statistics for those expressions that can be used for pruning.
///
/// It is up to implementors to determine how to collect these statistics.
/// Some example use cases include:
/// 1. Matching on basic expressions like `min(column)` or `max(column)`
///    and returning statistics from file metadata.
/// 2. Sampling data at runtime to get more accurate statistics.
/// 3. Querying an external metastore for statistics.
///
/// # Supported expression types
///
/// The following expression types are meaningful for pruning:
///
/// - **Aggregate functions**: `min(column)`, `max(column)`,
///   `count(*) FILTER (WHERE column IS NULL)`,
///   `count(*) FILTER (WHERE column IS NOT NULL)`
/// - **InList**: `column IN (v1, v2, ...)` — see [InList semantics] below.
///
/// Implementors return `None` for any expression they cannot answer.
///
/// # InList semantics
///
/// For `column IN (v1, v2, ..., vN)`, the returned `BooleanArray` has one
/// entry per container with three-valued logic:
///
/// - `true` — the column in this container ONLY contains values in
///   `{v1, ..., vN}`. Every row in the container satisfies the `IN`
///   predicate (assuming non-null values; see below).
/// - `false` — the column in this container contains NONE of the values
///   in `{v1, ..., vN}`. No row can satisfy the `IN` predicate, so the
///   container can be pruned.
/// - `null` — it is not known whether the column contains any of the
///   values. The container cannot be pruned.
///
/// ## Null handling
///
/// - **Null values in the column**: SQL `IN` returns `NULL` when the
///   column value is `NULL`, regardless of the list contents. Containers
///   where the column has null values should return `null` (unknown)
///   unless the implementation can determine that all non-null values
///   still satisfy or violate the predicate.
/// - **Null values in the list** (`column IN (1, NULL, 3)`): Per SQL
///   semantics, `x IN (1, NULL, 3)` returns `TRUE` if `x` is 1 or 3,
///   `NULL` if `x` is any other non-null value (because `x = NULL` is
///   unknown), and `NULL` if `x` is `NULL`. Null literals in the list
///   therefore weaken pruning — a container can no longer return `false`
///   unless it can prove the column has no values at all.
/// - **`NOT IN` with nulls** (`column NOT IN (1, NULL, 3)`): This can
///   never return `TRUE` for non-null column values because `x != NULL`
///   is always unknown. A container can only be pruned if it is known
///   to contain exclusively values in the list.
#[async_trait::async_trait]
pub trait StatisticsSource: Send + Sync {
    /// Returns the number of containers (row groups, files, etc.) that
    /// statistics are provided for. All returned arrays must have this length.
    fn num_containers(&self) -> usize;

    /// Returns statistics for each expression, or `None` for expressions
    /// that cannot be answered.
    async fn expression_statistics(
        &self,
        expressions: &[Expr],
    ) -> Result<Vec<Option<ArrayRef>>, DataFusionError>;
}

/// Blanket implementation of [`StatisticsSource`] for types that implement
/// [`PruningStatistics`].
///
/// This allows any type that implements [`PruningStatistics`] to be used as
/// a [`StatisticsSource`] without needing to implement the trait directly.
///
/// The implementation matches on expressions that can be directly answered
/// by the underlying [`PruningStatistics`]:
/// - `min(column)` → [`PruningStatistics::min_values`]
/// - `max(column)` → [`PruningStatistics::max_values`]
/// - `count(*) FILTER (WHERE column IS NOT NULL)` → [`PruningStatistics::row_counts`]
/// - `count(*) FILTER (WHERE column IS NULL)` → [`PruningStatistics::null_counts`]
/// - `column IN (lit1, lit2, ...)` → [`PruningStatistics::contained`]
///
/// Any other expressions return `None`.
#[async_trait::async_trait]
impl<T: PruningStatistics + Send + Sync> StatisticsSource for T {
    fn num_containers(&self) -> usize {
        PruningStatistics::num_containers(self)
    }

    async fn expression_statistics(
        &self,
        expressions: &[Expr],
    ) -> Result<Vec<Option<ArrayRef>>, DataFusionError> {
        Ok(expressions
            .iter()
            .map(|expr| resolve_expression_sync(self, expr))
            .collect())
    }
}

/// Pre-resolved statistics cache. Created asynchronously via
/// [`StatisticsSource`], evaluated synchronously by
/// [`PruningPredicate::evaluate`].
///
/// Keyed by [`Expr`] so that a single cache can serve multiple
/// [`PruningPredicate`](crate::PruningPredicate) instances (e.g., after dynamic filter changes
/// rebuild the predicate but reuse the same resolved stats).
/// Missing entries are treated as unknown — safe for pruning
/// (the predicate will conservatively keep the container).
///
/// [`PruningPredicate::evaluate`]: crate::PruningPredicate::evaluate
pub struct ResolvedStatistics {
    num_containers: usize,
    cache: HashMap<Expr, ArrayRef>,
}

impl ResolvedStatistics {
    /// Create an empty cache with no resolved statistics.
    /// All lookups will return `None`, causing `evaluate()` to use
    /// null arrays (conservative — no pruning).
    pub fn new_empty(num_containers: usize) -> Self {
        Self {
            num_containers,
            cache: HashMap::new(),
        }
    }

    /// Resolve statistics for the given expressions from an async source.
    pub async fn resolve(
        source: &(impl StatisticsSource + ?Sized),
        expressions: &[Expr],
    ) -> Result<Self, DataFusionError> {
        let num_containers = source.num_containers();
        let arrays = source.expression_statistics(expressions).await?;
        let cache = expressions
            .iter()
            .zip(arrays)
            .filter_map(|(expr, arr)| arr.map(|a| (expr.clone(), a)))
            .collect();
        Ok(Self {
            num_containers,
            cache,
        })
    }

    /// Look up a resolved expression. Returns `None` if not in cache.
    pub fn get(&self, expr: &Expr) -> Option<&ArrayRef> {
        self.cache.get(expr)
    }

    /// Look up a resolved expression, returning a null array of the given
    /// type if the expression is not in the cache.
    pub fn get_or_null(
        &self,
        expr: &Expr,
        data_type: &arrow::datatypes::DataType,
    ) -> ArrayRef {
        self.cache
            .get(expr)
            .cloned()
            .unwrap_or_else(|| new_null_array(data_type, self.num_containers))
    }

    /// Returns the number of containers these statistics cover.
    pub fn num_containers(&self) -> usize {
        self.num_containers
    }
}

/// Resolve a single expression synchronously against a [`PruningStatistics`] impl.
pub(crate) fn resolve_expression_sync(
    stats: &(impl PruningStatistics + ?Sized),
    expr: &Expr,
) -> Option<ArrayRef> {
    match expr {
        Expr::AggregateFunction(func) => resolve_aggregate_function(stats, func),
        Expr::InList(in_list) => resolve_in_list(stats, in_list),
        _ => None,
    }
}

/// Resolve all expressions synchronously against a [`PruningStatistics`] impl,
/// returning a [`ResolvedStatistics`] cache.
pub(crate) fn resolve_all_sync(
    stats: &(impl PruningStatistics + ?Sized),
    expressions: &[Expr],
) -> ResolvedStatistics {
    let num_containers = stats.num_containers();
    let cache = expressions
        .iter()
        .filter_map(|expr| {
            resolve_expression_sync(stats, expr).map(|arr| (expr.clone(), arr))
        })
        .collect();
    ResolvedStatistics {
        num_containers,
        cache,
    }
}

/// Resolve an aggregate function expression against [`PruningStatistics`].
fn resolve_aggregate_function(
    stats: &(impl PruningStatistics + ?Sized),
    func: &datafusion_expr::expr::AggregateFunction,
) -> Option<ArrayRef> {
    use datafusion_functions_aggregate::count::Count;
    use datafusion_functions_aggregate::min_max::{Max, Min};

    let udaf = func.func.inner();

    if udaf.as_any().downcast_ref::<Min>().is_some() {
        // min(column) — reject if there's a filter
        if func.params.filter.is_some() {
            return None;
        }
        if let Some(Expr::Column(col)) = func.params.args.first() {
            return stats.min_values(col);
        }
    } else if udaf.as_any().downcast_ref::<Max>().is_some() {
        // max(column) — reject if there's a filter
        if func.params.filter.is_some() {
            return None;
        }
        if let Some(Expr::Column(col)) = func.params.args.first() {
            return stats.max_values(col);
        }
    } else if udaf.as_any().downcast_ref::<Count>().is_some()
        && let Some(filter) = &func.params.filter
    {
        match filter.as_ref() {
            // count(*) FILTER (WHERE col IS NOT NULL) → row_counts
            Expr::IsNotNull(inner) => {
                if let Expr::Column(col) = inner.as_ref() {
                    return stats.row_counts(col);
                }
            }
            // count(*) FILTER (WHERE col IS NULL) → null_counts
            Expr::IsNull(inner) => {
                if let Expr::Column(col) = inner.as_ref() {
                    return stats.null_counts(col);
                }
            }
            _ => {}
        }
    }

    None
}

/// Resolve an `IN` list expression against [`PruningStatistics::contained`].
///
/// Only supports `column IN (literal, literal, ...)`. Returns `None` for
/// expressions with non-column left-hand sides or non-literal list items.
///
/// For `NOT IN`, the result is inverted: `true` becomes `false` and vice
/// versa, while `null` stays `null`. See the [InList semantics] section
/// on [`StatisticsSource`] for details on null handling.
fn resolve_in_list(
    stats: &(impl PruningStatistics + ?Sized),
    in_list: &datafusion_expr::expr::InList,
) -> Option<ArrayRef> {
    // Only support `column IN (literal, literal, ...)`
    let Expr::Column(col) = in_list.expr.as_ref() else {
        return None;
    };

    let mut values = HashSet::with_capacity(in_list.list.len());
    for item in &in_list.list {
        match item {
            Expr::Literal(scalar, _) => {
                values.insert(scalar.clone());
            }
            _ => return None, // non-literal in list, can't resolve
        }
    }

    let result = stats.contained(col, &values)?;
    if in_list.negated {
        // NOT IN: invert the contained result
        let inverted = arrow::compute::not(&result).ok()?;
        Some(std::sync::Arc::new(inverted) as ArrayRef)
    } else {
        Some(std::sync::Arc::new(result) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, UInt64Array};
    use arrow::datatypes::DataType;
    use datafusion_common::pruning::PruningStatistics;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::ExprFunctionExt;
    use std::sync::Arc;

    /// A simple mock PruningStatistics for testing.
    #[derive(Debug)]
    struct MockStats {
        min: ArrayRef,
        max: ArrayRef,
        null_counts: ArrayRef,
        row_counts: ArrayRef,
        contained_result: Option<BooleanArray>,
    }

    impl MockStats {
        fn new() -> Self {
            Self {
                min: Arc::new(Int64Array::from(vec![Some(1), Some(10)])),
                max: Arc::new(Int64Array::from(vec![Some(5), Some(20)])),
                null_counts: Arc::new(UInt64Array::from(vec![0, 2])),
                row_counts: Arc::new(UInt64Array::from(vec![100, 100])),
                contained_result: None,
            }
        }

        fn with_contained(mut self, result: BooleanArray) -> Self {
            self.contained_result = Some(result);
            self
        }
    }

    impl PruningStatistics for MockStats {
        fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.min))
        }
        fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.max))
        }
        fn num_containers(&self) -> usize {
            self.min.len()
        }
        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.null_counts))
        }
        fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.row_counts))
        }
        fn contained(
            &self,
            _column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            self.contained_result.clone()
        }
    }

    fn col_expr(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    #[test]
    fn test_resolve_min() {
        let stats = MockStats::new();
        let expr =
            datafusion_functions_aggregate::min_max::min_udaf().call(vec![col_expr("a")]);
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
        let arr = result.unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_resolve_max() {
        let stats = MockStats::new();
        let expr =
            datafusion_functions_aggregate::min_max::max_udaf().call(vec![col_expr("a")]);
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
        let arr = result.unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_resolve_count_null() {
        let stats = MockStats::new();
        let expr = datafusion_functions_aggregate::count::count_udaf()
            .call(vec![Expr::Literal(ScalarValue::Boolean(Some(true)), None)])
            .filter(Expr::IsNull(Box::new(col_expr("a"))))
            .build()
            .unwrap();
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_count_not_null() {
        let stats = MockStats::new();
        let expr = datafusion_functions_aggregate::count::count_udaf()
            .call(vec![Expr::Literal(ScalarValue::Boolean(Some(true)), None)])
            .filter(Expr::IsNotNull(Box::new(col_expr("a"))))
            .build()
            .unwrap();
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_unsupported_returns_none() {
        let stats = MockStats::new();
        // A plain column is not a supported expression for stats
        let result = resolve_expression_sync(&stats, &col_expr("a"));
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_min_with_filter_returns_none() {
        let stats = MockStats::new();
        // min(a) FILTER (WHERE a > 0) — not supported
        let expr = datafusion_functions_aggregate::min_max::min_udaf()
            .call(vec![col_expr("a")])
            .filter(col_expr("a").gt(Expr::Literal(ScalarValue::Int64(Some(0)), None)))
            .build()
            .unwrap();
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_in_list() {
        let stats = MockStats::new()
            .with_contained(BooleanArray::from(vec![Some(true), Some(false)]));
        let expr = Expr::InList(datafusion_expr::expr::InList::new(
            Box::new(col_expr("a")),
            vec![
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                Expr::Literal(ScalarValue::Int64(Some(2)), None),
            ],
            false,
        ));
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
        let arr = result.unwrap();
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0));
        assert!(!bool_arr.value(1));
    }

    #[test]
    fn test_resolve_not_in_list() {
        let stats = MockStats::new()
            .with_contained(BooleanArray::from(vec![Some(true), Some(false)]));
        let expr = Expr::InList(datafusion_expr::expr::InList::new(
            Box::new(col_expr("a")),
            vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)],
            true, // negated
        ));
        let result = resolve_expression_sync(&stats, &expr);
        assert!(result.is_some());
        let arr = result.unwrap();
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        // Inverted: true→false, false→true
        assert!(!bool_arr.value(0));
        assert!(bool_arr.value(1));
    }

    #[test]
    fn test_resolve_all_sync_builds_cache() {
        let stats = MockStats::new();
        let exprs = vec![
            datafusion_functions_aggregate::min_max::min_udaf().call(vec![col_expr("a")]),
            datafusion_functions_aggregate::min_max::max_udaf().call(vec![col_expr("a")]),
            col_expr("unsupported"), // should be missing from cache
        ];

        let resolved = resolve_all_sync(&stats, &exprs);
        assert_eq!(resolved.num_containers(), 2);
        assert!(resolved.get(&exprs[0]).is_some()); // min
        assert!(resolved.get(&exprs[1]).is_some()); // max
        assert!(resolved.get(&exprs[2]).is_none()); // unsupported
    }

    #[test]
    fn test_resolved_statistics_get_or_null() {
        let stats = MockStats::new();
        let min_expr =
            datafusion_functions_aggregate::min_max::min_udaf().call(vec![col_expr("a")]);
        let resolved = resolve_all_sync(&stats, std::slice::from_ref(&min_expr));

        // Existing entry
        let arr = resolved.get_or_null(&min_expr, &DataType::Int64);
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.null_count(), 0);

        // Missing entry → null array
        let missing = col_expr("missing");
        let arr = resolved.get_or_null(&missing, &DataType::Int32);
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.null_count(), 2);
    }

    #[tokio::test]
    async fn test_resolved_statistics_resolve_async() {
        let stats = MockStats::new();
        let exprs = vec![
            datafusion_functions_aggregate::min_max::min_udaf().call(vec![col_expr("a")]),
            datafusion_functions_aggregate::min_max::max_udaf().call(vec![col_expr("a")]),
        ];

        let resolved = ResolvedStatistics::resolve(&stats, &exprs).await.unwrap();
        assert_eq!(resolved.num_containers(), 2);
        assert!(resolved.get(&exprs[0]).is_some());
        assert!(resolved.get(&exprs[1]).is_some());
    }

    #[test]
    fn test_new_empty_resolved_statistics() {
        let resolved = ResolvedStatistics::new_empty(5);
        assert_eq!(resolved.num_containers(), 5);
        let expr = col_expr("any");
        assert!(resolved.get(&expr).is_none());
    }
}
