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

use std::sync::{Arc, LazyLock};

use arrow::{
    array::record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use arrow_schema::SortOptions;
use datafusion::{
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
    prelude::{SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use datafusion_common::config::ConfigOptions;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::test::function_stub::count_udaf;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, Partitioning};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::{
    push_down_filter::PushdownFilter, PhysicalOptimizerRule,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_batches::CoalesceBatchesExec,
    filter::FilterExec,
    repartition::RepartitionExec,
};
use datafusion_physical_plan::{sorts::sort::SortExec, ExecutionPlan};

use futures::StreamExt;
use object_store::memory::InMemory;
use util::{format_plan_for_test, OptimizationTest, TestScanBuilder};

mod util;

#[test]
fn test_pushdown_into_scan() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

/// Show that we can use config options to determine how to do pushdown.
#[test]
fn test_pushdown_into_scan_with_config_options() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap()) as _;

    let mut cfg = ConfigOptions::default();
    insta::assert_snapshot!(
        OptimizationTest::new(
            Arc::clone(&plan),
            PushdownFilter {},
            false
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );

    cfg.execution.parquet.pushdown_filters = true;
    insta::assert_snapshot!(
        OptimizationTest::new(
            plan,
            PushdownFilter {},
            true
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_collapse() {
    // filter should be pushed down into the parquet scan with two filters
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate1 = col_lit_predicate("a", "foo", &schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar AND a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_projection() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let projection = vec![1, 0];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, Arc::clone(&scan))
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );

    // expect the predicate to be pushed down into the DataSource but the FilterExec to be converted to ProjectionExec
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1, a@0]
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b, a@0 as a]
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    ",
    );

    // add a test where the filter is on a column that isn't included in the output
    let projection = vec![1];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, scan)
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{},true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1]
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b]
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_push_down_through_transparent_nodes() {
    // expect the predicate to be pushed down into the DataSource
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 1));
    let predicate = col_lit_predicate("a", "foo", &schema());
    let filter = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());
    let repartition = Arc::new(
        RepartitionExec::try_new(filter, Partitioning::RoundRobinBatch(1)).unwrap(),
    );
    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, repartition).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{},true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=0
        -     FilterExec: a@0 = foo
        -       CoalesceBatchesExec: target_batch_size=1
        -         DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=0
          -   CoalesceBatchesExec: target_batch_size=1
          -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar AND a@0 = foo
    "
    );
}

#[test]
fn test_no_pushdown_through_aggregates() {
    // There are 2 important points here:
    // 1. The outer filter **is not** pushed down at all because we haven't implemented pushdown support
    //    yet for AggregateExec.
    // 2. The inner filter **is** pushed down into the DataSource.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 10));

    let filter = Arc::new(
        FilterExec::try_new(col_lit_predicate("a", "foo", &schema()), coalesce).unwrap(),
    );

    let aggregate_expr =
        vec![
            AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema()).unwrap()])
                .schema(schema())
                .alias("cnt")
                .build()
                .map(Arc::new)
                .unwrap(),
        ];
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("a", &schema()).unwrap(), "a".to_string()),
        (col("b", &schema()).unwrap(), "b".to_string()),
    ]);
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            filter,
            schema(),
        )
        .unwrap(),
    );

    let coalesce = Arc::new(CoalesceBatchesExec::new(aggregate, 100));

    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   CoalesceBatchesExec: target_batch_size=100
        -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([0])
        -       FilterExec: a@0 = foo
        -         CoalesceBatchesExec: target_batch_size=10
        -           DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: b@1 = bar
          -   CoalesceBatchesExec: target_batch_size=100
          -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt]
          -       CoalesceBatchesExec: target_batch_size=10
          -         DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[tokio::test]
async fn test_topk_dynamic_filter_pushdown() {
    // This test is a bit of a hack, but it shows that we can push down dynamic filters
    // into the DataSourceExec. The test is not perfect because we don't have a real
    // implementation of the dynamic filter yet, so we just use a static filter.
    let batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["bd", "bc"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
        record_batch!(
            ("a", Utf8, ["ac", "ad"]),
            ("b", Utf8, ["bb", "ba"]),
            ("c", Float64, [2.0, 1.0])
        )
        .unwrap(),
    ];
    let scan = TestScanBuilder::new(schema())
        .with_support(true)
        .with_batches(batches)
        .build();
    let plan = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("b", &schema()).unwrap(),
                SortOptions::new(true, false), // descending, nulls_first
            )]),
            Arc::clone(&scan),
        )
        .with_fetch(Some(1)),
    ) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false], filter=DynamicFilterPhysicalExpr [ true ]
        -   DataSourceExec: file_groups={1 group: [[test.paqruet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false], filter=DynamicFilterPhysicalExpr [ true ]
          -   DataSourceExec: file_groups={1 group: [[test.paqruet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ true ]
    "
    );

    // Actually apply the optimization to the plan
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    let plan = PushdownFilter {}.optimize(plan, &config).unwrap();
    let config = SessionConfig::new().with_batch_size(2);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let mut stream = plan.execute(0, Arc::clone(&task_ctx)).unwrap();
    // Iterate one batch
    stream.next().await.unwrap().unwrap();
    // Now check what our filter looks like
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false], filter=DynamicFilterPhysicalExpr [ b@1 > bd ]
    -   DataSourceExec: file_groups={1 group: [[test.paqruet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ b@1 > bd ]
    "
    );
}

/// Schema:
/// a: String
/// b: String
/// c: f64
static TEST_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let fields = vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ];
    Arc::new(Schema::new(fields))
});

fn schema() -> SchemaRef {
    Arc::clone(&TEST_SCHEMA)
}

/// Returns a predicate that is a binary expression col = lit
fn col_lit_predicate(
    column_name: &str,
    scalar_value: impl Into<ScalarValue>,
    schema: &Schema,
) -> Arc<dyn PhysicalExpr> {
    let scalar_value = scalar_value.into();
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema(column_name, schema).unwrap()),
        Operator::Eq,
        Arc::new(Literal::new(scalar_value)),
    ))
}
