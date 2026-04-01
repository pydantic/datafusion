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

//! MRE demonstrating that `ProjectionExec::project_statistics()` produces
//! unknown column statistics for any `ScalarFunctionExpr` output, including
//! `get_field()`. This affects cost-based optimization for any query that
//! projects columns through scalar functions.
//!
//! The test builds a Parquet file with known data and verifies whether
//! column statistics (min/max/null_count) survive a `get_field()` projection.

use arrow::array::{
    ArrayRef, BinaryViewBuilder, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::prelude::*;
use datafusion_common::stats::Precision;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tempfile::TempDir;

fn variant_schema() -> Schema {
    let typed_value_fields = Fields::from(vec![
        Field::new(
            "busy_ns",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::BinaryView, true),
                Field::new("typed_value", DataType::Int64, true),
            ])),
            true,
        ),
        Field::new(
            "db_system",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::BinaryView, true),
                Field::new("typed_value", DataType::Utf8, true),
            ])),
            true,
        ),
    ]);

    let variant_fields = Fields::from(vec![
        Field::new("metadata", DataType::BinaryView, true),
        Field::new("typed_value", DataType::Struct(typed_value_fields), true),
    ]);

    Schema::new(vec![Field::new(
        "variant",
        DataType::Struct(variant_fields),
        true,
    )])
}

fn binary_view_array(values: &[&[u8]]) -> ArrayRef {
    let mut builder = BinaryViewBuilder::new();
    for v in values {
        builder.append_value(v);
    }
    Arc::new(builder.finish())
}

fn build_batch() -> RecordBatch {
    let schema = Arc::new(variant_schema());

    let metadata = binary_view_array(&[b"m", b"m", b"m", b"m", b"m", b"m"]);

    // busy_ns: 100..600, no nulls
    let busy_ns_value =
        binary_view_array(&[b"100", b"200", b"300", b"400", b"500", b"600"]);
    let busy_ns_typed: ArrayRef =
        Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600]));
    let busy_ns = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::BinaryView, true)),
            busy_ns_value,
        ),
        (
            Arc::new(Field::new("typed_value", DataType::Int64, true)),
            busy_ns_typed,
        ),
    ]);

    // db_system: pg/mysql mix, no nulls
    let db_value = binary_view_array(&[b"pg", b"mysql", b"pg", b"pg", b"mysql", b"pg"]);
    let db_typed: ArrayRef = Arc::new(StringArray::from(vec![
        "pg", "mysql", "pg", "pg", "mysql", "pg",
    ]));
    let db_system = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::BinaryView, true)),
            db_value,
        ),
        (
            Arc::new(Field::new("typed_value", DataType::Utf8, true)),
            db_typed,
        ),
    ]);

    let busy_ns_field = Field::new(
        "busy_ns",
        DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::BinaryView, true),
            Field::new("typed_value", DataType::Int64, true),
        ])),
        true,
    );
    let db_system_field = Field::new(
        "db_system",
        DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::BinaryView, true),
            Field::new("typed_value", DataType::Utf8, true),
        ])),
        true,
    );

    let typed_value = StructArray::from(vec![
        (Arc::new(busy_ns_field), Arc::new(busy_ns) as ArrayRef),
        (Arc::new(db_system_field), Arc::new(db_system) as ArrayRef),
    ]);

    let typed_value_fields = Fields::from(vec![
        Field::new(
            "busy_ns",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::BinaryView, true),
                Field::new("typed_value", DataType::Int64, true),
            ])),
            true,
        ),
        Field::new(
            "db_system",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::BinaryView, true),
                Field::new("typed_value", DataType::Utf8, true),
            ])),
            true,
        ),
    ]);

    // variant: {
    //   metadata: BinaryView,
    //   typed_value: {
    //     busy_ns:   { value: BinaryView, typed_value: Int64 },
    //     db_system: { value: BinaryView, typed_value: Utf8  }
    //   }
    // }
    let variant = StructArray::from(vec![
        (
            Arc::new(Field::new("metadata", DataType::BinaryView, true)),
            metadata,
        ),
        (
            Arc::new(Field::new(
                "typed_value",
                DataType::Struct(typed_value_fields),
                true,
            )),
            Arc::new(typed_value) as ArrayRef,
        ),
    ]);

    RecordBatch::try_new(schema, vec![Arc::new(variant)]).unwrap()
}

fn write_parquet(batch: &RecordBatch) -> (TempDir, String) {
    let tmpdir = TempDir::new().unwrap();
    let path = tmpdir.path().join("stats_mre.parquet");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), Some(props))
            .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    (tmpdir, path.to_str().unwrap().to_string())
}

/// a plain column reference preserves statistics through ProjectionExec.
/// This is the control — proves the stats infrastructure works.
#[tokio::test]
async fn stats_preserved_for_plain_column() {
    // use a flat schema as control
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600]))],
    )
    .unwrap();

    let tmpdir = TempDir::new().unwrap();
    let path = tmpdir.path().join("flat.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let ctx = SessionContext::new();
    ctx.register_parquet("flat", path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let df = ctx.sql("SELECT id FROM flat").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let stats = plan.partition_statistics(None).unwrap();

    let row_count = stats.num_rows.get_value().copied().unwrap();
    assert_eq!(row_count, 6);
    let col_stats = &stats.column_statistics[0];
    assert!(
        col_stats.min_value != Precision::Absent,
        "Expected min_value to be present for a plain column projection, \
         got Absent. Stats: {col_stats:?}"
    );
    assert!(
        col_stats.max_value != Precision::Absent,
        "Expected max_value to be present for a plain column projection, \
         got Absent. Stats: {col_stats:?}"
    );
}

/// `get_field()` on a nested struct column should preserve statistics,
/// but currently produces unknown stats for the output column.
///
/// The Parquet file has exact statistics for the leaf column
/// `variant.typed_value.busy_ns.typed_value` (min=100, max=600),
/// but after ProjectionExec extracts it via `get_field()`, the output
/// column has Absent min/max/null_count.
#[tokio::test]
async fn stats_lost_through_get_field() {
    let batch = build_batch();
    let (_tmpdir, path) = write_parquet(&batch);

    let ctx = SessionContext::new();
    ctx.register_parquet("t", &path, Default::default())
        .await
        .unwrap();

    let sql = "SELECT variant['typed_value']['busy_ns']['typed_value'] AS busy_ns FROM t";
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let stats = plan.partition_statistics(None).unwrap();

    let row_count = stats.num_rows.get_value().copied().unwrap();
    assert_eq!(row_count, 6);

    let col_stats = &stats.column_statistics[0];
    println!("get_field output stats: {col_stats:?}");

    // this is what SHOULD work but currently fails:
    // get_field is a pure extraction — the output column's min/max
    // should be the same as the source leaf column's min/max.
    assert!(
        col_stats.min_value != Precision::Absent,
        "get_field() output has Absent min_value. \
         The source Parquet leaf has min=100 but ProjectionExec \
         returns ColumnStatistics::new_unknown() for ScalarFunctionExpr outputs. \
         Stats: {col_stats:?}"
    );
}
