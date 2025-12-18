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

/*
These are tests for union type comparison coercion

when comparing a union type with a scalar type, it will:
1. check if the scalar type matches any Union variant (exact type match preferred)
2. if no exact match, check if any variant can be cast to the scalar type
3. if yes, coerce to the scalar type (triggers cast from Union to scalar)
4. cast will extract values from the matching variant (and cast if needed), non-matching variants become null

It's important to note that the scalar type matching is greedy, meaning if you have a Union type
with variants of the same field, it will match by the smaller type id (since variants are sorted by type id)


Some of the *current* limitations are:
1. numeric literals default to int64
2. if multiple variants can cast to target type, picks the first castable variant (by type id order)

*/

use arrow::array::*;
use arrow::buffer::ScalarBuffer;
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use datafusion_common::Result;
use std::sync::Arc;

// create a Union(Int32, Utf8) sparse union array
fn create_sparse_union_array(values: Vec<UnionValue>) -> UnionArray {
    let union_fields = UnionFields::new(
        vec![0, 1],
        vec![
            Field::new("int", DataType::Int32, true),
            Field::new("str", DataType::Utf8, true),
        ],
    );

    let mut int_values = Vec::new();
    let mut str_values = Vec::new();
    let mut type_ids = Vec::new();

    for value in values {
        match value {
            UnionValue::Int(v) => {
                int_values.push(v);
                str_values.push(None);
                type_ids.push(0);
            }
            UnionValue::Str(v) => {
                int_values.push(None);
                str_values.push(v);
                type_ids.push(1);
            }
        }
    }

    let int_array = Int32Array::from(int_values);
    let str_array = StringArray::from(str_values);
    let type_ids = ScalarBuffer::<i8>::from(type_ids);

    UnionArray::try_new(
        union_fields,
        type_ids,
        None,
        vec![Arc::new(int_array) as Arc<dyn Array>, Arc::new(str_array)],
    )
    .unwrap()
}

#[derive(Debug)]
enum UnionValue {
    Int(Option<i32>),
    Str(Option<&'static str>),
}

// right now arrow does not support union cast support
// maybe the right thing to do is add functionality there...
#[test]
fn test_arrow_union_cast_support() {
    let union_fields = UnionFields::new(
        vec![0, 1],
        vec![
            Field::new("int", DataType::Int32, true),
            Field::new("str", DataType::Utf8, true),
        ],
    );
    let union_type = DataType::Union(union_fields, UnionMode::Sparse);

    // Arrow doesn't support casting Union types natively
    assert!(!can_cast_types(&union_type, &DataType::Int64));
    assert!(!can_cast_types(&union_type, &DataType::Int32));
    assert!(!can_cast_types(&union_type, &DataType::Utf8));
}

#[tokio::test]
async fn test_union_eq_int32() -> Result<()> {
    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(67)),
        UnionValue::Str(Some("hello")),
        UnionValue::Int(Some(123)),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("int", DataType::Int32, true),
                        Field::new("str", DataType::Utf8, true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    let df = ctx
        .sql("SELECT id FROM test WHERE val = CAST(67 AS INT)")
        .await?;
    let results = df.collect().await?;

    let expected = ["+----+", "| id |", "+----+", "| 1  |", "+----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_union_eq_string() -> Result<()> {
    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(67)),
        UnionValue::Str(Some("hello")),
        UnionValue::Str(Some("world")),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("int", DataType::Int32, true),
                        Field::new("str", DataType::Utf8, true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    let df = ctx.sql("SELECT id FROM test WHERE val = 'hello'").await?;
    let results = df.collect().await?;

    let expected = ["+----+", "| id |", "+----+", "| 2  |", "+----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_union_comparison_operators() -> Result<()> {
    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Int(Some(20)),
        UnionValue::Int(Some(30)),
        UnionValue::Str(Some("foo")),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("int", DataType::Int32, true),
                        Field::new("str", DataType::Utf8, true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    // test > - cast literals to Int32
    let df = ctx
        .sql("SELECT id FROM test WHERE val > CAST(15 AS INT)")
        .await?;
    let results = df.collect().await?;
    let expected = ["+----+", "| id |", "+----+", "| 2  |", "| 3  |", "+----+"];
    assert_batches_eq!(expected, &results);

    // test <
    let df = ctx
        .sql("SELECT id FROM test WHERE val < CAST(15 AS INT)")
        .await?;
    let results = df.collect().await?;
    let expected = ["+----+", "| id |", "+----+", "| 1  |", "+----+"];
    assert_batches_eq!(expected, &results);

    // test !=
    let df = ctx
        .sql("SELECT id FROM test WHERE val != CAST(20 AS INT)")
        .await?;
    let results = df.collect().await?;
    let expected = ["+----+", "| id |", "+----+", "| 1  |", "| 3  |", "+----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_union_with_null_values() -> Result<()> {
    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Int(None), // null int
        UnionValue::Str(Some("foo")),
        UnionValue::Str(None), // null string
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("int", DataType::Int32, true),
                        Field::new("str", DataType::Utf8, true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    let df = ctx
        .sql("SELECT id FROM test WHERE val = CAST(10 AS INT)")
        .await?;
    let results = df.collect().await?;
    let expected = ["+----+", "| id |", "+----+", "| 1  |", "+----+"];
    assert_batches_eq!(expected, &results);

    let df = ctx.sql("SELECT id FROM test WHERE val IS NULL").await?;
    let results = df.collect().await?;

    // row 2 has null int and row 4 has null string
    // both should appear as null after cast
    let expected = ["+----+", "| id |", "+----+", "| 2  |", "| 4  |", "+----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_union_non_matching_variants_are_null() -> Result<()> {
    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Str(Some("hello")),
        UnionValue::Int(Some(30)),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1],
                    vec![
                        Field::new("int", DataType::Int32, true),
                        Field::new("str", DataType::Utf8, true),
                    ],
                ),
                UnionMode::Sparse,
            ),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    // When casting to Int32, the string variant becomes NULL
    let df = ctx
        .sql("SELECT id, CAST(val AS INT) as val_int FROM test")
        .await?;
    let results = df.collect().await?;

    dbg!(&results);

    let expected = [
        "+----+---------+",
        "| id | val_int |",
        "+----+---------+",
        "| 1  | 10      |",
        "| 2  |         |", // null because it's a string
        "| 3  | 30      |",
        "+----+---------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

// tests cast-compatible variant matching
// when comparing Union(Int32, Utf8) with Int64, it finds the Int32 variant and casts it
#[tokio::test]
async fn test_union_cast_compatible_variant() -> Result<()> {
    let union_fields = UnionFields::new(
        vec![0, 1],
        vec![
            Field::new("int", DataType::Int32, true),
            Field::new("str", DataType::Utf8, true),
        ],
    );

    let union_array = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Str(Some("hello")),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(union_array),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    // Int32 variant can be cast to Int64, so this should work
    let df = ctx
        .sql("SELECT id FROM test WHERE val = CAST(10 AS BIGINT)")
        .await?;
    let results = df.collect().await?;

    let expected = ["+----+", "| id |", "+----+", "| 1  |", "+----+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

// todo: this should also be fixed since arrow-ord now has support for union arrays...
// test this with arrow pointed to main...
#[tokio::test]
async fn test_union_eq_same_union() -> Result<()> {
    let union_fields = UnionFields::new(
        vec![0, 1],
        vec![
            Field::new("int", DataType::Int32, true),
            Field::new("str", DataType::Utf8, true),
        ],
    );

    let union_array1 = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Str(Some("hello")),
    ]);

    let union_array2 = create_sparse_union_array(vec![
        UnionValue::Int(Some(10)),
        UnionValue::Str(Some("world")),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "val1",
            DataType::Union(union_fields.clone(), UnionMode::Sparse),
            true,
        ),
        Field::new(
            "val2",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(union_array1),
            Arc::new(union_array2),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test", batch)?;

    let df = ctx
        .sql("SELECT id FROM test WHERE val1 = val2")
        .await
        .unwrap();

    let exec_result = df.collect().await;
    assert!(exec_result.is_err());
    let err = exec_result.unwrap_err();
    assert!(err.to_string().contains("no natural order"),);

    Ok(())
}
