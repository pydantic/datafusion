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

//! Per-file *read plan* construction cost for wide parquet schemas.
//!
//! The parquet opener builds one read plan per file
//! (`build_projection_read_plan`): it resolves the scan's projection
//! expressions against the file schema into a leaf-level `ProjectionMask` plus
//! the Arrow schema the decoder will emit. Everything in that path is paid
//! once per *file*, so any step whose cost grows with the *schema* rather than
//! with the *projection* is multiplied by the number of files in the scan.
//!
//! These benchmarks isolate that cost: each file holds only a handful of rows
//! of tiny values, and the scan spans [`NUM_FILES`] of them, so wall time is
//! dominated by per-file planning rather than by IO or decoding.
//!
//! Shapes measured:
//!
//! 1. `flat_wide` — [`WIDE_COLS`] primitive columns, no structs at all,
//!    projected once as a bare column (the "all plain columns" fast path) and
//!    once through an expression (which defeats it). The gap is what the
//!    slower path costs on a wide schema with nothing to prune.
//! 2. `flat_wide_plus_struct` — the same, plus one struct column *last*. The
//!    projection never touches it, so this measures whether an unprojected
//!    nested column drags the projection onto the slow path.
//! 3. `wide_struct/full_schema` — one struct column with
//!    [`WIDE_STRUCT_FIELDS`] subfields, declared exactly as the file has it:
//!    no cast, no clipping. The baseline for (4).
//! 4. `wide_struct/narrowed_schema` — the same file declared with half the
//!    subfields, so the adapter inserts `CAST(s AS narrower_struct)` and the
//!    scan clips the read to the declared leaves. This is the path whose
//!    per-struct-level field matching is the thing to watch: it must not be
//!    quadratic in the number of subfields.
//! 5. `wide_struct/unrelated_column` — the very wide struct sits in the
//!    schema but only `id` is projected. Should cost the same as a schema
//!    with no struct in it at all.

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::ListingTableUrl;
use parquet::arrow::ArrowWriter;
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Files per scan. The read plan is rebuilt for each one, so this is the
/// multiplier on any per-file cost.
const NUM_FILES: usize = 32;
/// Rows per file: small enough that decoding is noise next to planning.
const ROWS_PER_FILE: usize = 8;
/// Number of top-level primitive columns in the "wide flat schema" shapes.
const WIDE_COLS: usize = 1000;
/// Number of subfields in the "wide struct" shapes.
const WIDE_STRUCT_FIELDS: usize = 1000;

fn int_col(i: usize) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(
        (0..ROWS_PER_FILE).map(|r| (i * ROWS_PER_FILE + r) as i32),
    ))
}

fn primitive_fields(count: usize, prefix: &str) -> Vec<Field> {
    (0..count)
        .map(|i| Field::new(format!("{prefix}{i}"), DataType::Int32, true))
        .collect()
}

/// [`WIDE_COLS`] primitive columns.
fn flat_wide_schema() -> SchemaRef {
    Arc::new(Schema::new(primitive_fields(WIDE_COLS, "c")))
}

/// [`WIDE_COLS`] primitive columns plus one small struct column, placed
/// *last* so the `has_struct_columns` gate has to scan every field before it
/// finds a struct.
fn flat_wide_plus_struct_schema() -> SchemaRef {
    let mut fields = primitive_fields(WIDE_COLS, "c");
    fields.push(Field::new(
        "s",
        DataType::Struct(Fields::from(primitive_fields(8, "f"))),
        true,
    ));
    Arc::new(Schema::new(fields))
}

/// One `id` column plus a struct with [`WIDE_STRUCT_FIELDS`] subfields.
fn wide_struct_schema(num_subfields: usize) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new(
            "s",
            DataType::Struct(Fields::from(primitive_fields(num_subfields, "f"))),
            true,
        ),
    ]))
}

/// The narrow declared schema for the clipping benchmark: the same struct with
/// only the even-numbered subfields, so half the leaves can be pruned.
fn wide_struct_narrow_schema() -> SchemaRef {
    let subfields: Vec<Field> = (0..WIDE_STRUCT_FIELDS)
        .step_by(2)
        .map(|i| Field::new(format!("f{i}"), DataType::Int32, true))
        .collect();
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("s", DataType::Struct(Fields::from(subfields)), true),
    ]))
}

fn batch_for(schema: &SchemaRef) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| match f.data_type() {
            DataType::Struct(children) => Arc::new(StructArray::new(
                children.clone(),
                children
                    .iter()
                    .enumerate()
                    .map(|(j, _)| int_col(i + j))
                    .collect(),
                None,
            )) as ArrayRef,
            _ => int_col(i),
        })
        .collect();
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

/// Write [`NUM_FILES`] identical small files with `schema` into a fresh dir.
fn write_files(schema: &SchemaRef) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let batch = batch_for(schema);
    for i in 0..NUM_FILES {
        let path = dir.path().join(format!("part-{i:04}.parquet"));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    dir
}

/// Register the files in `dir` as `t`, declaring `table_schema` (which may be
/// narrower than the files' physical schema).
fn context_for(dir: &TempDir, rt: &Runtime, table_schema: SchemaRef) -> SessionContext {
    // Statistics collection walks every column of every file, which would
    // swamp the read-plan cost this benchmark is trying to isolate.
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_collect_statistics(false),
    );
    let url =
        ListingTableUrl::parse(format!("file://{}/", dir.path().display())).unwrap();
    let config = rt
        .block_on(ListingTableConfig::new(url).infer_options(&ctx.state()))
        .unwrap()
        .with_schema(table_schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();
    ctx
}

fn run(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn wide_schema_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parquet_wide_schema_read_plan");

    // (1) wide flat schema, no structs anywhere. `c0 + 1` is not a bare
    // column reference, so the projection takes the slower path and pays the
    // `has_struct_columns` gate over all WIDE_COLS fields.
    let flat = flat_wide_schema();
    let flat_dir = write_files(&flat);
    let flat_ctx = context_for(&flat_dir, &rt, Arc::clone(&flat));
    group.bench_function("flat_wide/expr_projection", |b| {
        b.iter(|| run(&flat_ctx, &rt, "SELECT c0 + 1 FROM t"))
    });
    // The all-plain-columns fast path, for contrast.
    group.bench_function("flat_wide/column_projection", |b| {
        b.iter(|| run(&flat_ctx, &rt, "SELECT c0 FROM t"))
    });

    // (2) same, but with a struct column last: the gate now returns true and
    // the full PushdownChecker traversal runs.
    let flat_struct = flat_wide_plus_struct_schema();
    let flat_struct_dir = write_files(&flat_struct);
    let flat_struct_ctx = context_for(&flat_struct_dir, &rt, Arc::clone(&flat_struct));
    group.bench_function("flat_wide_plus_struct/expr_projection", |b| {
        b.iter(|| run(&flat_struct_ctx, &rt, "SELECT c0 + 1 FROM t"))
    });

    // (3)/(4) one very wide struct, declared in full vs. narrowed to half its
    // subfields. The difference isolates the cast-clipping work.
    let wide_struct = wide_struct_schema(WIDE_STRUCT_FIELDS);
    let wide_struct_dir = write_files(&wide_struct);

    let full_ctx = context_for(&wide_struct_dir, &rt, Arc::clone(&wide_struct));
    group.bench_function("wide_struct/full_schema", |b| {
        b.iter(|| run(&full_ctx, &rt, "SELECT s FROM t"))
    });

    let narrow_ctx = context_for(&wide_struct_dir, &rt, wide_struct_narrow_schema());
    group.bench_function("wide_struct/narrowed_schema", |b| {
        b.iter(|| run(&narrow_ctx, &rt, "SELECT s FROM t"))
    });

    // Projecting an unrelated column while a very wide struct sits in the
    // schema: nothing to clip, but the gate and the per-file leaf bookkeeping
    // still run.
    group.bench_function("wide_struct/unrelated_column", |b| {
        b.iter(|| run(&narrow_ctx, &rt, "SELECT id + 1 FROM t"))
    });

    group.finish();
}

criterion_group!(benches, wide_schema_benchmarks);
criterion_main!(benches);
