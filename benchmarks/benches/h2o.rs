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

use criterion::{criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_benchmarks::benchmark_harness::{BenchMode, run_sql_benchmarks};
use std::path::Path;
use tokio::runtime::Runtime;

fn data_path() -> String {
    std::env::var("H2O_DATA").unwrap_or_else(|_| "benchmarks/data/h2o".to_string())
}

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn register_csv(rt: &Runtime, ctx: &SessionContext, table_name: &str, path: &str) {
    rt.block_on(async {
        ctx.register_csv(table_name, path, Default::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register {table_name}: {e}"));
    });
}

fn register_parquet(rt: &Runtime, ctx: &SessionContext, table_name: &str, path: &str) {
    rt.block_on(async {
        ctx.register_parquet(table_name, path, Default::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register {table_name}: {e}"));
    });
}

fn register_data(rt: &Runtime, ctx: &SessionContext, table_name: &str, path: &str) {
    let extension = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    match extension {
        "csv" => register_csv(rt, ctx, table_name, path),
        "parquet" => register_parquet(rt, ctx, table_name, path),
        _ => panic!("Unsupported file extension: {extension}"),
    }
}

fn load_queries_from_file(path: &str) -> Vec<(String, Vec<String>)> {
    let contents = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Could not read query file {path}: {e}"));
    contents
        .split("\n\n")
        .enumerate()
        .filter(|(_, s)| !s.trim().is_empty())
        .map(|(i, s)| (format!("q{}", i + 1), vec![s.trim().to_string()]))
        .collect()
}

struct H2OVariant {
    group_name: &'static str,
    queries_file: &'static str,
    setup: fn(&Runtime, &SessionContext, &str),
}

fn setup_groupby(rt: &Runtime, ctx: &SessionContext, base: &str) {
    register_data(rt, ctx, "x", &format!("{base}/G1_1e7_1e7_100_0.csv"));
}

fn setup_join(rt: &Runtime, ctx: &SessionContext, base: &str) {
    register_data(rt, ctx, "x", &format!("{base}/J1_1e7_NA_0.csv"));
    register_data(rt, ctx, "small", &format!("{base}/J1_1e7_1e1_0.csv"));
    register_data(rt, ctx, "medium", &format!("{base}/J1_1e7_1e4_0.csv"));
    register_data(rt, ctx, "large", &format!("{base}/J1_1e7_1e7_NA.csv"));
}

fn setup_window(rt: &Runtime, ctx: &SessionContext, base: &str) {
    register_data(rt, ctx, "large", &format!("{base}/J1_1e7_1e7_NA.csv"));
}

const VARIANTS: &[H2OVariant] = &[
    H2OVariant {
        group_name: "h2o_groupby",
        queries_file: "benchmarks/queries/h2o/groupby.sql",
        setup: setup_groupby,
    },
    H2OVariant {
        group_name: "h2o_join",
        queries_file: "benchmarks/queries/h2o/join.sql",
        setup: setup_join,
    },
    H2OVariant {
        group_name: "h2o_window",
        queries_file: "benchmarks/queries/h2o/window.sql",
        setup: setup_window,
    },
];

fn benchmark_h2o_warm(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let base = data_path();
    for variant in VARIANTS {
        if !Path::new(variant.queries_file).exists() {
            continue;
        }
        let queries = load_queries_from_file(variant.queries_file);
        let make_ctx = || {
            let config = SessionConfig::from_env().unwrap();
            let ctx = SessionContext::new_with_config(config);
            (variant.setup)(&rt, &ctx, &base);
            ctx
        };
        run_sql_benchmarks(
            c,
            &rt,
            variant.group_name,
            BenchMode::Warm,
            &make_ctx,
            &queries,
        );
    }
}

fn benchmark_h2o_cold(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let base = data_path();
    for variant in VARIANTS {
        if !Path::new(variant.queries_file).exists() {
            continue;
        }
        let queries = load_queries_from_file(variant.queries_file);
        let group_name = format!("{}_cold", variant.group_name);
        let make_ctx = || {
            let config = SessionConfig::from_env().unwrap();
            let ctx = SessionContext::new_with_config(config);
            (variant.setup)(&rt, &ctx, &base);
            ctx
        };
        run_sql_benchmarks(c, &rt, &group_name, BenchMode::Cold, &make_ctx, &queries);
    }
}

criterion_group!(benches, benchmark_h2o_warm, benchmark_h2o_cold);
criterion_main!(benches);
