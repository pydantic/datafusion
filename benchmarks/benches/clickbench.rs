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
use datafusion_benchmarks::clickbench;
use std::path::PathBuf;
use tokio::runtime::Runtime;

fn data_path() -> String {
    std::env::var("CLICKBENCH_DATA")
        .unwrap_or_else(|_| "benchmarks/data/hits.parquet".to_string())
}

fn queries_path() -> PathBuf {
    PathBuf::from("benchmarks/queries/clickbench/queries")
}

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// SQL to create the hits view with proper EventDate casting.
const HITS_VIEW_DDL: &str = r#"CREATE VIEW hits AS
SELECT * EXCEPT ("EventDate"),
       CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate"
FROM hits_raw"#;

fn register_hits(rt: &Runtime, ctx: &SessionContext, path: &str) {
    rt.block_on(async {
        ctx.register_parquet("hits_raw", path, Default::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to register hits_raw: {e}"));
        ctx.sql(HITS_VIEW_DDL)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    });
}

fn load_queries() -> Vec<(String, Vec<String>)> {
    let query_dir = queries_path();
    let mut queries = Vec::new();
    for id in 0..=42 {
        let query_path = clickbench::get_query_path(&query_dir, id);
        if let Ok(Some(sql)) = clickbench::get_query_sql(&query_path) {
            queries.push((format!("q{id}"), vec![sql]));
        }
    }
    queries
}

fn benchmark_clickbench_warm(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let path = data_path();
    let make_ctx = || {
        let mut config = SessionConfig::from_env().unwrap();
        config.options_mut().execution.parquet.binary_as_string = true;
        let ctx = SessionContext::new_with_config(config);
        register_hits(&rt, &ctx, &path);
        ctx
    };
    run_sql_benchmarks(c, &rt, "clickbench", BenchMode::Warm, &make_ctx, &queries);
}

fn benchmark_clickbench_cold(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let path = data_path();
    let make_ctx = || {
        let mut config = SessionConfig::from_env().unwrap();
        config.options_mut().execution.parquet.binary_as_string = true;
        let ctx = SessionContext::new_with_config(config);
        register_hits(&rt, &ctx, &path);
        ctx
    };
    run_sql_benchmarks(
        c,
        &rt,
        "clickbench_cold",
        BenchMode::Cold,
        &make_ctx,
        &queries,
    );
}

criterion_group!(
    benches,
    benchmark_clickbench_warm,
    benchmark_clickbench_cold
);
criterion_main!(benches);
