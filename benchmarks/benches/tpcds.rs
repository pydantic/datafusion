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
use datafusion_benchmarks::tpcds::{
    TPCDS_QUERY_END_ID, TPCDS_QUERY_START_ID, TPCDS_TABLES, get_query_sql,
};
use tokio::runtime::Runtime;

fn data_path() -> String {
    std::env::var("TPCDS_DATA")
        .unwrap_or_else(|_| "benchmarks/data/tpcds_sf1".to_string())
}

fn query_path() -> String {
    std::env::var("TPCDS_QUERY_PATH")
        .unwrap_or_else(|_| "datafusion/core/tests/tpc-ds".to_string())
}

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn register_tables(rt: &Runtime, ctx: &SessionContext, path: &str) {
    for table in TPCDS_TABLES {
        let table_path = format!("{path}/{table}.parquet");
        rt.block_on(async {
            ctx.register_parquet(*table, &table_path, Default::default())
                .await
                .unwrap_or_else(|e| panic!("Failed to register table {table}: {e}"));
        });
    }
}

fn load_queries() -> Vec<(String, Vec<String>)> {
    let qpath = query_path();
    (TPCDS_QUERY_START_ID..=TPCDS_QUERY_END_ID)
        .filter_map(|id| {
            let sqls = get_query_sql(&qpath, id).ok()?;
            Some((format!("q{id}"), sqls))
        })
        .collect()
}

fn benchmark_tpcds_warm(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let path = data_path();
    let make_ctx = || {
        let config = SessionConfig::from_env().unwrap();
        let ctx = SessionContext::new_with_config(config);
        register_tables(&rt, &ctx, &path);
        ctx
    };
    run_sql_benchmarks(c, &rt, "tpcds", BenchMode::Warm, &make_ctx, &queries);
}

fn benchmark_tpcds_cold(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let path = data_path();
    let make_ctx = || {
        let config = SessionConfig::from_env().unwrap();
        let ctx = SessionContext::new_with_config(config);
        register_tables(&rt, &ctx, &path);
        ctx
    };
    run_sql_benchmarks(c, &rt, "tpcds_cold", BenchMode::Cold, &make_ctx, &queries);
}

criterion_group!(benches, benchmark_tpcds_warm, benchmark_tpcds_cold);
criterion_main!(benches);
