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
use datafusion_benchmarks::smj::SMJ_QUERIES;
use tokio::runtime::Runtime;

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn load_queries() -> Vec<(String, Vec<String>)> {
    SMJ_QUERIES
        .iter()
        .enumerate()
        .map(|(i, sql)| (format!("q{}", i + 1), vec![sql.trim().to_string()]))
        .collect()
}

fn benchmark_smj_warm(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let make_ctx = || {
        let mut config = SessionConfig::from_env().unwrap();
        config = config.set_bool("datafusion.optimizer.prefer_hash_join", false);
        SessionContext::new_with_config(config)
    };
    run_sql_benchmarks(c, &rt, "smj", BenchMode::Warm, &make_ctx, &queries);
}

fn benchmark_smj_cold(c: &mut criterion::Criterion) {
    let rt = make_runtime();
    let queries = load_queries();
    let make_ctx = || {
        let mut config = SessionConfig::from_env().unwrap();
        config = config.set_bool("datafusion.optimizer.prefer_hash_join", false);
        SessionContext::new_with_config(config)
    };
    run_sql_benchmarks(c, &rt, "smj_cold", BenchMode::Cold, &make_ctx, &queries);
}

criterion_group!(benches, benchmark_smj_warm, benchmark_smj_cold);
criterion_main!(benches);
