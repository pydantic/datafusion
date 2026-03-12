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

//! Shared benchmark harness for criterion-based SQL benchmarks.

use criterion::{BenchmarkGroup, SamplingMode, measurement::WallTime};
use datafusion::prelude::SessionContext;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Benchmark mode: warm reuses a single context, cold creates a fresh one per iteration.
pub enum BenchMode {
    Warm,
    Cold,
}

/// Execute a sequence of SQL statements, collecting (and discarding) all results.
pub async fn execute_sqls(ctx: &SessionContext, sqls: &[String]) {
    for sql in sqls {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
}

/// Run SQL benchmarks in either warm or cold mode using criterion.
///
/// - `make_ctx`: closure that creates a `SessionContext` with tables registered.
/// - `queries`: list of `(bench_name, sql_statements)` pairs.
pub fn run_sql_benchmarks(
    c: &mut criterion::Criterion,
    rt: &Runtime,
    group_name: &str,
    mode: BenchMode,
    make_ctx: &dyn Fn() -> SessionContext,
    queries: &[(String, Vec<String>)],
) {
    let mut group = c.benchmark_group(group_name);
    configure_group(&mut group);

    match mode {
        BenchMode::Warm => {
            // Setup once and warmup all queries
            let ctx = make_ctx();
            for (_, sqls) in queries {
                rt.block_on(execute_sqls(&ctx, sqls));
            }
            for (name, sqls) in queries {
                group.bench_function(name, |b| {
                    b.to_async(rt).iter(|| execute_sqls(&ctx, sqls));
                });
            }
        }
        BenchMode::Cold => {
            // Fresh context per measurement iteration
            for (name, sqls) in queries {
                group.bench_function(name, |b| {
                    b.to_async(rt).iter_custom(|iters| {
                        let sqls = sqls.clone();
                        async move {
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                let ctx = make_ctx();
                                let start = std::time::Instant::now();
                                execute_sqls(&ctx, &sqls).await;
                                total += start.elapsed();
                            }
                            total
                        }
                    });
                });
            }
        }
    }
    group.finish();
}

fn configure_group(group: &mut BenchmarkGroup<WallTime>) {
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
}
