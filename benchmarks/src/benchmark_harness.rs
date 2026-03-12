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
use datafusion_common::instant::Instant;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Controls whether benchmarks reuse a single [`SessionContext`] or create a
/// fresh one for every measurement iteration.
///
/// Both modes produce **one criterion benchmark per query** — the difference is
/// only in how the context (and therefore caches, registered tables, etc.) is
/// managed.
///
/// # Warm
///
/// A single [`SessionContext`] is created once via `make_ctx`, and then every
/// query is executed once as a warmup pass (to populate OS page cache, parquet
/// metadata caches, etc.).  Criterion then measures each query individually,
/// reusing the same context across all iterations.  This is closest to
/// steady-state production performance.
///
/// # Cold
///
/// For each measurement iteration, a **fresh** [`SessionContext`] is created via
/// `make_ctx`.  The context creation and table registration happen *outside* the
/// timed region (via [`criterion::Bencher::iter_custom`]), so only the query
/// execution itself is measured — but there are no warm caches.  This captures
/// first-query latency without penalizing for setup cost.
#[derive(Clone, Copy)]
pub enum BenchMode {
    Warm,
    Cold,
}

/// Execute a sequence of SQL statements against `ctx`, collecting and
/// discarding all result batches.
///
/// Most queries consist of a single SQL string, but some (e.g. TPC-H Q15)
/// comprise multiple statements (CREATE TEMP VIEW, SELECT, DROP VIEW) that must
/// run together as an atomic unit.  Each element of `sqls` is executed in order.
pub async fn execute_sqls(ctx: &SessionContext, sqls: &[String]) {
    for sql in sqls {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
}

/// Register a group of SQL queries as individual criterion benchmarks, using
/// either [`BenchMode::Warm`] or [`BenchMode::Cold`] execution strategy.
///
/// # How queries map to benchmarks
///
/// Each entry in `queries` becomes its **own** criterion benchmark function.
/// For example, given `group_name = "tpch"` and queries
/// `[("q1", [...]), ("q2", [...]), ...]`, criterion will report separate
/// timings for `tpch/q1`, `tpch/q2`, etc.  This is important for per-query
/// regression detection in CI (e.g. CodSpeed).
///
/// The `Vec<String>` in each query entry is the list of SQL statements that
/// make up that single logical query (usually just one statement; see
/// [`execute_sqls`] for why some queries have multiple).
///
/// # Arguments
///
/// * `c` — The criterion context, passed through from the benchmark function.
/// * `rt` — A tokio [`Runtime`] used to drive async query execution.  The same
///   runtime is reused across all queries in the group.
/// * `group_name` — Criterion benchmark group name.  Appears as the prefix in
///   benchmark output (e.g. `"tpch"` → `tpch/q1`).  Convention: use the suite
///   name for warm mode, and `"{suite}_cold"` for cold mode.
/// * `mode` — Whether to run in warm or cold mode (see [`BenchMode`]).
/// * `make_ctx` — A closure that creates a fully configured [`SessionContext`]
///   with all necessary tables registered.  Called once in warm mode, or once
///   per iteration in cold mode.  Must be safe to call from a synchronous
///   context (use `rt.block_on(...)` internally if async registration is
///   needed).
/// * `queries` — The list of `(benchmark_name, sql_statements)` pairs.  Each
///   pair becomes one criterion benchmark.
///
/// # Criterion tuning
///
/// All benchmarks in the group share the same tuning parameters (see
/// [`configure_group`]):
/// - **`SamplingMode::Flat`** — takes exactly `sample_size` measurements
///   rather than auto-scaling iteration count.  Necessary because these are
///   long-running benchmarks (milliseconds to seconds per query).
/// - **`sample_size(10)`** — the minimum criterion allows; 10 measurements
///   per query is sufficient for stable statistics on queries of this duration.
/// - **`measurement_time(30s)`** — generous time budget so that criterion
///   doesn't cut measurements short for slower queries.
/// - **`warm_up_time(5s)`** — criterion's built-in warmup phase (separate from
///   the explicit warmup pass in [`BenchMode::Warm`]).
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
            // Create one context for the entire group and run each query once
            // to warm caches (OS page cache, parquet metadata, etc.)
            let ctx = make_ctx();
            for (_, sqls) in queries {
                rt.block_on(execute_sqls(&ctx, sqls));
            }
            // Now register each query as its own criterion benchmark, all
            // sharing the same pre-warmed context.
            for (name, sqls) in queries {
                group.bench_function(name, |b| {
                    b.to_async(rt).iter(|| execute_sqls(&ctx, sqls));
                });
            }
        }
        BenchMode::Cold => {
            // Each query gets its own criterion benchmark.  Within each
            // benchmark, every iteration creates a fresh context.  We use
            // `iter_custom` so that only the query execution is timed — the
            // context creation and table registration are excluded.
            for (name, sqls) in queries {
                group.bench_function(name, |b| {
                    b.to_async(rt).iter_custom(|iters| {
                        let sqls = sqls.clone();
                        async move {
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                let ctx = make_ctx();
                                let start = Instant::now();
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

/// Apply criterion tuning parameters appropriate for long-running SQL
/// benchmarks (milliseconds to seconds per query).
///
/// These settings are shared by all benchmark groups created by this harness:
///
/// - **`SamplingMode::Flat`**: Don't auto-detect iteration count.  Flat mode
///   runs exactly `sample_size` iterations, which is correct for benchmarks
///   where a single iteration already takes a meaningful amount of time.
///   (The default `Auto` mode would try to pack many iterations into each
///   sample, which is wasteful for multi-millisecond queries.)
///
/// - **`sample_size(10)`**: The minimum criterion allows.  Ten measurements
///   is enough for stable statistics on queries that take tens of milliseconds
///   or more, and keeps total benchmark runtime manageable.
///
/// - **`measurement_time(30s)`**: Total time budget per benchmark function.
///   Generous enough that slow queries (1-2 seconds) still get their full
///   10 samples.
///
/// - **`warm_up_time(5s)`**: Criterion's own warmup phase, which runs before
///   measurement begins.  This is in addition to any explicit warmup done in
///   [`BenchMode::Warm`].
fn configure_group(group: &mut BenchmarkGroup<WallTime>) {
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));
}
