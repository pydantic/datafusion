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
//!
//! This module provides a fully generic, data-driven SQL benchmark harness.
//! Benchmark suites are defined as directories under `benchmarks/suites/` with
//! an optional `setup.sql` and a `queries/` subdirectory containing `.sql` files.

use criterion::{BenchmarkGroup, SamplingMode, measurement::WallTime};
use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use futures::stream::{FuturesUnordered, StreamExt};
use regex::Regex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
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

/// A discovered benchmark suite consisting of optional setup SQL and a list of
/// queries, each identified by name and containing one or more SQL statements.
pub struct Suite {
    /// SQL to run before benchmarking (e.g. CREATE TABLE, COPY, etc.).
    /// Already has environment variables substituted.
    pub setup_sql: Option<String>,
    /// Each entry is `(benchmark_name, vec_of_sql_statements)`.
    /// Names are derived from the `.sql` file stem (e.g. `"q01"` from `q01.sql`).
    /// Statements are split by `;` with empty entries filtered out.
    pub queries: Vec<(String, Vec<String>)>,
}

/// Returns the absolute path to the workspace root.
///
/// `CARGO_MANIFEST_DIR` is set at compile time by cargo and points to the
/// `benchmarks/` package directory.  The workspace root is its parent.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("benchmarks/ should be inside the workspace")
        .to_path_buf()
}

/// Create a multi-thread tokio runtime suitable for running benchmarks.
pub fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
}

/// Resolve `${VAR}` and `${VAR:-default}` patterns in a SQL string.
///
/// The `WORKSPACE` variable is always pre-set to [`workspace_root()`].
/// Additional variables are resolved from the process environment.
pub fn substitute_env_vars(sql: &str) -> String {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert(
        "WORKSPACE".to_string(),
        workspace_root().to_string_lossy().to_string(),
    );

    // Match ${VAR} or ${VAR:-default} where the default cannot contain '$',
    // ensuring inner variables like ${WORKSPACE} are resolved before outer
    // ones like ${TPCH_DATA:-${WORKSPACE}/path}. We loop until stable.
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^$]*))?\}")
        .expect("invalid regex");

    let mut result = sql.to_string();
    loop {
        let next = re
            .replace_all(&result, |caps: &regex::Captures| {
                let var_name = &caps[1];
                let default_value = caps.get(2).map(|m| m.as_str());

                // Check our pre-set vars first, then the environment.
                if let Some(val) = vars.get(var_name) {
                    val.clone()
                } else if let Ok(val) = std::env::var(var_name) {
                    val
                } else if let Some(default) = default_value {
                    default.to_string()
                } else {
                    // Leave unresolved variables as-is so errors are visible.
                    caps[0].to_string()
                }
            })
            .to_string();
        if next == result {
            break;
        }
        result = next;
    }
    result
}

/// Run the suite's `load.sh` script if it exists.
///
/// The script is expected to be idempotent — it checks whether data already
/// exists and only downloads/generates when missing.  This mirrors DuckDB's
/// approach where each benchmark declares its own data-loading step.
///
/// The script is skipped when the `BENCH_SKIP_LOAD` environment variable is
/// set to `1` (useful in CI where data is pre-provisioned).
pub fn run_load_script(suite_dir: &Path) {
    if std::env::var("BENCH_SKIP_LOAD").as_deref() == Ok("1") {
        return;
    }

    let load_script = suite_dir.join("load.sh");
    if !load_script.exists() {
        return;
    }

    eprintln!("Running data loader: {}", load_script.display());
    let status = Command::new("bash")
        .arg(&load_script)
        .status()
        .unwrap_or_else(|e| panic!("failed to run {}: {e}", load_script.display()));

    if !status.success() {
        panic!(
            "Data loader {} failed with exit code {:?}. \
             Fix the issue or run it manually, then retry the benchmark.",
            load_script.display(),
            status.code()
        );
    }
}

/// Discover a benchmark suite from a directory on disk.
///
/// The directory is expected to have the following layout:
/// ```text
/// suite_dir/
///   setup.sql       (optional)
///   queries/
///     q01.sql
///     q02.sql
///     ...
/// ```
///
/// - `setup.sql` is read if present; environment variables are substituted.
/// - Each `.sql` file under `queries/` is read, environment variables are
///   substituted, and the content is split by `;` into individual statements.
/// - Query files are sorted by filename so ordering is deterministic.
pub fn discover_suite(suite_dir: &Path) -> Suite {
    // Read optional setup.sql
    let setup_path = suite_dir.join("setup.sql");
    let setup_sql = if setup_path.exists() {
        let content = std::fs::read_to_string(&setup_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", setup_path.display()));
        Some(substitute_env_vars(&content))
    } else {
        None
    };

    // Discover query files
    let queries_dir = suite_dir.join("queries");
    let mut query_files: Vec<PathBuf> = Vec::new();

    if queries_dir.exists() {
        for entry in std::fs::read_dir(&queries_dir)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", queries_dir.display()))
        {
            let entry = entry.expect("failed to read directory entry");
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                query_files.push(path);
            }
        }
    }

    // Sort by filename for deterministic ordering
    query_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let queries = query_files
        .into_iter()
        .map(|path| {
            let stem = path
                .file_stem()
                .expect("sql file should have a stem")
                .to_string_lossy()
                .to_string();
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
            let content = substitute_env_vars(&content);
            let stmts: Vec<String> = content
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            (stem, stmts)
        })
        .collect();

    Suite { setup_sql, queries }
}

/// Create a [`SessionContext`] from the given config, respecting the
/// `DATAFUSION_RUNTIME_MEMORY_LIMIT` environment variable for memory pool
/// configuration (e.g. `2G`, `512M`).
///
/// This mirrors the behavior of `CommonOpt::runtime_env_builder()` in the
/// `dfbench` CLI binary, ensuring the criterion benchmarks honor the same
/// environment variable. See <https://github.com/apache/datafusion/pull/20631>.
fn make_session_context(config: SessionConfig) -> SessionContext {
    let memory_limit = std::env::var("DATAFUSION_RUNTIME_MEMORY_LIMIT")
        .ok()
        .and_then(|val| {
            SessionContext::parse_capacity_limit(
                "DATAFUSION_RUNTIME_MEMORY_LIMIT",
                &val,
            )
            .ok()
        });

    if let Some(limit) = memory_limit {
        let pool = Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(limit),
            NonZeroUsize::new(5).unwrap(),
        ));
        let rt_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("failed to build RuntimeEnv with memory pool");
        SessionContext::new_with_config_rt(config, rt_env)
    } else {
        SessionContext::new_with_config(config)
    }
}

/// Execute setup SQL against a [`SessionContext`], splitting by `;` and running
/// each statement in order.
pub fn execute_setup(rt: &Runtime, ctx: &SessionContext, setup_sql: &str) {
    let stmts: Vec<String> = setup_sql
        .split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    rt.block_on(execute_sqls(ctx, &stmts));
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

/// Execute a single query (which may consist of multiple SQL statements).
///
/// This is a convenience wrapper around [`execute_sqls`] for use with the
/// runtime.
pub fn execute_query(rt: &Runtime, ctx: &SessionContext, query_sql: &[String]) {
    rt.block_on(execute_sqls(ctx, query_sql));
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
/// - **`sample_size(10)`** — minimum number of measurements per benchmark. 10 is criterion's minimum.
/// - **`measurement_time(5s)`** — minimum time budget per benchmark function.
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
            // to warm caches (OS page cache, parquet metadata, etc.).
            // Queries run concurrently to reduce total warmup time for large
            // suites (e.g. IMDB with 113 queries).
            let ctx = make_ctx();
            rt.block_on(async {
                let futs: FuturesUnordered<_> = queries
                    .iter()
                    .map(|(_, sqls)| execute_sqls(&ctx, sqls))
                    .collect();
                futs.collect::<Vec<()>>().await;
            });
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
            //
            // We use the sync `iter_custom` (not `to_async`) because
            // `make_ctx` may call `rt.block_on()` for table registration,
            // which would panic if called from within an async context.
            for (name, sqls) in queries {
                group.bench_function(name, |b| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let ctx = make_ctx();
                            let start = Instant::now();
                            rt.block_on(execute_sqls(&ctx, sqls));
                            total += start.elapsed();
                        }
                        total
                    });
                });
            }
        }
    }
    group.finish();
}

/// Top-level entry point for running a data-driven benchmark suite.
///
/// This function:
/// 1. Creates a [`criterion::Criterion`] instance
/// 2. Resolves the suite directory to `{workspace}/benchmarks/suites/{suite_name}/`
/// 3. Discovers the suite (setup SQL + query files)
/// 4. Runs warm and cold benchmark groups
/// 5. Calls `criterion.final_summary()`
///
/// Benchmark suites are defined as directories containing an optional `setup.sql`
/// and a `queries/` subdirectory with `.sql` files. Environment variables in SQL
/// files are resolved via [`substitute_env_vars`].
pub fn run_suite_main(suite_name: &str) {
    let mut criterion = criterion::Criterion::default().configure_from_args();
    let rt = make_runtime();

    let suite_dir = workspace_root()
        .join("benchmarks")
        .join("suites")
        .join(suite_name);

    // Ensure data is available (idempotent — skips if data already exists).
    run_load_script(&suite_dir);

    let suite = discover_suite(&suite_dir);

    let make_ctx = || {
        let config = SessionConfig::from_env().unwrap();
        let ctx = make_session_context(config);
        if let Some(ref setup_sql) = suite.setup_sql {
            execute_setup(&rt, &ctx, setup_sql);
        }
        ctx
    };

    // Warm mode: reuse a single context across all queries
    run_sql_benchmarks(
        &mut criterion,
        &rt,
        suite_name,
        BenchMode::Warm,
        &make_ctx,
        &suite.queries,
    );

    // Cold mode: fresh context per iteration
    run_sql_benchmarks(
        &mut criterion,
        &rt,
        &format!("{suite_name}_cold"),
        BenchMode::Cold,
        &make_ctx,
        &suite.queries,
    );

    criterion.final_summary();
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
