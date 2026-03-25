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

//! Shared benchmark harness for divan-based SQL benchmarks.
//!
//! This module provides a fully generic, data-driven SQL benchmark harness.
//! Benchmark suites are defined as directories under `benchmarks/suites/` with
//! an optional `setup.sql` and a `queries/` subdirectory containing `.sql` files.
//!
//! Each bench file invokes the [`suite_benchmarks!`] macro which generates
//! `#[divan::bench]` functions for warm and cold modes, with each query
//! appearing as a separate benchmark via divan's `args` support.
//!
//! Unlike criterion, divan supports `--sample-count 1` (or `sample_count = 1`
//! in the attribute), which is important for long-running benchmarks where
//! running multiple samples is impractical.

use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream::{FuturesUnordered, StreamExt};
use regex::Regex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::runtime::Runtime;

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

/// Lazily-initialized state shared across all benchmarks in a suite.
///
/// Created once via [`SuiteState::init`] and cached in a `OnceLock` by the
/// [`suite_benchmarks!`] macro.  Holds the tokio runtime, discovered queries
/// (SQL strings already in memory), and the pre-warmed session context.
pub struct SuiteState {
    pub rt: Runtime,
    pub suite: Suite,
    /// Pre-warmed context for warm-mode benchmarks.
    pub warm_ctx: SessionContext,
}

impl SuiteState {
    /// Initialize a suite: discover queries, run the load script, create and
    /// warm the session context.
    pub fn init(suite_name: &str) -> Self {
        let rt = make_runtime();

        let suite_dir = workspace_root()
            .join("benchmarks")
            .join("suites")
            .join(suite_name);

        // Ensure data is available (idempotent — skips if data already exists).
        run_load_script(&suite_dir);

        let suite = discover_suite(&suite_dir);

        // Create and warm the context.
        let config = SessionConfig::from_env().unwrap();
        let ctx = make_session_context(config);
        if let Some(ref setup_sql) = suite.setup_sql {
            execute_setup(&rt, &ctx, setup_sql);
        }

        // Run each query once concurrently to warm OS page cache, parquet
        // metadata caches, etc.
        rt.block_on(async {
            let futs: FuturesUnordered<_> = suite
                .queries
                .iter()
                .map(|(_, sqls)| execute_sqls(&ctx, sqls))
                .collect();
            futs.collect::<Vec<()>>().await;
        });

        SuiteState {
            rt,
            suite,
            warm_ctx: ctx,
        }
    }

    /// Return the list of query names (for use as divan `args`).
    pub fn query_names(&self) -> Vec<String> {
        self.suite
            .queries
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Look up pre-loaded SQL statements by query name.
    pub fn find_query(&self, name: &str) -> &[String] {
        self.suite
            .queries
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, sqls)| sqls.as_slice())
            .unwrap_or_else(|| panic!("query {name:?} not found in suite"))
    }

    /// Create a fresh [`SessionContext`] with tables registered but no warm
    /// caches.  Used by cold-mode benchmarks.
    pub fn make_fresh_ctx(&self) -> SessionContext {
        let config = SessionConfig::from_env().unwrap();
        let ctx = make_session_context(config);
        if let Some(ref setup_sql) = self.suite.setup_sql {
            execute_setup(&self.rt, &ctx, setup_sql);
        }
        ctx
    }
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
/// exists and only downloads/generates when missing.
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
pub fn discover_suite(suite_dir: &Path) -> Suite {
    let setup_path = suite_dir.join("setup.sql");
    let setup_sql = if setup_path.exists() {
        let content = std::fs::read_to_string(&setup_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", setup_path.display()));
        Some(substitute_env_vars(&content))
    } else {
        None
    };

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
pub async fn execute_sqls(ctx: &SessionContext, sqls: &[String]) {
    for sql in sqls {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
}

/// Generate divan benchmark functions for a suite.
///
/// This macro creates:
/// - A `state()` function that lazily initializes the [`SuiteState`] once
/// - A `query_names()` function that returns discovered query names (for divan `args`)
/// - A `warm` benchmark: shared pre-warmed context, only planning+execution timed
/// - A `cold` benchmark: fresh context per iteration (creation excluded from timing
///   via `with_inputs`), only planning+execution timed
/// - A `main()` that calls `divan::main()`
///
/// # Usage
///
/// Each benchmark binary is just:
/// ```ignore
/// datafusion_benchmarks::suite_benchmarks!("tpch");
/// ```
///
/// Run with:
/// ```text
/// cargo bench --bench tpch
/// cargo bench --bench tpch -- --sample-count 1   # single sample
/// cargo bench --bench tpch -- warm               # only warm benchmarks
/// cargo bench --bench tpch -- warm q1             # only warm/q1
/// ```
#[macro_export]
macro_rules! suite_benchmarks {
    ($suite_name:literal) => {
        // When the `codspeed` feature is enabled, swap in the CodSpeed
        // compatibility layer as `divan` so all `divan::bench` attributes
        // and `divan::main()` calls route through CodSpeed's instrumentation.
        #[cfg(feature = "codspeed")]
        extern crate codspeed_divan_compat_walltime as divan;

        use std::sync::OnceLock;

        fn state() -> &'static $crate::benchmark_harness::SuiteState {
            static STATE: OnceLock<$crate::benchmark_harness::SuiteState> =
                OnceLock::new();
            STATE.get_or_init(|| {
                $crate::benchmark_harness::SuiteState::init($suite_name)
            })
        }

        fn query_names() -> &'static [String] {
            static NAMES: OnceLock<Vec<String>> = OnceLock::new();
            NAMES.get_or_init(|| state().query_names())
        }

        #[divan::bench(args = query_names())]
        fn warm(bencher: divan::Bencher, query_name: &String) {
            let s = state();
            let sqls = s.find_query(query_name);
            bencher.bench_local(|| {
                s.rt.block_on($crate::benchmark_harness::execute_sqls(
                    &s.warm_ctx,
                    sqls,
                ))
            });
        }

        #[divan::bench(args = query_names())]
        fn cold(bencher: divan::Bencher, query_name: &String) {
            let s = state();
            let sqls = s.find_query(query_name);
            bencher
                .with_inputs(|| s.make_fresh_ctx())
                .bench_local_values(|ctx| {
                    s.rt.block_on(
                        $crate::benchmark_harness::execute_sqls(&ctx, sqls),
                    )
                });
        }

        fn main() {
            divan::main();
        }
    };
}
