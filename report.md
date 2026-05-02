# Wide-schema parquet read perf — investigation log

## Branches and upstream

- DataFusion: `adrian/wide-schema-perf` on `pydantic/datafusion` (remote `origin`).
  - Pushed at: <https://github.com/pydantic/datafusion/tree/adrian/wide-schema-perf>
  - Based on `main` at `147617df4` (the wide-schema benchmark commit).
- arrow-rs: `adrian/wide-schema-perf` on `pydantic/arrow-rs` (remote `origin`).
  - Pushed at: <https://github.com/pydantic/arrow-rs/tree/adrian/wide-schema-perf>
  - Based on `main` at `d3cad6e0a`.

The DataFusion workspace at `Cargo.toml` patches `arrow-*` and `parquet`
to the local arrow-rs checkout, so the two branches are tested together.

## Benchmark and reproduction

```
BENCH_SUBGROUP=wide   cargo bench --bench sql -- "wide_schema"
BENCH_SUBGROUP=narrow cargo bench --bench sql -- "wide_schema"
```

For faster iteration I drove queries through `datafusion-cli`:

```
./target/profiling/datafusion-cli -f /tmp/q_wide_norun.sql
./target/profiling/datafusion-cli -f /tmp/q_narrow_norun.sql
```

with each `.sql` running `CREATE EXTERNAL TABLE events ...` once and the
Q04 SELECT 10 times back-to-back. Profiling used `samply record
--unstable-presymbolicate --save-only` and a small Python aggregator
(see `/tmp/profile_top.py`, `/tmp/compare2.py`).

## Findings

The wide dataset has **1024 cols × 256 files × 50k rows** and the
narrow dataset has **8 cols × 256 files × 50k rows** — same row count,
same file count, same bytes scanned per query. So any wide-vs-narrow
gap is per-file CPU overhead that scales with schema width.

### 1. Metadata cache thrashing (default 50MB cache)

Per-file `ParquetMetaData` for the wide dataset is ~1.5MB; the dataset
total is ~400MB. The default `metadata_cache_limit` is 50MB, so only
~30 of 256 files fit and the cache constantly evicts entries that other
threads still want. Effect on Q04 hot-cache wall time as a function of
cache limit (cold + 5 hot, ms; lower is better):

| limit | cold | hot (median) |
|---|---|---|
| 0 (disabled) | 705 | 100 |
| 10M | 707 | 85 |
| 50M (default) | 716 | **136** ← worst |
| 100M | 702 | 95 |
| 200M | 698 | 120 |
| 300M | 654 | 110 |
| 400M | 591 | 55 |
| 500M | 623 | 52 |
| 1G | 669 | 80 |
| 4G | 648 | 63 |

The 50M default lands precisely in the worst regime: large enough that
threads compete on the cache mutex and re-fetch evicted entries, small
enough that almost nothing stays cached. With cache disabled (no lock
contention) it's actually faster than 50M. With the cache sized to fit
the working set (~400M+) it's ~2.5× faster than the default.

Profiling at 50M showed `_pthread_mutex_firstfit_lock_wait` at ~12% of
samples and `evict_entries` plus `Arc::drop_slow` of `ParquetMetaData`
at another ~3%.

### 2. O(N²) per-file CPU in `statistics_from_parquet_metadata`

`statistics_from_parquet_metadata` iterates every logical field and
calls `StatisticsConverter::try_new`, which internally does an O(N)
linear scan in `parquet_column` (the comment said "this could be made
more efficient (#TBD)"). Plus an O(N) `Fields::find` on the arrow
side. For the wide dataset that's `1024² × 256 ~ 268M` ops per query
just to set up stats.

The same `parquet_column` linear scan is hit again per row group from
`row_group_filter` and `page_filter` for each filter column.

### 3. Per-file arrow-schema reconstruction

Every file open went through `ArrowReaderMetadata::try_new`, which
calls `parquet_to_arrow_schema_and_fields` to walk every parquet leaf
and build the matching arrow `Schema` + `FieldLevels`. With 256 files
that's 256 full walks per query, even though they all share the same
logical schema. Plus the embedded arrow-IPC schema metadata is
flatbuffer-decoded per file.

In the warm-cache profile this showed up as:
- `ArrowReaderMetadata::try_new` 4.6%
- `parquet_to_arrow_schema_and_fields` 4.2%
- `flatbuffers::verifier::TableVerifier::visit_field` 1.4%
- `arrow_ipc::convert::fb_to_schema` 1%

### 4. Smaller per-file walks

- `apply_file_schema_type_coercions` always built a 1024-entry HashMap
  even when the early-return condition would fire and discard it.
- `DefaultFilesMetadataCache::put`/`evict_entries`/`remove` called
  `FileMetadata::memory_size()` (which walks the entire metadata
  structure) every time, even though the size doesn't change after
  insertion.

## Changes made

### arrow-rs (`adrian/wide-schema-perf`)

| File | Change |
|---|---|
| `parquet/src/schema/types.rs` | `SchemaDescriptor` precomputes a `root_to_first_leaf` inverse map at construction; new `root_first_leaf_index` accessor. |
| `parquet/src/arrow/mod.rs` | `parquet_column` uses `root_first_leaf_index` (O(N) scan → O(1) lookup). New re-export of `parquet_to_arrow_schema_and_field_levels`. |
| `parquet/src/arrow/schema/mod.rs` | New public `parquet_to_arrow_schema_and_field_levels` that returns both `(Schema, FieldLevels)` in one walk. |
| `parquet/src/arrow/arrow_reader/mod.rs` | New `ArrowReaderMetadata::from_field_levels` constructor that packages a precomputed `(metadata, schema, field_levels)` triple — bypasses `try_new`'s per-leaf walk. New `ArrowReaderOptions::supplied_schema()`/`skip_arrow_metadata()`/`virtual_columns()` accessors so callers can decide whether their cached arrow view is applicable. |
| `parquet/src/arrow/arrow_reader/statistics.rs` | New `StatisticsConverter::from_arrow_field` constructor that takes a resolved `(field, parquet_leaf_index)` pair, skipping the redundant arrow + parquet name lookups inside `try_new`. |
| `parquet/src/arrow/async_reader/mod.rs` | New `AsyncFileReader::get_arrow_reader_metadata` trait method (default impl delegates to `try_new`). `load_async` now goes through it so cache-aware readers can short-circuit. |

### DataFusion (`adrian/wide-schema-perf`)

| File | Change |
|---|---|
| `datafusion/datasource-parquet/src/metadata.rs` | `statistics_from_parquet_metadata` rewritten to be O(N) per file: precompute a logical→parquet leaf index map once, use the new `from_arrow_field` constructor in the loop, drop the redundant `parquet_column` lookup inside `summarize_column_statistics`. `CachedParquetMetaData` now holds a `OnceLock<CachedArrowView>` with the per-file arrow `Schema` and `FieldLevels`, lazily built from the cached parquet metadata. |
| `datafusion/datasource-parquet/src/reader.rs` | `CachedParquetFileReader` overrides `get_arrow_reader_metadata` so warm-cache hits return a fully-built `ArrowReaderMetadata` via `from_field_levels` instead of re-walking the parquet schema. The duplicate `CachedParquetMetaData` definition that lived in `reader.rs` was removed in favor of the one in `metadata.rs`. |
| `datafusion/datasource-parquet/src/file_format.rs` | `apply_file_schema_type_coercions` does the early-return check first; only builds the 1024-entry name → type HashMap when transforms are actually required. |
| `datafusion/execution/src/cache/file_metadata_cache.rs` | `DefaultFilesMetadataCache` stores `memory_size` alongside the entry so `put`/`evict_entries`/`remove` don't re-walk the metadata structure. |

## Measured impact (Q04, profiling build)

| Scenario | Before | After | Δ |
|---|---|---|---|
| narrow, hot | ~25 ms | ~25 ms | (control) |
| wide @50M cache, **cold** | ~1010 ms | ~700 ms | **−31%** |
| wide @50M cache, hot | ~108 ms | ~92 ms | −15% |
| wide @2G cache, **cold** | ~830 ms | ~560 ms | **−33%** |
| wide @2G cache, hot | ~47 ms | ~42 ms | −11% |

In the warm-cache profile after the fixes:
- `ArrowReaderMetadata::try_new` 4.6% → **1.3%**
- `parquet_to_arrow_field_levels_with_virtual` (per-file walk) ~4% → **~1.5%** (and that 1.5% is `infer_schema` during CREATE TABLE — once per session)
- `statistics_from_parquet_metadata` 3.2% → **1.7%**
- `flatbuffers verifier` + `fb_to_schema` ~2.4% → **~0.4%**

## Verification

- `cargo test -p datafusion-datasource-parquet --lib metadata::` — pass.
- `cargo test -p datafusion-execution --lib` — 63 pass.
- `cargo test -p parquet --features arrow --lib schema::` — 104 pass.
- `cargo test -p parquet --features arrow --lib arrow_reader::` — 104 pass.
- The 16 `row_filter` / `row_group_filter` test failures in the parquet
  datasource crate are environmental — they need the
  `parquet-testing` git submodule (set `PARQUET_TEST_DATA` or run
  `git submodule update --init`); they fail the same way without my
  changes.

## Still open

The default cache size is the dominant remaining issue. Beyond bumping
it, the structural fix is reducing what needs to be cached (e.g. cache
the basic metadata always, defer the page index until needed) and/or
caching downstream-derived state across files that share a schema.

Per-file work in the morsel planner / predicate construction
(`prepare_filters`, `build_pruning_predicates`, `apply_file_schema_type_coercions`,
`PruningPredicate::try_new`) is still O(N_columns) per file. For
queries that only reference a handful of columns this is the next
target — a "reduced" arrow schema containing only referenced columns,
and per-(physical_file_schema, predicate) caches for the predicate
machinery, would push it toward O(num_columns_referenced).
