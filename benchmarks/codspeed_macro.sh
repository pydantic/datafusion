#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
DATA_DIR="${SCRIPT_DIR}/data"
DFBENCH="./target/release/dfbench"

# TPC-H SF1 (22 queries)
for q in $(seq 1 22); do
  codspeed exec -- $DFBENCH tpch --iterations 1 --path "${DATA_DIR}/tpch_sf1" --format parquet --query $q
done

# ClickBench partitioned (43 queries: q0-q42, 100 parquet files)
for q in $(seq 0 42); do
  codspeed exec -- $DFBENCH clickbench --iterations 1 --path "${DATA_DIR}/hits_partitioned" --queries-path "${SCRIPT_DIR}/queries/clickbench/queries" --query $q
done
