#!/bin/bash
set -e

DFBENCH="./dfbench-tunable"
CB_DATA="benchmarks/data/hits_partitioned"
CB_QUERIES="benchmarks/queries/clickbench/queries"
TPCDS_DATA="benchmarks/data/tpcds_sf1"
TPCDS_QUERIES="datafusion/core/tests/tpc-ds"
ITERS=3

run_cb() {
    local label="$1"; shift
    local q="$1"; shift
    local out
    out=$(env "$@" \
        DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
        DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true \
        DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true \
        $DFBENCH clickbench --iterations $ITERS --path $CB_DATA --queries-path $CB_QUERIES --query "$q" 2>&1)
    local avg
    avg=$(echo "$out" | grep "avg time" | sed 's/.*avg time: //' | sed 's/ ms//')
    printf "%-30s CB-Q%-3s %s ms\n" "$label" "$q" "$avg"
}

run_tpcds() {
    local label="$1"; shift
    local q="$1"; shift
    local out
    out=$(env "$@" \
        DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
        DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true \
        DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true \
        $DFBENCH tpcds --iterations $ITERS --path $TPCDS_DATA --query_path $TPCDS_QUERIES --query "$q" 2>&1)
    local avg
    avg=$(echo "$out" | grep "avg time" | sed 's/.*avg time: //' | sed 's/ ms//')
    printf "%-30s TPCDS-Q%-3s %s ms\n" "$label" "$q" "$avg"
}

echo "=== Key queries: CB-Q23 (wide, selective), TPCDS-Q9 (narrow), CB-Q4 (medium) ==="
echo ""

# CB Q23: wide table (105 cols), filter on 1 col. Low ratio → row-level is ideal.
# TPCDS Q9: narrow table (23 cols), filter on few cols. High ratio → post-scan is ideal.
# CB Q4: medium query

echo "--- Baseline (ratio=0.5, z=2.0, default collection) ---"
run_cb   "baseline" 23
run_cb   "baseline" 4
run_tpcds "baseline" 9

echo ""
echo "--- Vary byte ratio threshold ---"
for ratio in 0.01 0.05 0.1 0.2 0.3 0.5 0.8 1.0; do
    run_cb   "ratio=$ratio" 23 DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=$ratio
    run_cb   "ratio=$ratio" 4  DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=$ratio
    run_tpcds "ratio=$ratio" 9  DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=$ratio
    echo ""
done

echo "--- Vary z-score ---"
for z in 0.0 0.5 1.0 1.5 2.0 3.0; do
    run_cb   "z=$z" 23 DATAFUSION_CONFIDENCE_Z=$z
    run_tpcds "z=$z" 9  DATAFUSION_CONFIDENCE_Z=$z
    echo ""
done

echo "--- Vary min_rows ---"
for rows in 100 500 1000 2000 5000 10000 50000; do
    run_cb   "minrows=$rows" 23 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=$rows \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=$rows
    run_tpcds "minrows=$rows" 9 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=$rows \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=$rows
    echo ""
done

echo "--- Vary min_bytes_per_sec ---"
for bps in 1000000 10000000 50000000 100000000 500000000 1000000000; do
    run_cb   "bps=$bps" 23 DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=$bps
    run_tpcds "bps=$bps" 9  DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=$bps
    echo ""
done

echo "=== Best combo candidates ==="
for combo_label in \
    "r0.1/z1.0/mr1000/bps50M:DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.1:DATAFUSION_CONFIDENCE_Z=1.0:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=1000:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=1000:DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=50000000" \
    "r0.2/z1.5/mr2000/bps100M:DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.2:DATAFUSION_CONFIDENCE_Z=1.5:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=2000:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=2000:DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=100000000" \
    "r0.3/z2.0/mr5000/bps100M:DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.3:DATAFUSION_CONFIDENCE_Z=2.0:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=5000:DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=5000:DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=100000000"
do
    IFS=: read -r label e1 e2 e3 e4 e5 <<< "$combo_label"
    run_cb   "$label" 23 "$e1" "$e2" "$e3" "$e4" "$e5"
    run_cb   "$label" 4  "$e1" "$e2" "$e3" "$e4" "$e5"
    run_tpcds "$label" 9  "$e1" "$e2" "$e3" "$e4" "$e5"
    echo ""
done
