#!/bin/bash
set -e

DFBENCH="./dfbench-tunable"
DATA="benchmarks/data/hits_partitioned"
QUERIES="benchmarks/queries/clickbench/queries"
ITERS=3

run_q() {
    local label="$1"; shift
    local q="$1"; shift
    # remaining args are env var assignments
    local out
    out=$(env "$@" \
        DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
        DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true \
        DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true \
        $DFBENCH clickbench --iterations $ITERS --path $DATA --queries-path $QUERIES --query "$q" 2>&1)
    local avg
    avg=$(echo "$out" | grep "avg time" | sed 's/.*avg time: //' | sed 's/ ms//')
    printf "%-25s Q%-3s %s ms\n" "$label" "$q" "$avg"
}

echo "=== Parameter sweep ==="
echo ""

# Baseline
for q in 23 28; do
    run_q "default(0.5/z2.0)" $q
done

echo ""
# Vary byte ratio threshold
for ratio in 0.02 0.05 0.1 0.3 1.0; do
    for q in 23 28; do
        run_q "ratio=$ratio" $q DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=$ratio
    done
    echo ""
done

# Vary z-score
for z in 0.5 1.0 1.5 3.0; do
    for q in 23 28; do
        run_q "z=$z" $q DATAFUSION_CONFIDENCE_Z=$z
    done
    echo ""
done

# Vary min_rows
for rows in 500 1000 2000 10000; do
    for q in 23 28; do
        run_q "minrows=$rows" $q \
            DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=$rows \
            DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=$rows
    done
    echo ""
done

# Vary min_bytes_per_sec
for bps in 1000000 10000000 100000000 500000000 1000000000; do
    for q in 23 28; do
        run_q "bps=$bps" $q DATAFUSION_EXECUTION_PARQUET_FILTER_PUSHDOWN_MIN_BYTES_PER_SEC=$bps
    done
    echo ""
done

# Best combo candidates
echo "=== Best combo candidates ==="
for q in 23 28; do
    run_q "combo1:r0.02/z1/mr1000" $q \
        DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.02 \
        DATAFUSION_CONFIDENCE_Z=1.0 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=1000 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=1000
done
echo ""

for q in 23 28; do
    run_q "combo2:r0.05/z1.5/mr2000" $q \
        DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.05 \
        DATAFUSION_CONFIDENCE_Z=1.5 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=2000 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=2000
done
echo ""

for q in 23 28; do
    run_q "combo3:r0.1/z2/mr5000" $q \
        DATAFUSION_COLLECTING_BYTE_RATIO_THRESHOLD=0.1 \
        DATAFUSION_CONFIDENCE_Z=2.0 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MIN_ROWS=5000 \
        DATAFUSION_EXECUTION_PARQUET_FILTER_STATISTICS_COLLECTION_MAX_ROWS=5000
done
