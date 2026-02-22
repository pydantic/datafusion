#!/bin/bash
set -e

RUNS=3
RESULTS_DIR="bench_results"
mkdir -p "$RESULTS_DIR"

run_bench() {
    local label="$1"
    local binary="$2"
    local sql_file="$3"
    local pushdown="$4"
    local outfile="$RESULTS_DIR/${label}.txt"

    echo "=== $label ===" | tee "$outfile"
    for i in $(seq 1 $RUNS); do
        echo "  Run $i..." >&2
        DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=$pushdown \
        DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=$pushdown \
        /usr/bin/time -l "$binary" -f "$sql_file" > /dev/null 2> "$RESULTS_DIR/${label}_run${i}_time.txt"
        # Extract real time from time output
        real_secs=$(grep "real" "$RESULTS_DIR/${label}_run${i}_time.txt" | awk '{print $1}' || true)
        if [ -z "$real_secs" ]; then
            # macOS time format: total seconds on first line
            real_secs=$(head -1 "$RESULTS_DIR/${label}_run${i}_time.txt" | awk '{print $1}')
        fi
        echo "  Run $i: ${real_secs}s" | tee -a "$outfile"
    done
}

echo "Starting benchmarks at $(date)"

# Use hyperfine if available, otherwise fall back to time
if command -v hyperfine &>/dev/null; then
    echo "Using hyperfine"

    for config in "main_pushon:datafusion-cli-main-new:true" "main_pushoff:datafusion-cli-main-new:false" "dynamic_pushon:datafusion-cli-dynamic-new:true" "dynamic_pushoff:datafusion-cli-dynamic-new:false"; do
        IFS=: read -r label binary pushdown <<< "$config"
        for bench in "clickbench:bench_clickbench.sql" "tpcds_q9:bench_tpcds_q9.sql"; do
            IFS=: read -r benchname sql <<< "$bench"
            name="${label}_${benchname}"
            echo "--- $name ---"
            DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=$pushdown \
            DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=$pushdown \
            hyperfine --warmup 1 --runs $RUNS --export-json "$RESULTS_DIR/${name}.json" \
                "./$binary -f $sql" 2>&1 | tee "$RESULTS_DIR/${name}.txt"
        done
    done
else
    echo "Using /usr/bin/time (hyperfine not found)"

    for config in "main_pushon:datafusion-cli-main-new:true" "main_pushoff:datafusion-cli-main-new:false" "dynamic_pushon:datafusion-cli-dynamic-new:true" "dynamic_pushoff:datafusion-cli-dynamic-new:false"; do
        IFS=: read -r label binary pushdown <<< "$config"
        for bench in "clickbench:bench_clickbench.sql" "tpcds_q9:bench_tpcds_q9.sql"; do
            IFS=: read -r benchname sql <<< "$bench"
            name="${label}_${benchname}"
            run_bench "$name" "./$binary" "$sql" "$pushdown"
        done
    done
fi

echo ""
echo "All benchmarks completed at $(date)"
echo "Results in $RESULTS_DIR/"
