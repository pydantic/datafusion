# --- Current Branch: Dynamic ON ---
export RESULTS_NAME=dynamic-on-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
export DATAFUSION_OPTIMIZER_REPARTITION_FILE_MIN_SIZE=5242880
export DATAFUSION_OPTIMIZER_ENABLE_JOIN_DYNAMIC_FILTER_PUSHDOWN=false
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned

# --- Current Branch: Dynamic OFF ---
export RESULTS_NAME=dynamic-off-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=false
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
export DATAFUSION_OPTIMIZER_REPARTITION_FILE_MIN_SIZE=5242880
export DATAFUSION_OPTIMIZER_ENABLE_JOIN_DYNAMIC_FILTER_PUSHDOWN=false
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned

# --- Switch to Main ---
git checkout main

# --- Main Branch: ON ---
export RESULTS_NAME=main-on-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned

# --- Main Branch: OFF ---
export RESULTS_NAME=main-off-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=false
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned

# --- Main Branch Control: ON ---
export RESULTS_NAME=mainctl-on-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=true
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned

# --- Main Branch Control: OFF ---
export RESULTS_NAME=mainctl-off-1-thread
export DATAFUSION_EXECUTION_TARGET_PARTITIONS=1
export DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS=false
export DATAFUSION_EXECUTION_PARQUET_REORDER_FILTERS=true
# ./bench.sh run tpch
# ./bench.sh run tpch10
# ./bench.sh run tpcds
./bench.sh run clickbench_partitioned