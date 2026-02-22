SET datafusion.execution.parquet.pushdown_filters = false;
SET datafusion.execution.target_partitions = 1;

CREATE EXTERNAL TABLE store_sales STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/store_sales.parquet';

-- Check metadata
SELECT count(*) FROM store_sales;
