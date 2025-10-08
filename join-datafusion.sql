SET datafusion.execution.parquet.pushdown_filters = true;
SET datafusion.execution.parquet.reorder_filters = true;
SET datafusion.execution.parquet.schema_force_view_types = true;


-- create two tables: small_table with 1K rows and large_table with 100K rows
CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION 'benchmarks/data/tpch_sf10/customer/';
CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'benchmarks/data/tpch_sf10/orders/';

-- Join the two tables, with a filter on small_table
EXPLAIN ANALYZE
SELECT *
FROM customer
JOIN orders on c_custkey = o_custkey
WHERE c_phone = '25-989-741-2988';