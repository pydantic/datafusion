CREATE EXTERNAL TABLE part STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/part/';
CREATE EXTERNAL TABLE supplier STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/supplier/';
CREATE EXTERNAL TABLE partsupp STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/partsupp/';
CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/customer/';
CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/orders/';
CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/lineitem/';
CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/nation/';
CREATE EXTERNAL TABLE region STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/region/';
