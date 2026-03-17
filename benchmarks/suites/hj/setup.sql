CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/lineitem/';
CREATE EXTERNAL TABLE supplier STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/supplier/';
CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/nation/';
CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/customer/';
