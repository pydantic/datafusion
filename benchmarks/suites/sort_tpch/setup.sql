CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '${TPCH_DATA:-${WORKSPACE}/benchmarks/data/tpch_sf1}/lineitem/';
