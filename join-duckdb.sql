EXPLAIN ANALYZE
SELECT *
FROM read_parquet('benchmarks/data/tpch_sf10/customer/*.parquet') AS customer
JOIN read_parquet('benchmarks/data/tpch_sf10/orders/*.parquet') AS orders ON customer.c_custkey = orders.o_custkey
WHERE customer.c_phone = '25-989-741-2988';