CREATE VIEW hits AS SELECT * FROM read_parquet('benchmarks/data/hits_partitioned/*');

EXPLAIN ANALYZE
SELECT * FROM read_parquet('benchmarks/data/hits_partitioned/*') WHERE "URL"::text LIKE '%google%' ORDER BY "EventTime" LIMIT 10;
