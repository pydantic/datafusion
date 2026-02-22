-- Test query for lock contention measurement
-- ClickBench Q23 (the one with regression)
SET datafusion.execution.parquet.pushdown_filters = true;
SET datafusion.execution.parquet.reorder_filters = true;
SET datafusion.execution.parquet.binary_as_string = true;

CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 'benchmarks/data/hits_partitioned/';

SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY "EventTime" LIMIT 10;
