SET datafusion.execution.parquet.pushdown_filters = true;
SET datafusion.execution.parquet.reorder_filters = true;
SET datafusion.execution.parquet.binary_as_string = true;
SET datafusion.execution.target_partitions = 1;

CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 'benchmarks/data/hits_partitioned/';

explain analyze
SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY "EventTime" LIMIT 10;
