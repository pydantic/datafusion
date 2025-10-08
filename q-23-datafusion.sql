SET datafusion.execution.parquet.pushdown_filters = true;
SET datafusion.execution.parquet.reorder_filters = true;
SET datafusion.execution.parquet.schema_force_view_types = true;

CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 'benchmarks/data/hits_partitioned/';

EXPLAIN ANALYZE
SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY "EventTime" LIMIT 10;
