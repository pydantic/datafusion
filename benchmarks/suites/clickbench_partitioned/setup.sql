SET datafusion.execution.parquet.binary_as_string = true;
CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '${CLICKBENCH_DATA:-${WORKSPACE}/benchmarks/data/hits_partitioned}';
CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw;
