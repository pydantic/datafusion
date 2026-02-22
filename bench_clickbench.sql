set datafusion.execution.parquet.binary_as_string = true;
set datafusion.execution.target_partitions = 1;

CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION 'benchmarks/data/hits_partitioned/';

SELECT "MobilePhone", "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> '' GROUP BY "MobilePhone", "MobilePhoneModel" ORDER BY u DESC LIMIT 10;
