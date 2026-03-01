copy (
    select {f1: 1, f2: 2} as s
) to 'struct.parquet';

create external table struct
stored as parquet
location 'struct.parquet';

SELECT
    get_field(s, 'f1') AS __datafusion_extracted_2
FROM struct
WHERE COALESCE(get_field(s, 'f1'), get_field(s, 'f2')) = 42;