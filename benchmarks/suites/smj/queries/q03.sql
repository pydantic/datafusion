WITH t1_sorted AS (
    SELECT value % 10000 as key, value as data
    FROM range(1000000)
    ORDER BY key, data
),
t2_sorted AS (
    SELECT value % 10000 as key, value as data
    FROM range(1000000)
    ORDER BY key, data
)
SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
