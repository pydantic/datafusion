WITH t1_sorted AS (
    SELECT value % 15000 as key, value as data
    FROM range(100000)
    ORDER BY key, data
),
t2_sorted AS (
    SELECT value % 10000 as key
    FROM range(5000000)
    ORDER BY key
)
SELECT t1_sorted.key, t1_sorted.data
FROM t1_sorted
WHERE NOT EXISTS (
    SELECT 1 FROM t2_sorted
    WHERE t2_sorted.key = t1_sorted.key
)
