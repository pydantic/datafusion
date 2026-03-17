WITH t1_sorted AS (
    SELECT value % 10000 as key, value as data
    FROM range(100000)
    ORDER BY key, data
),
t2_sorted AS (
    SELECT value % 10000 as key, value as data
    FROM range(1000000)
    ORDER BY key, data
)
SELECT t1_sorted.key as k1, t1_sorted.data as d1,
       t2_sorted.key as k2, t2_sorted.data as d2
FROM t1_sorted FULL JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
WHERE (t1_sorted.data IS NULL OR t2_sorted.data IS NULL
       OR t1_sorted.data <> t2_sorted.data)
  AND (t1_sorted.data IS NULL OR t1_sorted.data % 10 = 0)
