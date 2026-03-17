WITH t1_sorted AS (
    SELECT value as key FROM range(100000) ORDER BY value
),
t2_sorted AS (
    SELECT value as key FROM range(100000) ORDER BY value
)
SELECT t1_sorted.key as k1, t2_sorted.key as k2
FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
