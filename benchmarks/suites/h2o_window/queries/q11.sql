SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS my_rolling_sum_by_id2
FROM large
