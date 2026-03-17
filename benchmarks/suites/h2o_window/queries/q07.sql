SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (ORDER BY id3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS my_rolling_sum
FROM large
