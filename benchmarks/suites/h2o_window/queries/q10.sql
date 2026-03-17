SELECT
    id1,
    id2,
    id3,
    v2,
    avg(v2) OVER (PARTITION BY id2 ORDER BY id3 ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS my_moving_average_by_id2
FROM large
