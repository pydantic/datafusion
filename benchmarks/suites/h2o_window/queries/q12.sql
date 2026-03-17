SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id2 ORDER BY v2 RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS my_range_between_by_id2
FROM large
