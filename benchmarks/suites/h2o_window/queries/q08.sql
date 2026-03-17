SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (ORDER BY v2 RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS my_range_between
FROM large
