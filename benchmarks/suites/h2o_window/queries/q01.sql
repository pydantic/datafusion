SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER () AS window_basic
FROM large
