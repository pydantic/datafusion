SELECT
    id1,
    id2,
    id3,
    v2,
    sum(v2) OVER (PARTITION BY id1) AS sum_by_id1,
    sum(v2) OVER (PARTITION BY id2) AS sum_by_id2,
    sum(v2) OVER (PARTITION BY id3) AS sum_by_id3
FROM large
