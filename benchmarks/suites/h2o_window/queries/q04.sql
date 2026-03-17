SELECT
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (PARTITION BY id2 ORDER BY id3) AS first_by_id2_ordered_by_id3
FROM large
