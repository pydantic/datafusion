SELECT
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (ORDER BY id3 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS my_lag,
    first_value(v2) OVER (ORDER BY id3 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS my_lead
FROM large
