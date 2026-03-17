SELECT
    id1,
    id2,
    id3,
    v2,
    first_value(v2) OVER (ORDER BY id3) AS first_order_by,
    row_number() OVER (ORDER BY id3) AS row_number_order_by
FROM large
