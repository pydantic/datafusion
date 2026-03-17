SELECT l.k
FROM (
  SELECT l_suppkey * 4 / 3 as k
  FROM lineitem
) l
JOIN (
  SELECT s_suppkey * 4 / 3 as k FROM supplier
) s ON l.k = s.k
