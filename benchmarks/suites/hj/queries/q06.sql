SELECT l.k
FROM (
  SELECT CASE
              WHEN l_suppkey % 10 = 0 THEN l_suppkey * 4 / 3
              WHEN l_suppkey % 10 < 9 THEN (l_suppkey * 4 / 3 / 4) * 4 + 3
              ELSE l_suppkey * 4 / 3 + 1000000
         END as k
  FROM lineitem
) l
JOIN (
  SELECT s_suppkey * 4 / 3 as k FROM supplier
) s ON l.k = s.k
