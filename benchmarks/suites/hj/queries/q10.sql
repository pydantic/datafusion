SELECT l.k
FROM (
  SELECT CASE
              WHEN l_suppkey % 10 = 0 THEN l_suppkey * 5
              WHEN l_suppkey % 10 < 9 THEN l_suppkey * 5 + 1
              ELSE l_suppkey * 5 + 1000000
         END as k
  FROM lineitem
) l
JOIN (
  SELECT s_suppkey * 5 as k FROM supplier
) s ON l.k = s.k
