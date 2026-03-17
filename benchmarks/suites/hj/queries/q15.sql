SELECT l.k
FROM (
  SELECT CASE
              WHEN l_suppkey % 10 = 0 THEN ((l_suppkey % 80000) + 1) * 25 / 4
              ELSE ((l_suppkey % 80000) + 1) * 25 / 4 + 1
         END as k
  FROM lineitem
) l
JOIN (
  SELECT CASE
              WHEN s_suppkey <= 80000 THEN (s_suppkey * 25) / 4
              ELSE ((s_suppkey - 80000) * 25) / 4
         END as k
  FROM supplier
) s ON l.k = s.k
