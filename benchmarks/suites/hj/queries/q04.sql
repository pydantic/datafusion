SELECT l.k
FROM (
  SELECT CASE WHEN l_suppkey % 10 = 0 THEN l_suppkey ELSE l_suppkey + 1000000 END as k
  FROM lineitem
) l
JOIN (
  SELECT s_suppkey as k FROM supplier
) s ON l.k = s.k
