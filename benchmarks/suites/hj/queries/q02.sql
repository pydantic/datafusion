SELECT l.k
FROM (
  SELECT c_nationkey * 40 as k
  FROM customer
) l
JOIN (
  SELECT n_nationkey * 40 as k FROM nation
) s ON l.k = s.k
