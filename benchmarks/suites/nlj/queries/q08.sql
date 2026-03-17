SELECT *
FROM range(200000) AS t1
LEFT JOIN range(10000) AS t2
ON (t1.value + t2.value) % 1000 = 0
