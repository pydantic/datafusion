SELECT *
FROM range(30000) AS t1
JOIN range(30000) AS t2
ON (t1.value + t2.value) % 5 = 0
