SELECT t1.*
FROM range(30000) AS t2
RIGHT ANTI JOIN range(30000) AS t1
ON t2.value < t1.value
