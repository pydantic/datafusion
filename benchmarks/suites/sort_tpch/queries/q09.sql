SELECT l_orderkey, l_suppkey, l_linenumber, l_comment, l_partkey
FROM lineitem
ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
