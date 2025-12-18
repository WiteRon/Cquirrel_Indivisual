SELECT l_orderkey, o_orderdate, o_shippriority,
SUM(l_extendedprice * (1- l_discount)) AS revenue
FROM lineitem, customer, orders
WHERE c_mktsegment = 'AUTOMOBILE'
AND c_custkey = o_custkey AND l_orderkey = o_orderkey
AND o_orderdate < date '1995-03-13'
AND l_shipdate > date '1995-03-13'
GROUP BY l_orderkey, o_orderdate, o_shippriority

