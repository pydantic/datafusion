CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION 'benchmarks/data/tpch_sf10/customer/';
CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'benchmarks/data/tpch_sf10/orders/';

EXPLAIN
SELECT customer.c_custkey || 'a'
FROM customer
JOIN orders on c_custkey = o_custkey
WHERE c_phone = '25-989-741-2988';

-- +---------------+------------------------------------------------------------+
-- | plan_type     | plan                                                       |
-- +---------------+------------------------------------------------------------+
-- | physical_plan | ┌───────────────────────────┐                              |
-- |               | │    CoalesceBatchesExec    │                              |
-- |               | │    --------------------   │                              |
-- |               | │     target_batch_size:    │                              |
-- |               | │            8192           │                              |
-- |               | └─────────────┬─────────────┘                              |
|               | ┌─────────────┴─────────────┐                              |
|               | │        HashJoinExec       │                              |
|               | │    --------------------   │                              |
|               | │            on:            ├──────────────┐               |
|               | │  (c_custkey = o_custkey)  │              │               |
|               | └─────────────┬─────────────┘              │               |
|               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
|               | │    CoalesceBatchesExec    ││    CoalesceBatchesExec    │ |
|               | │    --------------------   ││    --------------------   │ |
|               | │     target_batch_size:    ││     target_batch_size:    │ |
|               | │            8192           ││            8192           │ |
|               | └─────────────┬─────────────┘└─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
|               | │      RepartitionExec      ││      RepartitionExec      │ |
|               | │    --------------------   ││    --------------------   │ |
|               | │ partition_count(in->out): ││ partition_count(in->out): │ |
|               | │          12 -> 12         ││          12 -> 12         │ |
|               | │                           ││                           │ |
|               | │    partitioning_scheme:   ││    partitioning_scheme:   │ |
|               | │  List(o_custkey, 12)      ││  List(c_custkey, 12)      │ |
|               | └─────────────┬─────────────┘└─────────────┬─────────────┘ |
|               | ┌───────────────────────────┐┌───────────────────────────┐ |
|               | │       ProjectionExec      ││       ProjectionExec      │ |
|               | │    --------------------   ││    --------------------   │ |
|               | │       expr:               ││       expr:               │ |
|               | │  *, hash_part(o_custkey)  ││  *, hash_part(o_custkey)  │ |
|               | └─────────────┬─────────────┘└─────────────┬─────────────┘ |
-- |               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
-- |               | │    CoalesceBatchesExec    ││       DataSourceExec      │ |
-- |               | │    --------------------   ││    --------------------   │ |
-- |               | │     target_batch_size:    ││         files: 23         │ |
-- |               | │            8192           ││      format: parquet      │ |
-- |               | │                           ││      predicate: true      │ |
-- |               | └─────────────┬─────────────┘└───────────────────────────┘ |
-- |               | ┌─────────────┴─────────────┐                              |
-- |               | │         FilterExec        │                              |
-- |               | │    --------------------   │                              |
-- |               | │         predicate:        │                              |
-- |               | │ c_phone = 25-989-741-2988 │                              |
-- |               | └─────────────┬─────────────┘                              |
-- |               | ┌─────────────┴─────────────┐                              |
-- |               | │       DataSourceExec      │                              |
-- |               | │    --------------------   │                              |
-- |               | │         files: 23         │                              |
-- |               | │      format: parquet      │                              |
-- |               | │                           │                              |
-- |               | │         predicate:        │                              |
-- |               | │ c_phone = 25-989-741-2988 │                              |
-- |               | └───────────────────────────┘                              |
-- |               |                                                            |
-- +---------------+------------------------------------------------------------+
-- 1 row(s) fetched. 
-- Elapsed 0.019 seconds.