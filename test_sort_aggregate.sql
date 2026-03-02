-- Test query for sort injection rule
-- This aggregate has multiple grouping keys which should trigger sort injection

CREATE TABLE events (
    timestamp INT,
    trace_id INT,
    category STRING,
    amount FLOAT
);

-- Verify the query plan shows a Sort before the Aggregate
EXPLAIN VERBOSE
SELECT timestamp, trace_id, category, COUNT(*) as cnt, SUM(amount) as total
FROM events
GROUP BY timestamp, trace_id, category;
