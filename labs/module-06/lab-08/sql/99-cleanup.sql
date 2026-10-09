-- Clean up: dropping a stream or table created with AS SELECT also stops its query.
-- DELETE TOPIC removes the Kafka topic as well. Drop the derived objects first.
DROP TABLE IF EXISTS ${ME}_minutes_by_caller DELETE TOPIC;
DROP STREAM IF EXISTS ${ME}_long_calls DELETE TOPIC;
DROP STREAM IF EXISTS ${ME}_calls DELETE TOPIC;
