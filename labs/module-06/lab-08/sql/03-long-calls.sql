-- Part 4.1: a persistent query that filters. It keeps running on the ksqlDB server
-- and writes every call of 2 minutes or more to a new topic.
CREATE STREAM ${ME}_long_calls
  WITH (KAFKA_TOPIC='${ME}.ksql.long_calls', PARTITIONS=3, REPLICAS=3) AS
  SELECT caller, call_id, callee, duration_sec, roaming
  FROM ${ME}_calls
  WHERE duration_sec >= 120
  EMIT CHANGES;
