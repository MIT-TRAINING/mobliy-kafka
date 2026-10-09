-- Part 4.2: a persistent aggregation. The result is a TABLE: one row per caller,
-- updated every time that caller makes a call.
CREATE TABLE ${ME}_minutes_by_caller
  WITH (KAFKA_TOPIC='${ME}.ksql.minutes_by_caller', PARTITIONS=3, REPLICAS=3) AS
  SELECT caller,
         COUNT(*)          AS calls,
         SUM(duration_sec) AS total_sec
  FROM ${ME}_calls
  GROUP BY caller
  EMIT CHANGES;
