-- Part 3: a STREAM over a new topic. ksqlDB creates the topic because PARTITIONS is given.
-- Names start with your prefix: the topic for RBAC on Platform, the stream because
-- every learner shares one ksqlDB server there.
CREATE STREAM ${ME}_calls (
  caller        VARCHAR KEY,
  call_id       VARCHAR,
  callee        VARCHAR,
  duration_sec  INT,
  roaming       BOOLEAN
) WITH (KAFKA_TOPIC='${ME}.ksql.calls', VALUE_FORMAT='JSON', PARTITIONS=3, REPLICAS=3);
