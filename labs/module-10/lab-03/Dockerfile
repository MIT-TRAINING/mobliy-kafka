# Module 10 Lab 03: the Confluent Platform Connect worker plus the Debezium PostgreSQL plugin.
# Built once by `docker compose build`. The plugin version is pinned, as it would be in production:
# every worker in a Connect cluster must run the same plugin version.
FROM confluentinc/cp-kafka-connect:8.3.2
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:3.2.6
