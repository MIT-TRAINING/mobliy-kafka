#!/usr/bin/env bash
# Idempotent: SCRAM users, prefix ACLs, dynamic defaults and quotas for the shared cluster.
# Usage: bootstrap-security.sh ADMIN_PROPERTIES USERS_CSV [BOOTSTRAP]
#   USERS_CSV lines: user,password   (l01..l19 learners; the user "trainer" is treated as super user)
# Needs the Kafka CLI on PATH (or KAFKA_BIN=/opt/kafka/bin).
set -euo pipefail

ADMIN=${1:?admin client properties}
CSV=${2:?users csv}
BS=${3:-apache-kafka.lab.internal:9092}
K=${KAFKA_BIN:+$KAFKA_BIN/}
LEARNER_PRODUCE=${LEARNER_PRODUCE:-1048576}   # bytes/s per learner PER BROKER (cluster-wide cap = x number of brokers)
LEARNER_CONSUME=${LEARNER_CONSUME:-2097152}

cfg()  { "${K}kafka-configs.sh" --bootstrap-server "$BS" --command-config "$ADMIN" --alter "$@" >/dev/null; }
acl()  { "${K}kafka-acls.sh"    --bootstrap-server "$BS" --command-config "$ADMIN" --add "$@" >/dev/null; }

echo "== dynamic cluster defaults"
cfg --entity-type brokers --entity-default --add-config min.insync.replicas=2

echo "== quotas"
cfg --entity-type users --entity-default \
    --add-config "producer_byte_rate=$LEARNER_PRODUCE,consumer_byte_rate=$LEARNER_CONSUME,request_percentage=25,controller_mutation_rate=5"
cfg --entity-type users --entity-name trainer \
    --add-config 'producer_byte_rate=52428800,consumer_byte_rate=52428800,request_percentage=100,controller_mutation_rate=100'

echo "== users and ACLs"
while IFS=, read -r user password || [[ -n "$user" ]]; do   # "|| [[ -n ..." keeps a last line that has no trailing newline
  [[ -z "$user" || "$user" == \#* ]] && continue
  cfg --entity-type users --entity-name "$user" --add-config "SCRAM-SHA-512=[password=$password]"
  [[ "$user" == "trainer" ]] && { echo "  $user (super user)"; continue; }
  p="User:$user"
  acl --allow-principal "$p" --operation All --topic "$user." --resource-pattern-type prefixed
  acl --allow-principal "$p" --operation All --group "$user." --resource-pattern-type prefixed
  acl --allow-principal "$p" --operation All --transactional-id "$user." --resource-pattern-type prefixed
  acl --allow-principal "$p" --operation Describe --operation DescribeConfigs --cluster
  echo "  $user"
done < "$CSV"
echo "done"
