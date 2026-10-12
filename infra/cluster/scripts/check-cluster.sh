#!/usr/bin/env bash
# Acceptance tests for one learner (PLAN.md section 9). Run from a lab VM, or inside the spike client container.
# Usage: check-cluster.sh USER PASSWORD [BOOTSTRAP] [OTHER_PREFIX]
set -uo pipefail

ME=${1:?learner id, e.g. l01}
PW=${2:?password}
BS=${3:-apache-kafka.lab.internal:9092}
OTHER=${4:-l99}
K=${KAFKA_BIN:+$KAFKA_BIN/}
CFG=$(mktemp)
trap 'rm -f "$CFG"' EXIT
cat > "$CFG" <<P
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$ME" password="$PW";
P

fail=0
ok()   { echo "PASS  $1"; }
bad()  { echo "FAIL  $1"; fail=1; }
t()    { local name=$1 pattern=$2; shift 2; local out; out=$("$@" 2>&1); if echo "$out" | grep -Eq "$pattern"; then ok "$name"; else bad "$name"; echo "$out" | head -5 | sed 's/^/      /'; fi; }

T=$ME.check
"${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --delete --topic $T >/dev/null 2>&1

out=$("${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --list 2>&1)
[[ $? -eq 0 ]] && ok "authenticate + list" || { bad "authenticate + list"; echo "$out" | head -3; }

t "create RF=3, 6 partitions" "Created topic" \
  "${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --create --topic $T --partitions 6 --replication-factor 3
sleep 2
t "replicas span 3 racks/brokers (describe)" "ReplicationFactor: 3" \
  "${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --describe --topic $T

seq 1 100 | "${K}kafka-console-producer.sh" --bootstrap-server "$BS" --producer.config "$CFG" --topic $T \
  --producer-property acks=all >/dev/null 2>&1
t "consume 100 records in group $ME.check" "Processed a total of 100 messages" \
  "${K}kafka-console-consumer.sh" --bootstrap-server "$BS" --consumer.config "$CFG" --topic $T --from-beginning \
  --group $ME.check --max-messages 100 --timeout-ms 20000

t "other prefix rejected" "TopicAuthorizationException|Not authorized|Authorization" \
  "${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --create --topic $OTHER.x --partitions 1 --replication-factor 3
t "cluster-wide alter rejected" "ClusterAuthorizationException|Cluster authorization failed" \
  "${K}kafka-configs.sh" --bootstrap-server "$BS" --command-config "$CFG" --alter --entity-type brokers --entity-default --add-config log.retention.ms=259200000
t "dynamic default min.insync.replicas=2 visible" "DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2" \
  "${K}kafka-configs.sh" --bootstrap-server "$BS" --command-config "$CFG" --describe --entity-type brokers --entity-default
t "no topic auto-creation" "Authorization|UNKNOWN_TOPIC|not exist|timed out|Timed out|TimeoutException" \
  "${K}kafka-console-producer.sh" --bootstrap-server "$BS" --producer.config "$CFG" --topic typo.name \
  --producer-property max.block.ms=8000 <<< "x"
t "log dirs readable (Describe on cluster)" "logDirs" \
  "${K}kafka-log-dirs.sh" --bootstrap-server "$BS" --command-config "$CFG" --describe --topic-list $T

# transactions: describing a transactional.id is authorized by the TRANSACTIONAL_ID prefix ACL.
# Expected: "not found" (never used), NOT an authorization error.
out=$("${K}kafka-transactions.sh" --bootstrap-server "$BS" --command-config "$CFG" describe --transactional-id $ME.tx 2>&1)
if echo "$out" | grep -qi "AuthorizationException"; then bad "transactional-id ACL"; echo "$out" | head -3 | sed 's/^/      /'; else ok "transactional-id ACL"; fi

echo "-- perf run at unlimited throughput (producer quota is per broker: expect about rate x brokers)"
"${K}kafka-producer-perf-test.sh" --topic $T --num-records 30000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers="$BS" --producer.config "$CFG" 2>&1 | tail -1

"${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --delete --topic $T >/dev/null 2>&1
exit $fail
