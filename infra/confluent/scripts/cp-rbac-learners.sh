#!/usr/bin/env bash
# Role bindings and ACLs for the shared Confluent Platform cluster (Modules 6-10):
#   learner lNN : ResourceOwner on Topic:lNN. and Group:lNN. (prefixed), SystemAdmin on the Kafka cluster
#                 (except learners listed in RESOURCE_OWNER_ONLY),
#                 ACL Describe/DescribeConfigs on the cluster,
#                 ResourceOwner on Subject:lNN. in Schema Registry (Module 6 Lab 06),
#                 DeveloperWrite on the ksqlDB cluster (Module 6 Lab 08)
#   ksql        : ResourceOwner on Topic:lNN. and Subject:lNN., because persistent queries
#                 read and write the learners' topics as the ksqlDB service principal
#   trainer     : SystemAdmin on the Kafka cluster
# Mirrors the prefix rules of the shared Apache cluster (infra/LAB-SETUP.md §4). Safe to re-run.
#
# Prereq: confluent login --url $MDS_URL --certificate-authority-path secrets/certs/ca.pem  (as "mds")
#         secrets/mds-admin.properties: SASL_SSL/PLAIN client config for user mds (for kafka-acls.sh)
# Usage:  cp-rbac-learners.sh learners.csv
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CSV=${1:?usage: cp-rbac-learners.sh learners.csv}
MDS_URL=${MDS_URL:-https://cp-kafka.lab.internal:8090}
CA="$HERE/secrets/certs/ca.pem"
ADMIN_CFG="$HERE/secrets/mds-admin.properties"
BS=${CP:-cp-kafka.lab.internal:9092}
# Component cluster IDs as registered by cp-ansible (check: confluent cluster list --url $MDS_URL ...)
SR_CLUSTER=${SR_CLUSTER_ID:-schema-registry}
KSQL_CLUSTER=${KSQL_CLUSTER_ID:-default_}
# Learners who get only their prefixed ResourceOwner bindings, no SystemAdmin (space-separated)
RESOURCE_OWNER_ONLY=${RESOURCE_OWNER_ONLY:-}

CP_ID=$(confluent cluster describe --url "$MDS_URL" --certificate-authority-path "$CA" -o json \
        | jq -r '.scope[] | select(.type=="kafka-cluster") | .id')
[[ -n "$CP_ID" ]] || { echo "Could not read the Kafka cluster ID from MDS"; exit 1; }
echo "Kafka cluster ID: $CP_ID"

bind() {  # principal role [resource]
  local args=(--principal "$1" --role "$2" --kafka-cluster "$CP_ID")
  [[ -n "${3:-}" ]] && args+=(--resource "$3" --prefix)
  confluent iam rbac role-binding create "${args[@]}" >/dev/null 2>&1 \
    && echo "ok   $1 $2 ${3:-cluster}" || echo "skip $1 $2 ${3:-cluster} (exists or failed: re-run with -vv)"
}

# like bind, for a resource in Schema Registry or ksqlDB: principal role resource prefix(yes|no) scope-flags...
bind_in() {
  local args=(--principal "$1" --role "$2" --resource "$3" --kafka-cluster "$CP_ID" "${@:5}")
  [[ "$4" == yes ]] && args+=(--prefix)
  confluent iam rbac role-binding create "${args[@]}" >/dev/null 2>&1 \
    && echo "ok   $1 $2 $3 ${*:5}" || echo "skip $1 $2 $3 ${*:5} (exists or failed: re-run with -vv)"
}

bind User:trainer SystemAdmin
for scope in "--schema-registry-cluster $SR_CLUSTER" "--ksql-cluster $KSQL_CLUSTER"; do
  confluent iam rbac role-binding create --principal User:trainer --role SystemAdmin \
    --kafka-cluster "$CP_ID" $scope >/dev/null 2>&1 \
    && echo "ok   User:trainer SystemAdmin $scope" || echo "skip User:trainer SystemAdmin $scope"
done

# Confluent Server starts with a cluster-wide dynamic default min.insync.replicas=1, which
# overrides the static 2 from hosts.yml. Set the dynamic default too (same as the Apache cluster).
kafka-configs.sh --bootstrap-server "$BS" --command-config "$ADMIN_CFG" --entity-type brokers --entity-default \
  --alter --add-config min.insync.replicas=2 >/dev/null
echo "ok   cluster default min.insync.replicas=2"

while IFS=, read -r id _ <&3; do
  [[ -z "$id" || "$id" == id ]] && continue
  bind "User:$id" ResourceOwner "Topic:$id."
  bind "User:$id" ResourceOwner "Group:$id."
  # Full control of the Kafka cluster (brokers, all topics and groups, role bindings, Control Center).
  # This overrides the prefix isolation above for Kafka resources; it does not cover Schema Registry,
  # Connect or ksqlDB, which keep the bindings below. For monitoring only, bind Operator instead.
  [[ " $RESOURCE_OWNER_ONLY " == *" $id "* ]] || bind "User:$id" SystemAdmin
  kafka-acls.sh --bootstrap-server "$BS" --command-config "$ADMIN_CFG" --add \
    --allow-principal "User:$id" --operation Describe --operation DescribeConfigs --cluster >/dev/null
  echo "ok   User:$id ACL Describe,DescribeConfigs on cluster"
  # Schema Registry: register and read schemas for their own topics (<topic>-value subjects)
  bind_in "User:$id" ResourceOwner "Subject:$id." yes --schema-registry-cluster "$SR_CLUSTER"
  # ksqlDB: submit statements and queries to the shared ksqlDB server
  bind_in "User:$id" DeveloperWrite "KsqlCluster:ksql-cluster" no --ksql-cluster "$KSQL_CLUSTER"
  # ...whose persistent queries run as User:ksql on the learner's topics and subjects
  bind "User:ksql" ResourceOwner "Topic:$id."
  bind_in "User:ksql" ResourceOwner "Subject:$id." yes --schema-registry-cluster "$SR_CLUSTER"
done 3< "$CSV"

echo "Check one learner:  confluent iam rbac role-binding list --principal User:l01 --kafka-cluster $CP_ID"
