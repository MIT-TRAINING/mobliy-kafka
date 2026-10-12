#!/usr/bin/env bash
# Prints the Debezium PostgreSQL source connector as JSON, the body for  POST localhost:8083/connectors
#
#   ./cdc-connector.sh cloud      replication slot cdc_cloud
#   ./cdc-connector.sh platform   replication slot cdc_platform
#
# The two outputs differ only in the slot name: each target keeps its own position in the database log.
# The file holds no secret: the worker resolves ${env:CDC_DB_PASSWORD} when it starts the task.
# Environment: ME (required), NAME (default $ME.cdc-crm).
set -euo pipefail

target=${1:-}
[[ $target == cloud || $target == platform ]] || { echo "usage: cdc-connector.sh cloud|platform" >&2; exit 2; }
: "${ME:?set ME (your prefix)}"

jq -n --arg name "${NAME:-$ME.cdc-crm}" --arg prefix "$ME.cdc" --arg slot "cdc_$target" '{
  "name": $name,
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.dbname": "crm",
    "database.user": "cdc_reader",
    "database.password": "${env:CDC_DB_PASSWORD}",
    "plugin.name": "pgoutput",
    "publication.name": "cdc_subscribers",
    "publication.autocreate.mode": "disabled",
    "slot.name": $slot,
    "table.include.list": "crm.subscribers",
    "topic.prefix": $prefix,
    "snapshot.mode": "initial"
  }
}'
