#!/usr/bin/env bash
# Prints the configuration of a Datagen Source connector as JSON.
#
#   ./datagen-config.sh cloud      body for  confluent connect cluster create --config-file
#   ./datagen-config.sh platform   body for  POST $CONNECT_URL/connectors
#
# Both targets generate the same call records from cdr-datagen.avsc, so you can
# compare the two configurations line by line.
# Environment (Module 6 Lab 07, "Before you start"):
#   ME           your prefix, e.g. l07 (required)
#   NAME         connector name   (default: $ME.datagen-cdr)
#   TOPIC        target topic     (default: $ME.cdr.datagen)
#   INTERVAL_MS  maximum pause between records in ms (default: 1000)
#   CC_CFG       cloud only: your Kafka client file with the API key (default: ~/kafka/ccloud.properties)
# The cloud output CONTAINS YOUR API SECRET: write it to a file with mode 600 and delete it afterwards.
set -euo pipefail

target=${1:?usage: datagen-config.sh cloud|platform}
: "${ME:?set ME (your prefix)}"
here=$(cd "$(dirname "$0")" && pwd)
name=${NAME:-$ME.datagen-cdr}
topic=${TOPIC:-$ME.cdr.datagen}
interval=${INTERVAL_MS:-1000}
schema=$(jq -c . "$here/cdr-datagen.avsc")

case "$target" in
  cloud)
    cfg=${CC_CFG:-$HOME/kafka/ccloud.properties}
    key=$(sed -n 's/.*username="\([^"]*\)".*/\1/p' "$cfg")
    secret=$(sed -n 's/.*password="\([^"]*\)".*/\1/p' "$cfg")
    [[ -n "$key" && -n "$secret" ]] || { echo "no API key found in $cfg" >&2; exit 1; }
    jq -n --arg name "$name" --arg topic "$topic" --arg schema "$schema" --arg interval "$interval" \
          --arg key "$key" --arg secret "$secret" '{
      "name": $name,
      "connector.class": "DatagenSource",
      "kafka.auth.mode": "KAFKA_API_KEY",
      "kafka.api.key": $key,
      "kafka.api.secret": $secret,
      "kafka.topic": $topic,
      "output.data.format": "JSON",
      "schema.string": $schema,
      "max.interval": $interval,
      "tasks.max": "1"
    }'
    ;;
  platform)
    jq -n --arg name "$name" --arg topic "$topic" --arg schema "$schema" --arg interval "$interval" '{
      "name": $name,
      "config": {
        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
        "kafka.topic": $topic,
        "schema.string": $schema,
        "max.interval": $interval,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
      }
    }'
    ;;
  *) echo "usage: datagen-config.sh cloud|platform" >&2; exit 2 ;;
esac
