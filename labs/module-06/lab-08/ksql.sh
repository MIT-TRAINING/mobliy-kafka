#!/usr/bin/env bash
# Sends ksqlDB statements to the ksqlDB REST API and prints the answer.
#
#   ./ksql.sh "SHOW STREAMS;"                 one statement
#   ./ksql.sh -f sql/01-calls-stream.sql      every statement in a file
#
# ${ME} in a statement or file is replaced with your prefix before sending.
# Environment (Module 6 Lab 08, "Before you start"):
#   KSQL_URL   ksqlDB endpoint (Cloud: https://pksqlc-....confluent.cloud:443, Platform: $KSQL_URL)
#   KSQL_AUTH  user:password (Platform LDAP user) or key:secret (Cloud ksqlDB API key)
#   KSQL_CA    optional CA file (Platform: $CP_CA)
#   ME         your prefix, e.g. l07
# SELECT statements go to /query (push and pull queries), everything else to /ksql.
# Push queries read from the start of the topic (auto.offset.reset=earliest).
set -euo pipefail

if [[ "${1:-}" == -f ]]; then
  stmt=$(cat "${2:?usage: ksql.sh -f <file.sql>}")
else
  stmt=${1:?usage: ksql.sh \"<statement>;\" | ksql.sh -f <file.sql>}
fi
: "${KSQL_URL:?set KSQL_URL}" "${KSQL_AUTH:?set KSQL_AUTH}" "${ME:?set ME (your prefix)}"
stmt=${stmt//'${ME}'/$ME}
# drop -- comment lines so a file can document its statements
stmt=$(grep -v '^[[:space:]]*--' <<<"$stmt")

ca=(); [[ -n "${KSQL_CA:-}" ]] && ca=(--cacert "$KSQL_CA")
body=$(jq -n --arg s "$stmt" '{ksql: $s, streamsProperties: {"ksql.streams.auto.offset.reset": "earliest"}}')
post() {
  curl -sS -N -u "$KSQL_AUTH" ${ca[@]+"${ca[@]}"} -X POST "$KSQL_URL$1" \
    -H 'Content-Type: application/vnd.ksql.v1+json; charset=utf-8' \
    -H 'Accept: application/vnd.ksql.v1+json' -d "$body"
}

if [[ "$stmt" =~ ^[[:space:]]*[Ss][Ee][Ll][Ee][Cc][Tt][[:space:]] ]]; then
  post /query           # streams rows as they arrive; Ctrl+C stops a push query without LIMIT
  echo
else
  # one line per statement: OK <message> / ERROR <message>; listings and descriptions as JSON
  post /ksql | jq -r 'if type == "array" then .[] else . end
    | if .commandStatus then "OK    \(.commandStatus.message)"
      elif .["@type"] == "statement_error" or .error_code then "ERROR \(.message)"
      else . end'
fi
