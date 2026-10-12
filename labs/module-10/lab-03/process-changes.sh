#!/usr/bin/env bash
# A small "subscriber-events" service: consumes the CDC topic, turns each database change into a
# business event and produces it to a second topic. Stop it with Ctrl-C; it resumes where it stopped,
# because it reads as consumer group $ME.cdc-processor.
#
#   ./process-changes.sh <bootstrap> <client-config>
#
# Input   $ME.cdc.crm.subscribers     Debezium change events (key = {"msisdn":…}, value = envelope or null)
# Output  $ME.cdc.subscriber-events   key = msisdn, value = {"event":…, "msisdn":…, …}
#
# op "r" (snapshot rows) are the starting state, not changes: they are counted but produce no event.
# A null value is the tombstone that follows a delete: it is skipped.
set -euo pipefail

bs=${1:?usage: process-changes.sh <bootstrap> <client-config>}
cfg=${2:?usage: process-changes.sh <bootstrap> <client-config>}
: "${ME:?set ME (your prefix)}"

# One input line is  <key JSON>|<value JSON or null>.  One output line is  <msisdn>|<event JSON>.
read -r -d '' TO_EVENT <<'JQ' || true
(index("|")) as $i
| (.[:$i] | fromjson | .msisdn) as $msisdn
| .[$i+1:] as $raw
| if $raw == "null" then empty else ($raw | fromjson) as $v
  | ($v.after // $v.before) as $row
  | { msisdn: $msisdn, changedAt: ($v.source.ts_ms / 1000 | floor | todate) } as $base
  | if   $v.op == "c" then $base + { event: "NEW_SUBSCRIBER", plan: $row.plan, circle: $row.circle, action: "send welcome SMS" }
    elif $v.op == "d" then $base + { event: "SUBSCRIBER_REMOVED", lastPlan: $v.before.plan, action: "close billing account" }
    elif $v.op == "u" and $v.before.plan   != $v.after.plan   then $base + { event: "PLAN_CHANGED",   from: $v.before.plan,   to: $v.after.plan,   action: "re-rate from today" }
    elif $v.op == "u" and $v.before.status != $v.after.status then $base + { event: "STATUS_CHANGED", from: $v.before.status, to: $v.after.status, action: (if $v.after.status == "SUSPENDED" then "bar outgoing calls" else "restore service" end) }
    elif $v.op == "u" then $base + { event: "PROFILE_UPDATED" }
    else empty end
  | "\($msisdn)|\(tojson)"
  end
JQ

echo "processing $ME.cdc.crm.subscribers -> $ME.cdc.subscriber-events (Ctrl-C to stop)" >&2
kafka-console-consumer.sh --bootstrap-server "$bs" --command-config "$cfg" \
    --topic "$ME.cdc.crm.subscribers" --group "$ME.cdc-processor" --from-beginning \
    --formatter-property print.key=true --formatter-property key.separator='|' \
  | jq -R -r --unbuffered "$TO_EVENT" \
  | tee /dev/stderr \
  | kafka-console-producer.sh --bootstrap-server "$bs" --command-config "$cfg" \
      --topic "$ME.cdc.subscriber-events" \
      --reader-property parse.key=true --reader-property key.separator='|'
