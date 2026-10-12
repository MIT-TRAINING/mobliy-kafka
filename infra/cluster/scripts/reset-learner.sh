#!/usr/bin/env bash
# Delete every topic and consumer group whose name starts with "<id>." (e.g. l07.). Trainer use, run from the trainer VM
# or anywhere admin.properties is available. Usage: reset-learner.sh l07 [ADMIN_PROPERTIES]
set -euo pipefail
ID=${1:?learner id, e.g. l07}
ADMIN=${2:-$(cd "$(dirname "$0")/.." && pwd)/secrets/admin.properties}
K=${KAFKA_BIN:+$KAFKA_BIN/}
BS=$(grep '^bootstrap.servers=' "$ADMIN" | cut -d= -f2)
for t in $("${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$ADMIN" --list | grep "^$ID\."); do
  echo "delete topic $t"; "${K}kafka-topics.sh" --bootstrap-server "$BS" --command-config "$ADMIN" --delete --topic "$t"
done
for g in $("${K}kafka-consumer-groups.sh" --bootstrap-server "$BS" --command-config "$ADMIN" --list | grep "^$ID\."); do
  echo "delete group $g"; "${K}kafka-consumer-groups.sh" --bootstrap-server "$BS" --command-config "$ADMIN" --delete --group "$g" || true
done
