#!/usr/bin/env bash
# Start / stop / reset the 3-node KRaft cluster this case study runs on.
#
# It is the Module 2 cluster (labs/module-02/docker-compose.yml) plus the
# Module 3 override (labs/module-03/docker-compose.override.yml), which lowers
# log.retention.check.interval.ms from 5 minutes to 15 seconds so retention is
# visible during a class. Same project name (kafka-m2) as the labs, so this
# reuses their containers and volumes.
#
#   ./scripts/cluster.sh up       start (or keep running) and wait until healthy
#   ./scripts/cluster.sh status
#   ./scripts/cluster.sh down     stop, keep data
#   ./scripts/cluster.sh reset    stop and DELETE all topics and data
#   ./scripts/cluster.sh cli      open a shell in kafka-1 with the Kafka CLI tools ($BS is set)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABS="$(cd "$HERE/../../../labs" && pwd)"
COMPOSE=(docker compose -p kafka-m2
  -f "$LABS/module-02/docker-compose.yml"
  -f "$LABS/module-03/docker-compose.override.yml")

case "${1:-up}" in
  up)
    "${COMPOSE[@]}" up -d
    echo "Waiting for the three brokers to become healthy..."
    for _ in $(seq 1 40); do
      healthy=$(docker ps --filter "name=kafka-" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
      [ "$healthy" -ge 3 ] && break
      sleep 3
    done
    "${COMPOSE[@]}" ps
    ;;
  status) "${COMPOSE[@]}" ps ;;
  down)   "${COMPOSE[@]}" down ;;
  reset)  "${COMPOSE[@]}" down -v ;;
  cli)    docker exec -it kafka-1 bash ;;
  *) echo "usage: $0 {up|status|down|reset|cli}" >&2; exit 1 ;;
esac
