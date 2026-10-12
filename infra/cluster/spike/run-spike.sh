#!/usr/bin/env bash
# Phase 0 spike: 3 controllers + 3 brokers in Docker, same config templates as the AWS nodes.
# Usage: run-spike.sh up | test | down
set -euo pipefail
cd "$(dirname "$0")"
IMG=apache/kafka:4.3.1
WORK=$PWD/work
NET=kafka-spike
QV="1@ctl-1:9093,2@ctl-2:9093,3@ctl-3:9093"
BROKER_PW=brokerpw-spike
ADMIN_PW=adminpw-spike
RENDER=../scripts/render-config.py

up() {
  rm -rf "$WORK"; mkdir -p "$WORK/conf" "$WORK/data"
  chmod 777 "$WORK/data"
  CID=$(docker run --rm $IMG /opt/kafka/bin/kafka-storage.sh random-uuid)
  echo "cluster id $CID"
  docker network create $NET >/dev/null 2>&1 || true
  racks=(a b c)
  for n in 1 2 3; do
    $RENDER ../config/controller.properties.tmpl node_id=$n quorum_voters=$QV > "$WORK/conf/ctl-$n.properties"
    $RENDER ../config/broker.properties.tmpl node_id=$((10+n)) rack=${racks[$((n-1))]} quorum_voters=$QV \
        hostname=broker-$((10+n)) broker_password=$BROKER_PW > "$WORK/conf/broker-$((10+n)).properties"
  done
  cat > "$WORK/conf/admin.properties" <<P
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="$ADMIN_PW";
P
  for n in 1 2 3; do start_node ctl-$n "--add-scram SCRAM-SHA-512=[name=admin,password=$ADMIN_PW] --add-scram SCRAM-SHA-512=[name=broker,password=$BROKER_PW]"; done
  for n in 11 12 13; do start_node broker-$n ""; done
  docker run -d --name spike-client --network $NET --network-alias apache-kafka -v "$WORK/conf:/conf" -v "$PWD/../scripts:/scripts" \
     --entrypoint sleep $IMG infinity >/dev/null
  echo "$CID" > "$WORK/cluster-id"
}

start_node() {  # name, extra storage-format args
  local name=$1 extra=$2
  docker run -d --name spike-$name --network $NET --network-alias $name -v "$WORK/conf:/conf" \
    --entrypoint bash $IMG -c "
      set -e
      # format only once: a restart must keep the existing data and meta.properties
      [ -f /var/lib/kafka/data/meta.properties ] || /opt/kafka/bin/kafka-storage.sh format -t $(cat "$WORK/cluster-id" 2>/dev/null || echo "$CID") -c /conf/$name.properties $extra
      exec /opt/kafka/bin/kafka-server-start.sh /conf/$name.properties" >/dev/null
}

test_() {
  local c="docker exec spike-client"
  export KAFKA_BIN=/opt/kafka/bin
  echo "== wait for brokers"
  for i in $(seq 1 30); do
    $c /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller ctl-1:9093 describe --status 2>/dev/null | grep -q LeaderId && break; sleep 2
  done
  $c /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller ctl-1:9093 describe --status | egrep "LeaderId|CurrentVoters|CurrentObservers"
  BS=broker-11:9092
  for i in $(seq 1 30); do
    $c /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server $BS --command-config /conf/admin.properties >/dev/null 2>&1 && break; sleep 2
  done
  printf 'trainer,trainerpw\nl01,pw01\nl02,pw02\n' > "$WORK/conf/users.csv"
  $c env KAFKA_BIN=/opt/kafka/bin /scripts/bootstrap-security.sh /conf/admin.properties /conf/users.csv $BS
  for u in "l01 pw01" "l02 pw02"; do
    set -- $u
    echo "== check $1"
    $c env KAFKA_BIN=/opt/kafka/bin /scripts/check-cluster.sh $1 $2 $BS || true
  done
}

down() { docker rm -f $(docker ps -aq --filter name=spike-) 2>/dev/null || true; docker network rm $NET 2>/dev/null || true; rm -rf "$WORK"; }

case ${1:-} in up) up;; test) test_;; down) down;; *) echo "usage: $0 up|test|down"; exit 1;; esac
