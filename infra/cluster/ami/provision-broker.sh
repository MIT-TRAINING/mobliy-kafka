#!/usr/bin/env bash
# Runs as root on the builder (Ubuntu 24.04). Installs what a Kafka controller/broker node needs.
# Per-node configuration is NOT baked in: terraform/node-boot.sh.tftpl writes it at first boot.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

KAFKA_VERSION=4.3.1          # keep in sync with infra/ami/provision.sh and the Docker labs
SCALA_VERSION=2.13
JMX_EXPORTER_VERSION=1.0.1
NODE_EXPORTER_VERSION=1.8.2
CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")

apt-get update
apt-get -y upgrade
apt-get install -y ca-certificates curl gnupg jq unzip net-tools dnsutils htop chrony

# Temurin 21 (same JDK the lab VMs use)
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg
echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb ${CODENAME} main" \
  > /etc/apt/sources.list.d/adoptium.list
apt-get update
apt-get install -y temurin-21-jre

# Kafka
id kafka >/dev/null 2>&1 || useradd --system --home-dir /var/lib/kafka --shell /usr/sbin/nologin kafka
install -d -o kafka -g kafka -m 0750 /var/lib/kafka /var/lib/kafka/data
install -d -o kafka -g kafka -m 0750 /var/log/kafka    # LOG_DIR: the kafka user cannot write under /opt/kafka
cd /opt
curl -fsSLO "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ln -sfn "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka
# CLI on PATH for admins on the node (wrapper scripts: kafka-run-class.sh resolves its classpath from its own path)
for f in /opt/kafka/bin/*.sh; do
  printf '#!/bin/sh\nexec %s "$@"\n' "$f" > "/usr/local/bin/$(basename "$f")"
  chmod 755 "/usr/local/bin/$(basename "$f")"
done

# Prometheus JMX exporter (Java agent, port 7071)
install -d /opt/jmx
curl -fsSL -o /opt/jmx/jmx_prometheus_javaagent.jar \
  "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"
cat > /opt/jmx/kafka.yml <<'YML'
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  # per-topic / per-partition server metrics
  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.*)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels: { clientId: "$3", topic: "$4", partition: "$5" }
  - pattern: kafka.server<type=(.+), name=(.+), topic=(.+)><>(Count|OneMinuteRate|Value)
    name: kafka_server_$1_$2_$4
    type: GAUGE
    labels: { topic: "$3" }
  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels: { clientId: "$3" }
  - pattern: kafka.server<type=(.+), name=(.+)><>(Count|OneMinuteRate|Value)
    name: kafka_server_$1_$2_$3
    type: GAUGE
  # request latency and rates
  - pattern: kafka.network<type=RequestMetrics, name=(.+), request=(.+)><>(Count|Mean|99thPercentile)
    name: kafka_network_requestmetrics_$1_$3
    type: GAUGE
    labels: { request: "$2" }
  - pattern: kafka.network<type=(.+), name=(.+)><>(Count|Value|OneMinuteRate)
    name: kafka_network_$1_$2_$3
    type: GAUGE
  # replication, controller, raft, log
  - pattern: kafka.controller<type=(.+), name=(.+)><>(Count|Value|OneMinuteRate)
    name: kafka_controller_$1_$2_$3
    type: GAUGE
  - pattern: kafka.log<type=Log, name=(.+), topic=(.+), partition=(.*)><>Value
    name: kafka_log_log_$1
    type: GAUGE
    labels: { topic: "$2", partition: "$3" }
  - pattern: kafka.log<type=(.+), name=(.+)><>(Count|Value)
    name: kafka_log_$1_$2_$3
    type: GAUGE
  - pattern: kafka.server<type=raft-metrics><>(.+)
    name: kafka_raft_$1
    type: GAUGE
  # JVM
  - pattern: 'java.lang<type=(.+)><(.+)>(.+): (.*)'
    name: jvm_$1_$3
    type: GAUGE
YML

# node_exporter (port 9100)
cd /tmp
curl -fsSLO "https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
tar -xzf "node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz"
install -m 0755 "node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter" /usr/local/bin/node_exporter
rm -rf node_exporter-*
cat > /etc/systemd/system/node_exporter.service <<'UNIT'
[Unit]
Description=Prometheus node_exporter
After=network-online.target

[Service]
User=nobody
ExecStart=/usr/local/bin/node_exporter
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT

# Kafka service. NOT enabled here: node-boot.sh.tftpl enables it after the first-boot configuration and format.
cat > /etc/systemd/system/kafka.service <<'UNIT'
[Unit]
Description=Apache Kafka (KRaft)
After=network-online.target
ConditionPathExists=/etc/kafka/server.properties

[Service]
User=kafka
Group=kafka
EnvironmentFile=-/etc/kafka/kafka.env
Environment=KAFKA_OPTS=-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/kafka.yml
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl disable kafka || true

cat > /etc/sysctl.d/99-kafka.conf <<'SYSCTL'
vm.swappiness=1
vm.max_map_count=262144
SYSCTL

# --- verify: fail the bake here, not on 7 nodes later ---
java -version
test -s /opt/jmx/jmx_prometheus_javaagent.jar
/opt/kafka/bin/kafka-storage.sh --help >/dev/null
node_exporter --version
systemd-analyze verify /etc/systemd/system/kafka.service || true
echo "provision-broker.sh OK"
