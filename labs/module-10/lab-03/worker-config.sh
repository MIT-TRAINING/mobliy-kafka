#!/usr/bin/env bash
# Writes the "where is Kafka and who am I" part of the Connect worker's configuration.
#
#   ./worker-config.sh cloud      from ~/kafka/sa-app.properties (service account sa-lNN-app)
#   ./worker-config.sh platform   from ~/kafka/cp.properties     (your LDAP user) + the course CA
#
# Output, both in $CDC_DIR (Module 10 Lab 03, "Before you start"):
#   worker.env   CONNECT_* variables for docker compose (env_file). Holds a secret: mode 600, never printed.
#   ca.pem       the CA the worker trusts: a copy of cp-ca.pem for Platform, an empty file for Cloud
#                (Cloud certificates chain to a public CA that Java already trusts).
# Each setting is written three times: for the worker itself, and with the PRODUCER_ and CONSUMER_
# prefixes, because source tasks produce (and sink tasks consume) with their own clients.
# Set CLIENT_CFG to use another client file.
set -euo pipefail

target=${1:-}
case "$target" in
  cloud)    src=${CLIENT_CFG:-$HOME/kafka/sa-app.properties} ;;
  platform) src=${CLIENT_CFG:-$HOME/kafka/cp.properties} ;;
  *) echo "usage: worker-config.sh cloud|platform" >&2; exit 2 ;;
esac
: "${CDC_DIR:?export CDC_DIR (Before you start)}"
[[ -r $src ]] || { echo "cannot read $src" >&2; exit 1; }

umask 077
mkdir -p "$CDC_DIR"

ca=$(sed -n 's/^ssl.truststore.location=//p' "$src")
if [[ -n $ca ]]; then
  cp "$ca" "$CDC_DIR/ca.pem"
else
  : > "$CDC_DIR/ca.pem"
fi
chmod 644 "$CDC_DIR/ca.pem"          # a CA certificate is public; the container must be able to read it

{
  echo "# Connect worker connection for '$target', generated from $src. Contains a secret."
  while IFS= read -r line; do
    key=${line%%=*}; value=${line#*=}
    case "$key" in
      bootstrap.servers|security.protocol|sasl.mechanism|sasl.jaas.config|ssl.truststore.type) ;;
      ssl.truststore.location) value=/etc/kafka-connect/ca.pem ;;
      *) continue ;;
    esac
    var=$(tr 'a-z.' 'A-Z_' <<< "$key")
    echo "CONNECT_$var=$value"
    [[ $key == bootstrap.servers ]] && continue
    echo "CONNECT_PRODUCER_$var=$value"
    echo "CONNECT_CONSUMER_$var=$value"
  done < "$src"
} > "$CDC_DIR/worker.env"

grep -q '^CONNECT_SASL_JAAS_CONFIG=' "$CDC_DIR/worker.env" || { echo "no sasl.jaas.config in $src" >&2; exit 1; }
echo "wrote $CDC_DIR/worker.env for $target ($(grep -c '^CONNECT_' "$CDC_DIR/worker.env") settings), secrets hidden:"
sed -E "s/password=[^;]*/password=<hidden>/" "$CDC_DIR/worker.env" | grep -E '^CONNECT_(BOOTSTRAP|SECURITY|SASL|SSL)_'
