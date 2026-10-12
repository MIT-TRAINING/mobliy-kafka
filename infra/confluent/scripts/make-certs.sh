#!/usr/bin/env bash
# Creates the course CA and one TLS certificate per Confluent Platform host for cp-ansible
# (ssl_custom_certs). Broker certificates also carry cp-kafka.lab.internal, the bootstrap
# name in the learners' cp.properties, so hostname verification passes.
# Output: secrets/certs/{ca.pem,ca-key.pem,<host>.pem,<host>-key.pem}. Re-running keeps the CA.
# Usage:  make-certs.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
D="$HERE/secrets/certs"; mkdir -p "$D"; chmod 700 "$HERE/secrets" "$D"
DAYS=${CERT_DAYS:-120}
HOSTS=(cp-ctl-1 cp-ctl-2 cp-ctl-3 cp-broker-1 cp-broker-2 cp-broker-3 cp-services)

if [[ ! -f "$D/ca.pem" ]]; then
  openssl req -x509 -newkey rsa:4096 -nodes -days $((DAYS + 30)) \
    -subj "/CN=Kafka Course CA" -keyout "$D/ca-key.pem" -out "$D/ca.pem"
fi

for h in "${HOSTS[@]}"; do
  fqdn="$h.lab.internal"
  san="DNS:$fqdn"
  [[ $h == cp-broker-* ]] && san="$san,DNS:cp-kafka.lab.internal"
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$fqdn" \
    -keyout "$D/$fqdn-key.pem" -out "$D/$fqdn.csr"
  openssl x509 -req -in "$D/$fqdn.csr" -CA "$D/ca.pem" -CAkey "$D/ca-key.pem" -CAcreateserial \
    -days "$DAYS" -out "$D/$fqdn.pem" \
    -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$san")
  rm -f "$D/$fqdn.csr"
  echo "ok   $fqdn ($san)"
done
chmod 600 "$D"/*-key.pem
echo "CA for clients: $D/ca.pem (distributed to the VMs as ~/kafka/cp-ca.pem)"
