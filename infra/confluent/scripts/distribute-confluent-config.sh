#!/usr/bin/env bash
# Writes the Module 6-10 client files on every lab VM (same pattern as
# infra/cluster/scripts/distribute-client-config.sh):
#   ~/kafka/cp.properties  (mode 600)  the learner's LDAP user over SASL_SSL/PLAIN
#   ~/kafka/cp-ca.pem                  the course CA
#   ~/kafka/confluent.env              endpoints only, no secrets
# The trainer VM gets the "trainer" LDAP user. Runs from the admin laptop.
# Usage: distribute-confluent-config.sh [id ...]   (default: every VM in infra/terraform output "vms")
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
INFRA="$(cd "$HERE/.." && pwd)"
PEM="$INFRA/secrets/kafka-lab-admin.pem"
PW="$HERE/secrets/cp-ldap-passwords.csv"
CA="$HERE/secrets/certs/ca.pem"
ENVFILE="$HERE/secrets/confluent.env"            # copy of confluent.env.example with your values
[[ -f "$ENVFILE" ]] || ENVFILE="$HERE/confluent.env.example"
CP_BS=$(grep '^export CP=' "$ENVFILE" | cut -d= -f2)
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=8 -o BatchMode=yes)

VMS=$(terraform -chdir="$INFRA/terraform" output -json vms)
ids=("$@"); [[ ${#ids[@]} -eq 0 ]] && ids=($(jq -r 'keys[]' <<<"$VMS"))

fail=0
for id in "${ids[@]}"; do
  ip=$(jq -r --arg id "$id" '.[$id].public_ip' <<<"$VMS")
  pw=$(awk -F, -v u="$id" '$1==u{print $2}' "$PW")
  if [[ -z "$pw" || "$ip" == null ]]; then echo "SKIP $id (no LDAP user or VM)"; continue; fi
  if "${SSH[@]}" learner@"$ip" "umask 077; mkdir -p ~/kafka; cat > ~/kafka/cp.properties" <<P \
     && "${SSH[@]}" learner@"$ip" "umask 022; cat > ~/kafka/cp-ca.pem" < "$CA" \
     && "${SSH[@]}" learner@"$ip" "umask 022; cat > ~/kafka/confluent.env" < "$ENVFILE"
bootstrap.servers=$CP_BS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$id" password="$pw";
ssl.truststore.type=PEM
ssl.truststore.location=/home/learner/kafka/cp-ca.pem
P
  then echo "ok   $id ($ip)"; else echo "FAIL $id ($ip)"; fail=1; fi
done
exit $fail
