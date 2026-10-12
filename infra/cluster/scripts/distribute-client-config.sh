#!/usr/bin/env bash
# Writes ~/kafka/apache.properties (mode 600) on every lab VM, with that learner's own SCRAM user.
# The trainer VM gets the trainer (super user) credentials. Runs from the admin laptop (SSH is open to the admin IP only).
# Usage: distribute-client-config.sh [id ...]      (default: every VM in infra/terraform output "vms")
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"; INFRA="$(cd "$HERE/.." && pwd)"
PEM="$INFRA/secrets/kafka-lab-admin.pem"
USERS="$HERE/secrets/users.csv"
BS=$(terraform -chdir="$HERE/terraform" output -raw bootstrap)
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=8 -o BatchMode=yes)

VMS=$(terraform -chdir="$INFRA/terraform" output -json vms)
ids=("$@"); [[ ${#ids[@]} -eq 0 ]] && ids=($(jq -r 'keys[]' <<<"$VMS"))

fail=0
for id in "${ids[@]}"; do
  ip=$(jq -r --arg id "$id" '.[$id].public_ip' <<<"$VMS")
  pw=$(awk -F, -v u="$id" '$1==u{print $2}' "$USERS")
  if [[ -z "$pw" || "$ip" == null ]]; then echo "SKIP $id (no kafka user or VM)"; continue; fi
  if "${SSH[@]}" learner@"$ip" "umask 077; mkdir -p ~/kafka; cat > ~/kafka/apache.properties" <<P
bootstrap.servers=$BS
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$id" password="$pw";
P
  then echo "ok   $id ($ip)"; else echo "FAIL $id ($ip)"; fail=1; fi
done
exit $fail
