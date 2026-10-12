#!/usr/bin/env bash
# Deploys Kafka UI (provectus/kafka-ui) on the trainer VM and publishes it at https://kafka-ui.<dns_zone>
# through the VM's Caddy. Every user in secrets/users.csv logs in (HTTP basic auth) with their Kafka
# username and password and gets a Kafka UI "cluster" named after them that connects as that SCRAM user,
# so the broker ACLs decide what they can see and do. Caddy stops a learner from opening any other
# user's cluster; trainer (super user) can open all of them. Idempotent; re-run after users.csv changes.
# Runs from the admin laptop (SSH is open to the admin IP only).
# Needs DNS: kafka-ui.<dns_zone> A -> trainer Elastic IP (Route 53 public zone).
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"; INFRA="$(cd "$HERE/.." && pwd)"
PEM="$INFRA/secrets/kafka-lab-admin.pem"
USERS="$HERE/secrets/users.csv"
FQDN="${FQDN:-kafka-ui.kafka.supercloudlabs.com}"
BS=$(terraform -chdir="$HERE/terraform" output -raw bootstrap)
IP=$(terraform -chdir="$INFRA/terraform" output -json vms | jq -r '.trainer.public_ip')
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=8 -o BatchMode=yes "ubuntu@$IP")

render_config() {   # one Kafka UI cluster per Kafka user, each with that user's own SCRAM credentials
  echo "auth:"
  echo "  type: DISABLED   # Caddy authenticates; see /etc/caddy/kafka-ui.caddy"
  echo "kafka:"
  echo "  clusters:"
  while IFS=, read -r u p || [[ -n "$u" ]]; do
    [[ -z "$u" ]] && continue
    cat <<C
    - name: $u
      bootstrapServers: $BS
      properties:
        "[security.protocol]": SASL_PLAINTEXT
        "[sasl.mechanism]": SCRAM-SHA-512
        "[sasl.jaas.config]": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="$u" password="$p";'
C
  done < "$USERS"
}

echo "== kafka-ui container on trainer VM ($IP)"
render_config | "${SSH[@]}" 'sudo install -d -m 700 /opt/kafka-ui && sudo tee /opt/kafka-ui/config.yml >/dev/null && sudo chmod 644 /opt/kafka-ui/config.yml'
"${SSH[@]}" 'sudo tee /opt/kafka-ui/docker-compose.yml >/dev/null' < "$HERE/kafka-ui/docker-compose.yml"
"${SSH[@]}" 'sudo docker compose --project-directory /opt/kafka-ui -p shared-kafka-ui up -d --force-recreate --wait --quiet-pull'

# Caddy would keep failing Let's Encrypt validation until the name points at the trainer VM
NS=$(dig +short NS "${FQDN#*.}" | head -1)   # ask Route 53 directly: resolvers may cache an earlier NXDOMAIN
if [[ "$(dig +short @"$NS" "$FQDN" A | tail -1)" != "$IP" ]]; then
  echo "!! $FQDN does not resolve to $IP yet: create the Route 53 A record, then re-run this script"; exit 1
fi

echo "== Caddy site $FQDN (basic auth + per-user cluster restriction)"
# Rendered on the VM so the passwords are bcrypt-hashed with its own caddy binary; users.csv goes over stdin.
"${SSH[@]}" 'sudo tee /opt/kafka-ui/render-caddy.sh >/dev/null' < "$HERE/kafka-ui/render-caddy.sh"
"${SSH[@]}" "sudo bash /opt/kafka-ui/render-caddy.sh '$FQDN'" < "$USERS"

echo "== done: https://$FQDN  (log in with the Kafka username/password from secrets/users.csv)"
