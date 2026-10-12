#!/usr/bin/env bash
# Deletes everything cc-provision.sh created: clusters, environments and service accounts
# listed in the inventory. Deleting a cluster deletes its topics, API keys stay orphaned
# until their owner is removed, so learner users are removed last (optional, REMOVE_USERS=1).
# Irreversible. Asks for confirmation unless FORCE=1.
#
# Prereq: `confluent login --save` as an OrganizationAdmin.
# Usage:  cc-teardown.sh secrets/cc-inventory.csv
set -euo pipefail
INV=${1:?usage: cc-teardown.sh secrets/cc-inventory.csv}

confluent organization describe
echo "This deletes every cluster, environment and service account in $INV."
if [[ "${FORCE:-0}" != 1 ]]; then
  read -rp 'Type DELETE to continue: ' answer
  [[ "$answer" == DELETE ]] || { echo "Aborted."; exit 1; }
fi

USERS=$(confluent iam user list -o json)
while IFS=, read -r id email eid lkc sa <&3; do
  [[ "$id" == id ]] && continue
  [[ -n "$lkc" ]] && confluent kafka cluster delete "$lkc" --environment "$eid" --force || true
  [[ -n "$eid" ]] && confluent environment delete "$eid" --force || true
  [[ -n "$sa"  ]] && confluent iam service-account delete "$sa" --force || true
  if [[ "${REMOVE_USERS:-0}" == 1 && -n "$email" ]]; then
    uid=$(jq -r --arg e "$email" '.[] | select(.email==$e) | .id' <<<"$USERS")
    [[ -n "$uid" ]] && confluent iam user delete "$uid" --force || true
  fi
  echo "done $id"
done 3< "$INV"

echo "Check the Billing page: no clusters or connectors may remain."
