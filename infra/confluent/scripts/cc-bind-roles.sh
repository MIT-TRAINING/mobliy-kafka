#!/usr/bin/env bash
# Grants every learner EnvironmentAdmin on their own environment (infra/LAB-SETUP.md §6).
# Users get an ID only after they accept the invitation, so run this after cc-provision.sh
# and again on day 1 for late acceptors. Safe to re-run.
#
# Prereq: `confluent login --save` as an OrganizationAdmin.
# Usage:  cc-bind-roles.sh secrets/cc-inventory.csv
set -euo pipefail
INV=${1:?usage: cc-bind-roles.sh secrets/cc-inventory.csv}
USERS=$(confluent iam user list -o json)
pending=0

while IFS=, read -r id email eid lkc sa <&3; do
  [[ "$id" == id || "$id" == trainer || -z "$email" ]] && continue
  uid=$(jq -r --arg e "$email" '.[] | select(.email==$e) | .id' <<<"$USERS")
  if [[ -z "$uid" ]]; then echo "WAIT $id ($email has not accepted the invitation)"; pending=1; continue; fi
  if confluent iam rbac role-binding list --principal "User:$uid" --role EnvironmentAdmin \
       --environment "$eid" -o json | jq -e 'length > 0' >/dev/null; then
    echo "ok   $id $uid EnvironmentAdmin on $eid (already bound)"
  else
    confluent iam rbac role-binding create --principal "User:$uid" --role EnvironmentAdmin \
      --environment "$eid" >/dev/null
    echo "ok   $id $uid EnvironmentAdmin on $eid (bound now)"
  fi
done 3< "$INV"

exit $pending
