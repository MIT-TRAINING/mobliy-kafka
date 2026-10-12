#!/usr/bin/env bash
# Creates the per-learner Confluent Cloud resources for Modules 6-10 (infra/LAB-SETUP.md §6):
#   environment env-lNN, Basic cluster lNN-basic, service account sa-lNN-app, invitation e-mail.
# Optionally (TRAINER=1) the trainer's env-trainer with a Standard cluster.
# Safe to re-run: existing resources are found by name and reused.
#
# Prereq: `confluent login --save` as an OrganizationAdmin of the course organization.
# Usage:  cc-provision.sh learners.csv            (CSV columns: id,email)
#         CC_CLOUD=aws CC_REGION=ap-south-1 TRAINER=1 cc-provision.sh learners.csv
# Output: secrets/cc-inventory.csv  (IDs only, no keys or secrets)
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CSV=${1:?usage: cc-provision.sh learners.csv}
CLOUD=${CC_CLOUD:-aws}
REGION=${CC_REGION:-ap-south-1}
OUT="$HERE/secrets/cc-inventory.csv"
mkdir -p "$HERE/secrets"; chmod 700 "$HERE/secrets"

# Read-only calls are retried: a timed-out lookup must never look like "not found",
# or the script would create a duplicate environment or cluster. Creates are not retried.
cf() {
  local i
  for i in 1 2 3 4; do confluent "$@" && return 0; echo "retry $i: confluent $*" >&2; sleep $((i * 5)); done
  return 1
}
one_id() {  # json name -> the single id with that name ("" if none, error if several)
  local ids; ids=$(jq -r --arg n "$2" '.[] | select(.name==$n) | .id' <<<"$1")
  if [[ $(grep -c . <<<"$ids") -gt 1 ]]; then echo "several resources named $2: $ids" >&2; return 1; fi
  echo "$ids"
}
env_id()     { local j; j=$(cf environment list -o json) || return 1; one_id "$j" "$1"; }
cluster_id() { local j; j=$(cf kafka cluster list --environment "$1" -o json) || return 1; one_id "$j" "$2"; }
sa_id()      { local j; j=$(cf iam service-account list -o json) || return 1; one_id "$j" "$1"; }

ensure_env() {  # name -> id
  local id; id=$(env_id "$1") || return 1
  [[ -n "$id" ]] || id=$(confluent environment create "$1" -o json | jq -r .id) || return 1
  [[ "$id" == env-* ]] || return 1
  echo "$id"
}
ensure_cluster() {  # env-id name type -> id
  local id; id=$(cluster_id "$1" "$2") || return 1
  [[ -n "$id" ]] || id=$(confluent kafka cluster create "$2" --cloud "$CLOUD" --region "$REGION" \
                           --type "$3" --environment "$1" -o json | jq -r .id) || return 1
  [[ "$id" == lkc-* ]] || return 1
  echo "$id"
}
ensure_sa() {  # name -> id
  local id; id=$(sa_id "$1") || return 1
  [[ -n "$id" ]] || id=$(confluent iam service-account create "$1" \
                           --description "Module 7 application identity for learner ${1#sa-}" -o json | jq -r .id) || return 1
  [[ "$id" == sa-* ]] || return 1
  echo "$id"
}

tmp=$(mktemp); echo "id,email,env_id,lkc_id,sa_id" > "$tmp"
failed=0
fail() { echo "FAIL $1: $2 (nothing else done for $1; re-run the script)" >&2; failed=1; }

# fd 3 keeps the CSV away from the confluent commands' stdin
while IFS=, read -r id email <&3; do
  [[ -z "$id" || "$id" == id ]] && continue
  eid=$(ensure_env "env-$id")              || { fail "$id" "environment"; continue; }
  lkc=$(ensure_cluster "$eid" "$id-basic" basic) || { fail "$id" "cluster"; continue; }
  sa=$(ensure_sa "sa-$id-app")             || { fail "$id" "service account"; continue; }
  users=$(cf iam user list -o json)        || { fail "$id" "user lookup"; continue; }
  invs=$(cf iam user invitation list -o json) || { fail "$id" "invitation lookup"; continue; }
  if jq -e --arg e "$email" 'any(.[]; .email==$e)' <<<"$users" >/dev/null; then
    state="user exists"
  elif jq -e --arg e "$email" 'any(.[]; .email==$e)' <<<"$invs" >/dev/null; then
    state="invited earlier"
  else
    confluent iam user invitation create "$email" >/dev/null || { fail "$id" "invitation"; continue; }
    state="invited now"
  fi
  printf 'ok   %-4s %-12s %-12s %-12s %s\n' "$id" "$eid" "$lkc" "$sa" "$state"
  echo "$id,$email,$eid,$lkc,$sa" >> "$tmp"
done 3< "$CSV"

if [[ "${TRAINER:-0}" == 1 ]]; then
  if eid=$(ensure_env env-trainer) && lkc=$(ensure_cluster "$eid" trainer-standard standard); then
    printf 'ok   %-4s %-12s %-12s\n' trainer "$eid" "$lkc"
    echo "trainer,,$eid,$lkc," >> "$tmp"
  else
    fail trainer "environment or cluster"
  fi
fi

# A partial inventory would make cc-bind-roles.sh skip learners, so keep the old one on failure
if [[ $failed == 1 ]]; then rm -f "$tmp"; echo "Some learners failed; inventory not updated. Re-run when the network is stable." >&2; exit 1; fi
mv "$tmp" "$OUT"; chmod 600 "$OUT"
echo "Inventory: $OUT"
echo "Next: wait until learners accept the invitations, then run cc-bind-roles.sh $OUT"
