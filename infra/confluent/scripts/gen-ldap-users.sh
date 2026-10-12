#!/usr/bin/env bash
# Generates the OpenLDAP entries for the shared Confluent Platform cluster:
#   learners l01..l18 + trainer (group "learners" / "trainers"), and the service users
#   cp-ansible needs (mds, kafka_broker, schema_registry, connect_worker, ksql, control_center).
# Passwords are random; hashes go into the LDIF, clear text only into secrets/cp-ldap-passwords.csv.
# Re-running keeps existing passwords.
# Usage:  gen-ldap-users.sh learners.csv        then on cp-ldap:  ldapadd -x -D cn=admin,dc=lab,dc=internal -W -f users.ldif
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
CSV=${1:?usage: gen-ldap-users.sh learners.csv}
S="$HERE/secrets"; mkdir -p "$S"; chmod 700 "$S"
PW="$S/cp-ldap-passwords.csv"; LDIF="$S/users.ldif"
BASE=dc=lab,dc=internal
[[ -f "$PW" ]] || echo "user,password" > "$PW"; chmod 600 "$PW"

learners=($(tail -n +2 "$CSV" | cut -d, -f1 | grep -v '^$'))
services=(mds kafka_broker schema_registry connect_worker ksql control_center)

password_for() {
  local p; p=$(awk -F, -v u="$1" '$1==u{print $2}' "$PW")
  if [[ -z "$p" ]]; then p=$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-16); echo "$1,$p" >> "$PW"; fi
  echo "$p"
}
ssha() { python3 -c 'import sys,os,hashlib,base64; s=os.urandom(8); p=sys.argv[1].encode(); print("{SSHA}"+base64.b64encode(hashlib.sha1(p+s).digest()+s).decode())' "$1"; }

{
  printf 'dn: ou=people,%s\nobjectClass: organizationalUnit\nou: people\n\n' "$BASE"
  printf 'dn: ou=groups,%s\nobjectClass: organizationalUnit\nou: groups\n\n' "$BASE"
  for u in "${learners[@]}" trainer "${services[@]}"; do
    printf 'dn: uid=%s,ou=people,%s\nobjectClass: inetOrgPerson\nuid: %s\ncn: %s\nsn: %s\nuserPassword: %s\n\n' \
      "$u" "$BASE" "$u" "$u" "$u" "$(ssha "$(password_for "$u")")"
  done
  printf 'dn: cn=learners,ou=groups,%s\nobjectClass: groupOfNames\ncn: learners\n' "$BASE"
  for u in "${learners[@]}"; do printf 'member: uid=%s,ou=people,%s\n' "$u" "$BASE"; done
  printf '\ndn: cn=trainers,ou=groups,%s\nobjectClass: groupOfNames\ncn: trainers\nmember: uid=trainer,ou=people,%s\n' "$BASE" "$BASE"
} > "$LDIF"
chmod 600 "$LDIF"
password_for ldap_admin >/dev/null   # cn=admin of the directory; used by cp-ansible/ldap.yml

echo "LDIF:      $LDIF  (${#learners[@]} learners + trainer + ${#services[@]} service users)"
echo "Passwords: $PW"
echo "Export for cp-ansible:"
for v in mds:CP_MDS_PASSWORD kafka_broker:CP_BROKER_LDAP_PASSWORD schema_registry:CP_SR_LDAP_PASSWORD \
         connect_worker:CP_CONNECT_LDAP_PASSWORD ksql:CP_KSQL_LDAP_PASSWORD control_center:CP_C3_LDAP_PASSWORD; do
  echo "  export ${v#*:}=\$(awk -F, '\$1==\"${v%%:*}\"{print \$2}' $PW)"
done
