#!/usr/bin/env bash
# Start, stop or show the 8 Confluent Platform hosts in the right order.
#   start: LDAP, controllers, brokers, then services (Schema Registry, Connect, ksqlDB, Control Center).
#   stop:  services, brokers, controllers, then LDAP.
# Private IPs, EBS data, DNS and certificates survive stop/start. The trial licence keeps counting while stopped.
set -euo pipefail
export AWS_PROFILE="${AWS_PROFILE:-kafka-training}" AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
ids() { aws ec2 describe-instances --filters Name=tag:Stack,Values=confluent-platform "Name=tag:Role,Values=$1" "Name=instance-state-name,Values=$2" \
          --query 'Reservations[].Instances[].InstanceId' --output text; }
case "${1:-}" in
  start)
    for r in cp-ldap cp-controller cp-broker cp-services; do
      i=$(ids $r stopped); [[ -z "$i" ]] && continue
      echo "starting $r"; aws ec2 start-instances --instance-ids $i >/dev/null; aws ec2 wait instance-status-ok --instance-ids $i
    done
    echo "started; allow ~2 min for the brokers, then check describe --status from the trainer VM" ;;
  stop)
    for r in cp-services cp-broker cp-controller cp-ldap; do
      i=$(ids $r running); [[ -z "$i" ]] && continue
      echo "stopping $r"; aws ec2 stop-instances --instance-ids $i >/dev/null; aws ec2 wait instance-stopped --instance-ids $i
    done
    echo "stopped" ;;
  status)
    aws ec2 describe-instances --filters Name=tag:Stack,Values=confluent-platform "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,InstanceType,State.Name]' --output text | sort | column -t ;;
  *) echo "usage: $0 start|stop|status"; exit 1 ;;
esac
