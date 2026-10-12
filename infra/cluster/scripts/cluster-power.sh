#!/usr/bin/env bash
# Start or stop the 7 Kafka nodes in the right order.
#   start: controllers first, then brokers.   stop: brokers first, then controllers.
# Private IPs and EBS survive stop/start, so data and DNS are intact.
set -euo pipefail
export AWS_PROFILE="${AWS_PROFILE:-kafka-training}" AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
ids() { aws ec2 describe-instances --filters Name=tag:Project,Values=kafka-training "Name=tag:Role,Values=$1" "Name=instance-state-name,Values=$2" \
          --query 'Reservations[].Instances[].InstanceId' --output text; }
case "${1:-}" in
  start)
    c=$(ids kafka-controller stopped); [[ -n "$c" ]] && { aws ec2 start-instances --instance-ids $c >/dev/null; aws ec2 wait instance-status-ok --instance-ids $c; }
    b=$(ids kafka-broker stopped);     [[ -n "$b" ]] && { aws ec2 start-instances --instance-ids $b >/dev/null; aws ec2 wait instance-status-ok --instance-ids $b; }
    echo "started; allow ~1 min for Kafka to rejoin, then run check-cluster.sh" ;;
  stop)
    b=$(ids kafka-broker running);     [[ -n "$b" ]] && { aws ec2 stop-instances --instance-ids $b >/dev/null; aws ec2 wait instance-stopped --instance-ids $b; }
    c=$(ids kafka-controller running); [[ -n "$c" ]] && { aws ec2 stop-instances --instance-ids $c >/dev/null; aws ec2 wait instance-stopped --instance-ids $c; }
    echo "stopped" ;;
  *) echo "usage: $0 start|stop"; exit 1 ;;
esac
