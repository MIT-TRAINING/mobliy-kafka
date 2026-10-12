#!/usr/bin/env bash
# Trainer drill: stop or start Kafka on one broker via SSM (announce it to learners first).
# Usage: broker-failure-demo.sh stop|start broker-12
# Watch from the trainer VM:  kafka-topics.sh --bootstrap-server $APACHE --command-config $CFG --describe --under-replicated-partitions
set -euo pipefail
export AWS_PROFILE="${AWS_PROFILE:-kafka-training}" AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
ACTION=${1:?stop|start}; NODE=${2:?broker-NN}
[[ "$ACTION" == stop || "$ACTION" == start ]] || { echo "stop|start"; exit 1; }
IID=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=kafka-$NODE" Name=instance-state-name,Values=running --query 'Reservations[0].Instances[0].InstanceId' --output text)
[[ "$IID" == i-* ]] || { echo "no running instance kafka-$NODE"; exit 1; }
CMD=$(aws ssm send-command --instance-ids "$IID" --document-name AWS-RunShellScript \
  --parameters "commands=[\"systemctl $ACTION kafka && systemctl is-active kafka || true\"]" --query Command.CommandId --output text)
sleep 5
aws ssm get-command-invocation --command-id "$CMD" --instance-id "$IID" --query '{status:Status,out:StandardOutputContent}' --output json
