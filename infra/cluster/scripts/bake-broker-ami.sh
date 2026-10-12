#!/usr/bin/env bash
# Bakes the Kafka node AMI: builder EC2 -> provision-broker.sh -> stop -> create-image -> terminate builder.
# Prereq: infra/terraform (lab VPC, SG, admin key) and infra/cluster/terraform base apply exist.
# The builder runs in the lab public subnet with the lab VM SG (SSH from the admin IP) and the admin key.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"          # infra/cluster
INFRA="$(cd "$HERE/.." && pwd)"                   # infra
export AWS_PROFILE="${AWS_PROFILE:-kafka-training}" AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"

lab() { terraform -chdir="$INFRA/terraform" output -json bake | jq -r "$1"; }
clu() { terraform -chdir="$HERE/terraform" output -json bake | jq -r "$1"; }
SUBNET=$(lab .subnet_id); SG=$(lab .security_group_id); KEY=$(lab .key_name); PEM=$(lab .key_file)
PREFIX=$(clu .ami_name_prefix)

BASE_AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
AMI_NAME="${PREFIX}$(date +%Y%m%d-%H%M)"
SSH_OPTS=(-i "$PEM" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
mkdir -p "$HERE/secrets"

echo ">> launching builder from $BASE_AMI"
IID=$(aws ec2 run-instances --image-id "$BASE_AMI" --instance-type t3a.medium --key-name "$KEY" \
  --subnet-id "$SUBNET" --security-group-ids "$SG" --associate-public-ip-address \
  --credit-specification CpuCredits=unlimited \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp2,DeleteOnTermination=true}" \
  --metadata-options HttpTokens=required \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kafka-broker-ami-builder},{Key=Project,Value=kafka-training}]' \
  --query 'Instances[0].InstanceId' --output text)
cleanup() { echo ">> terminating builder $IID"; aws ec2 terminate-instances --instance-ids "$IID" >/dev/null || true; }
trap cleanup EXIT

aws ec2 wait instance-running --instance-ids "$IID"
IP=$(aws ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo ">> builder $IID at $IP - waiting for SSH"
for _ in $(seq 1 40); do ssh "${SSH_OPTS[@]}" -o ConnectTimeout=5 ubuntu@"$IP" true 2>/dev/null && break; sleep 5; done
ssh "${SSH_OPTS[@]}" ubuntu@"$IP" 'cloud-init status --wait' >/dev/null

scp "${SSH_OPTS[@]}" "$HERE/ami/provision-broker.sh" ubuntu@"$IP":/tmp/provision-broker.sh
echo ">> provisioning"
ssh "${SSH_OPTS[@]}" ubuntu@"$IP" 'sudo bash /tmp/provision-broker.sh' 2>&1 | tee "$HERE/secrets/bake-broker-$(date +%Y%m%d-%H%M).log" | tail -25

echo ">> sanitising image"
ssh "${SSH_OPTS[@]}" ubuntu@"$IP" 'sudo bash -s' <<'REMOTE'
set -x
apt-get clean
cloud-init clean --logs
rm -f /etc/ssh/ssh_host_* /home/*/.ssh/authorized_keys /root/.ssh/authorized_keys /tmp/provision-broker.sh
rm -rf /var/lib/apt/lists/* /home/*/.bash_history /root/.bash_history
find /var/log -type f -exec truncate -s 0 {} \;
sync
REMOTE

echo ">> stopping builder and creating AMI $AMI_NAME"
aws ec2 stop-instances --instance-ids "$IID" >/dev/null
aws ec2 wait instance-stopped --instance-ids "$IID"
AMI=$(aws ec2 create-image --instance-id "$IID" --name "$AMI_NAME" --description "Kafka training: Apache Kafka node (controller/broker)" \
  --tag-specifications "ResourceType=image,Tags=[{Key=Project,Value=kafka-training}]" "ResourceType=snapshot,Tags=[{Key=Project,Value=kafka-training}]" \
  --query ImageId --output text)
aws ec2 wait image-available --image-ids "$AMI"
echo ">> AMI ready: $AMI ($AMI_NAME)"
