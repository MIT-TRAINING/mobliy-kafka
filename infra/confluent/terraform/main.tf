# Shared Confluent Platform 8.x cluster (infra/LAB-SETUP.md §5, infra/confluent/README.md Part B).
# Separate Terraform root next to infra/terraform (lab VMs) and infra/cluster/terraform (Apache
# cluster, which owns the private subnets, NAT gateway and the lab.internal zone reused here).
# Terraform only builds hosts and names; cp-ansible installs the software (Part B.5).

data "aws_vpc" "lab" {
  filter {
    name   = "tag:Name"
    values = [var.vpc_name]
  }
}

data "aws_security_group" "vm" {
  vpc_id = data.aws_vpc.lab.id
  name   = var.vm_security_group_name
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.lab.id]
  }
  filter {
    name   = "tag:Name"
    values = ["${var.private_subnet_name_prefix}*"]
  }
}

data "aws_subnet" "private" {
  for_each = toset(data.aws_subnets.private.ids)
  id       = each.value
}

data "aws_route53_zone" "internal" {
  name         = var.internal_zone
  private_zone = true
  vpc_id       = data.aws_vpc.lab.id
}

data "aws_ami" "ubuntu" {
  owners      = ["099720109477"] # Canonical
  most_recent = true
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

locals {
  # subnets ordered by AZ so node placement is stable
  subnets = [for s in sort([for s in data.aws_subnet.private : "${s.availability_zone}|${s.id}|${s.cidr_block}"]) : {
    az   = split("|", s)[0]
    id   = split("|", s)[1]
    cidr = split("|", s)[2]
  }]
  n = length(local.subnets)

  # One controller and one broker per AZ; services and LDAP in the first AZ.
  # Host numbers .41/.51/.61/.71 stay clear of the Apache nodes (.11, .21).
  hosts = merge(
    { for i in range(3) : "cp-ctl-${i + 1}" => { role = "controller", subnet = i % local.n, ip = cidrhost(local.subnets[i % local.n].cidr, 41 + floor(i / local.n)) } },
    { for i in range(3) : "cp-broker-${i + 1}" => { role = "broker", subnet = i % local.n, ip = cidrhost(local.subnets[i % local.n].cidr, 51 + floor(i / local.n)) } },
    { "cp-services" = { role = "services", subnet = 0, ip = cidrhost(local.subnets[0].cidr, 61) } },
    { "cp-ldap" = { role = "ldap", subnet = 0, ip = cidrhost(local.subnets[0].cidr, 71) } },
  )

  instance_type = {
    controller = var.controller_instance_type
    broker     = var.broker_instance_type
    services   = var.services_instance_type
    ldap       = var.ldap_instance_type
  }
  volume_gb = {
    controller = var.controller_volume_gb
    broker     = var.broker_volume_gb
    services   = var.services_volume_gb
    ldap       = var.ldap_volume_gb
  }

  deployed = var.deploy_nodes ? local.hosts : {}
}

# ---- security group: clients from the lab VMs only, everything else inside the cluster ----
resource "aws_security_group" "cp" {
  name        = "kafka-lab-cp"
  description = "Shared Confluent Platform cluster: clients from the lab VM SG only"
  vpc_id      = data.aws_vpc.lab.id
  tags        = { Name = "kafka-lab-cp" }
}

resource "aws_vpc_security_group_ingress_rule" "client" {
  for_each = {
    "9092" = "Kafka clients (SASL_SSL/PLAIN + LDAP)"
    "8090" = "MDS login and Admin REST"
    "8081" = "Schema Registry"
    "8083" = "Kafka Connect"
    "8088" = "ksqlDB"
    "9021" = "Control Center (published through Caddy on the trainer VM)"
  }
  security_group_id            = aws_security_group.cp.id
  referenced_security_group_id = data.aws_security_group.vm.id
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.key)
  to_port                      = tonumber(each.key)
  description                  = "${each.value} from lab VMs"
}

resource "aws_vpc_security_group_ingress_rule" "self" {
  security_group_id            = aws_security_group.cp.id
  referenced_security_group_id = aws_security_group.cp.id
  ip_protocol                  = "-1"
  description                  = "KRaft, inter-broker, MDS, LDAP and component traffic"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.cp.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# ---- IAM: no open SSH port; Ansible tunnels SSH through SSM Session Manager ----
resource "aws_iam_role" "node" {
  name = "kafka-lab-cp-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "node" {
  name = "kafka-lab-cp-node"
  role = aws_iam_role.node.name
}

# ---- hosts ----
resource "aws_instance" "node" {
  for_each = local.deployed

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = local.instance_type[each.value.role]
  subnet_id                   = local.subnets[each.value.subnet].id
  private_ip                  = each.value.ip
  vpc_security_group_ids      = [aws_security_group.cp.id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  key_name                    = var.key_name
  associate_public_ip_address = false

  credit_specification {
    cpu_credits = "unlimited"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = local.volume_gb[each.value.role]
    encrypted             = true
    delete_on_termination = true
    tags                  = { Name = "${each.key}-root" }
  }

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  user_data = templatefile("${path.module}/boot.sh.tftpl", {
    hostname = "${each.key}.${var.internal_zone}"
    swap_gb  = each.value.role == "services" ? var.services_swap_gb : 0
  })

  tags = {
    Name = each.key
    Role = "cp-${each.value.role}"
  }

  # A newer Ubuntu AMI or boot script must never silently replace a node (and its data).
  # To rebuild one deliberately: terraform apply -replace='aws_instance.node["cp-broker-2"]'
  lifecycle {
    ignore_changes = [ami, user_data]
  }
}

# ---- private DNS: per-host names and the learner bootstrap name ----
resource "aws_route53_record" "host" {
  for_each = local.hosts
  zone_id  = data.aws_route53_zone.internal.zone_id
  name     = each.key
  type     = "A"
  ttl      = 30
  records  = [each.value.ip]
}

# cp-kafka.lab.internal: any broker answers, clients then follow advertised.listeners
resource "aws_route53_record" "bootstrap" {
  zone_id = data.aws_route53_zone.internal.zone_id
  name    = "cp-kafka"
  type    = "A"
  ttl     = 30
  records = [for k, h in local.hosts : h.ip if h.role == "broker"]
}

# ---- Ansible connection file: instance IDs + SSH over SSM (git-ignored, under secrets/) ----
resource "local_file" "ansible_ssm" {
  count           = var.deploy_nodes ? 1 : 0
  filename        = "${path.module}/../secrets/ansible-ssm.yml"
  file_permission = "0600"
  content = yamlencode({
    all = {
      vars = {
        cp_secrets_dir               = abspath("${path.module}/../secrets")
        ansible_user                 = "ubuntu"
        ansible_ssh_private_key_file = abspath("${path.module}/../../secrets/kafka-lab-admin.pem")
        ansible_ssh_common_args      = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ProxyCommand=\"aws ssm start-session --profile ${var.aws_profile} --region ${var.aws_region} --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p\""
      }
      hosts = { for k, i in aws_instance.node : "${k}.${var.internal_zone}" => { ansible_host = i.id } }
    }
  })
}
