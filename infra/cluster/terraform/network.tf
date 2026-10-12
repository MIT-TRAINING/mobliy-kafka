locals {
  azs = slice(sort(tolist(setintersection(
    toset(data.aws_ec2_instance_type_offerings.controller.locations),
    toset(data.aws_ec2_instance_type_offerings.broker.locations),
  ))), 0, length(var.private_subnet_cidrs))
  nat = var.deploy_nodes && var.enable_nat
}

resource "aws_subnet" "private" {
  count                   = length(var.private_subnet_cidrs)
  vpc_id                  = data.aws_vpc.lab.id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = false
  tags                    = { Name = "kafka-lab-private-${local.azs[count.index]}" }
}

resource "aws_eip" "nat" {
  count  = local.nat ? 1 : 0
  domain = "vpc"
  tags   = { Name = "kafka-lab-nat" }
}

resource "aws_nat_gateway" "lab" {
  count         = local.nat ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = data.aws_subnet.public.id
  tags          = { Name = "kafka-lab" }
}

resource "aws_route_table" "private" {
  vpc_id = data.aws_vpc.lab.id
  tags   = { Name = "kafka-lab-private" }
}

resource "aws_route" "private_nat" {
  count                  = local.nat ? 1 : 0
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.lab[0].id
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ---- security group: reachable only from the lab VMs and from itself ----
resource "aws_security_group" "cluster" {
  name        = "kafka-lab-apache"
  description = "Shared Apache Kafka cluster: clients from the lab VM SG only"
  vpc_id      = data.aws_vpc.lab.id
  tags        = { Name = "kafka-lab-apache" }
}

resource "aws_vpc_security_group_ingress_rule" "client" {
  security_group_id            = aws_security_group.cluster.id
  referenced_security_group_id = data.aws_security_group.vm.id
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
  description                  = "Kafka clients (SASL/SCRAM) from lab VMs"
}

resource "aws_vpc_security_group_ingress_rule" "metrics" {
  for_each                     = toset(["7071", "9100"])
  security_group_id            = aws_security_group.cluster.id
  referenced_security_group_id = data.aws_security_group.vm.id
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.key)
  to_port                      = tonumber(each.key)
  description                  = "JMX exporter / node_exporter, scraped from the trainer VM"
}

resource "aws_vpc_security_group_ingress_rule" "self" {
  security_group_id            = aws_security_group.cluster.id
  referenced_security_group_id = aws_security_group.cluster.id
  ip_protocol                  = "-1"
  description                  = "Controller quorum and inter-broker traffic"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.cluster.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# ---- private DNS: apache-kafka.lab.internal and per-node names ----
resource "aws_route53_zone" "internal" {
  name = var.internal_zone
  vpc {
    vpc_id = data.aws_vpc.lab.id
  }
  comment = "Private names for the shared Kafka clusters"
}

resource "aws_route53_record" "node" {
  for_each = local.nodes
  zone_id  = aws_route53_zone.internal.zone_id
  name     = each.value.hostname
  type     = "A"
  ttl      = 30
  records  = [each.value.ip]
}

# Bootstrap name used by the labs: any broker answers, clients then follow advertised.listeners
resource "aws_route53_record" "bootstrap" {
  zone_id = aws_route53_zone.internal.zone_id
  name    = "apache-kafka"
  type    = "A"
  ttl     = 30
  records = [for k, n in local.nodes : n.ip if n.role == "broker"]
}
