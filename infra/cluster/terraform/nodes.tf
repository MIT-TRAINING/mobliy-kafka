locals {
  # node id -> hostname, AZ index, static private IP
  controllers = {
    for i in range(var.controller_count) : "ctl-${i + 1}" => {
      role     = "controller"
      node_id  = i + 1
      hostname = "ctl-${i + 1}.${var.internal_zone}"
      az_index = i % length(aws_subnet.private)
      ip       = cidrhost(var.private_subnet_cidrs[i % length(aws_subnet.private)], 11 + floor(i / length(aws_subnet.private)))
    }
  }
  brokers = {
    for i in range(var.broker_count) : "broker-${11 + i}" => {
      role     = "broker"
      node_id  = 11 + i
      hostname = "broker-${11 + i}.${var.internal_zone}"
      az_index = i % length(aws_subnet.private)
      ip       = cidrhost(var.private_subnet_cidrs[i % length(aws_subnet.private)], 21 + floor(i / length(aws_subnet.private)))
    }
  }
  nodes = merge(local.controllers, local.brokers)

  quorum_voters = join(",", [for k, c in local.controllers : "${c.node_id}@${c.hostname}:9093"])

  server_properties = {
    for k, n in local.nodes : k => templatefile(
      n.role == "controller" ? "${path.module}/../config/controller.properties.tmpl" : "${path.module}/../config/broker.properties.tmpl",
      {
        node_id         = n.node_id
        quorum_voters   = local.quorum_voters
        hostname        = n.hostname
        rack            = local.azs[n.az_index]
        broker_password = random_password.broker.result
      }
    )
  }

  # SCRAM users admin + broker are created when the controllers format their storage
  scram_args = "--add-scram 'SCRAM-SHA-512=[name=admin,password=${random_password.admin.result}]' --add-scram 'SCRAM-SHA-512=[name=broker,password=${random_password.broker.result}]'"

  deployed = var.deploy_nodes ? local.nodes : {}
}

resource "aws_instance" "node" {
  for_each = local.deployed

  ami                         = data.aws_ami.broker[0].id
  instance_type               = each.value.role == "controller" ? var.controller_instance_type : var.broker_instance_type
  subnet_id                   = aws_subnet.private[each.value.az_index].id
  private_ip                  = each.value.ip
  vpc_security_group_ids      = [aws_security_group.cluster.id]
  iam_instance_profile        = aws_iam_instance_profile.node.name
  associate_public_ip_address = false

  credit_specification {
    cpu_credits = "unlimited"
  }

  root_block_device {
    volume_type           = "gp2"
    volume_size           = each.value.role == "controller" ? var.controller_volume_gb : var.broker_volume_gb
    encrypted             = true
    delete_on_termination = true
    tags                  = { Name = "kafka-${each.key}-root" }
  }

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  user_data = templatefile("${path.module}/node-boot.sh.tftpl", {
    hostname          = each.value.hostname
    cluster_id        = random_id.cluster.b64_url
    server_properties = local.server_properties[each.key]
    heap              = each.value.role == "controller" ? var.controller_heap : var.broker_heap
    scram_args        = each.value.role == "controller" ? local.scram_args : ""
  })

  tags = {
    Name = "kafka-${each.key}"
    Role = "kafka-${each.value.role}"
  }

  # A changed AMI or boot script must never silently replace a node (and its data).
  # To rebuild one deliberately: terraform apply -replace='aws_instance.node["broker-12"]'
  lifecycle {
    ignore_changes = [ami, user_data]
  }

  depends_on = [aws_route.private_nat, aws_route53_record.node]
}
