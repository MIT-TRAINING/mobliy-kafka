output "bootstrap" {
  value = "cp-kafka.${var.internal_zone}:9092"
}

output "hosts" {
  description = "Host -> role, private IP, AZ, instance ID"
  value = {
    for k, h in local.hosts : "${k}.${var.internal_zone}" => {
      role = h.role
      ip   = h.ip
      az   = local.subnets[h.subnet].az
      id   = var.deploy_nodes ? aws_instance.node[k].id : null
    }
  }
}
