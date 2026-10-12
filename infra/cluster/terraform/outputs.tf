output "bootstrap" {
  value = "apache-kafka.${var.internal_zone}:9092"
}

output "cluster_id" {
  value = random_id.cluster.b64_url
}

output "bake" {
  description = "Inputs for infra/cluster/scripts/bake-broker-ami.sh"
  value = {
    ami_name_prefix = var.ami_name_prefix
    instance_type   = var.broker_instance_type
    root_volume_gb  = var.broker_volume_gb
  }
}

output "nodes" {
  description = "Node name -> private IP, role, AZ"
  value = {
    for k, n in local.nodes : k => {
      role = n.role
      ip   = n.ip
      az   = local.azs[n.az_index]
      id   = var.deploy_nodes ? aws_instance.node[k].id : null
    }
  }
}
