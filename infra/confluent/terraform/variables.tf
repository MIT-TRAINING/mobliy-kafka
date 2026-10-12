variable "aws_profile" {
  type    = string
  default = "kafka-training"
}

variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "deploy_nodes" {
  description = "false = security group, IAM and DNS names only; true = also launch the 8 hosts"
  type        = bool
  default     = false
}

variable "vpc_name" {
  description = "Name tag of the existing lab VPC (created by infra/terraform)"
  type        = string
  default     = "kafka-lab"
}

variable "vm_security_group_name" {
  description = "Lab VMs' security group (created by infra/terraform): the only client source"
  type        = string
  default     = "kafka-lab-vm"
}

variable "private_subnet_name_prefix" {
  description = "Private subnets, NAT and route table come from infra/cluster/terraform"
  type        = string
  default     = "kafka-lab-private-"
}

variable "internal_zone" {
  description = "Private Route 53 zone created by infra/cluster/terraform"
  type        = string
  default     = "lab.internal"
}

variable "key_name" {
  description = "Existing EC2 key pair (infra/secrets/kafka-lab-admin.pem); used by Ansible over SSM, port 22 is never opened"
  type        = string
  default     = "kafka-lab-admin"
}

variable "controller_instance_type" {
  type    = string
  default = "t3a.medium"
}

variable "broker_instance_type" {
  type    = string
  default = "t3a.large"
}

variable "services_instance_type" {
  type    = string
  default = "t3a.large"
}

variable "ldap_instance_type" {
  type    = string
  default = "t3a.small"
}

variable "controller_volume_gb" {
  type    = number
  default = 20
}

variable "broker_volume_gb" {
  type    = number
  default = 100
}

variable "services_volume_gb" {
  type    = number
  default = 100
}

variable "ldap_volume_gb" {
  type    = number
  default = 20
}

variable "services_swap_gb" {
  description = "Swap file on cp-services: Schema Registry, Connect, ksqlDB and Control Center share 8 GB"
  type        = number
  default     = 4
}
