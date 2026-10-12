variable "aws_profile" {
  type    = string
  default = "kafka-training"
}

variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "deploy_nodes" {
  description = "false = network, DNS, IAM and secrets only (needed before baking the AMI); true = also launch the 7 nodes and the NAT gateway"
  type        = bool
  default     = false
}

variable "enable_nat" {
  description = "NAT gateway for the private subnets (SSM Session Manager and OS patching). Only created when deploy_nodes is true"
  type        = bool
  default     = true
}

variable "vpc_name" {
  description = "Name tag of the existing lab VPC (created by infra/terraform)"
  type        = string
  default     = "kafka-lab"
}

variable "vm_security_group_name" {
  description = "Name of the lab VMs' security group (created by infra/terraform)"
  type        = string
  default     = "kafka-lab-vm"
}

variable "public_subnet_name" {
  type    = string
  default = "kafka-lab-public"
}

variable "private_subnet_cidrs" {
  description = "One private subnet per AZ for the cluster nodes"
  type        = list(string)
  default     = ["10.20.11.0/24", "10.20.12.0/24", "10.20.13.0/24"]
}

variable "internal_zone" {
  description = "Private Route 53 zone the course materials use (apache-kafka.lab.internal)"
  type        = string
  default     = "lab.internal"
}

variable "controller_instance_type" {
  type    = string
  default = "t3a.medium"
}

variable "controller_volume_gb" {
  type    = number
  default = 20
}

variable "broker_instance_type" {
  type    = string
  default = "t3a.large"
}

variable "broker_volume_gb" {
  type    = number
  default = 50
}

variable "broker_count" {
  type    = number
  default = 4
}

variable "controller_count" {
  type    = number
  default = 3
}

variable "controller_heap" {
  type    = string
  default = "-Xms1g -Xmx1g"
}

variable "broker_heap" {
  type    = string
  default = "-Xms4g -Xmx4g"
}

variable "learners" {
  description = "SCRAM users (same ids as the lab VMs, minus the trainer); the trainer user is always added"
  type        = list(string)
  default     = ["l01", "l02", "l03", "l04", "l05", "l06", "l07", "l08", "l09", "l10", "l11", "l12", "l13", "l14", "l15", "l16", "l17", "l18", "l19"]
}

variable "ami_name_prefix" {
  type    = string
  default = "kafka-broker-golden-"
}
