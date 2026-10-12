data "aws_vpc" "lab" {
  filter {
    name   = "tag:Name"
    values = [var.vpc_name]
  }
}

data "aws_subnet" "public" {
  vpc_id = data.aws_vpc.lab.id
  filter {
    name   = "tag:Name"
    values = [var.public_subnet_name]
  }
}

data "aws_security_group" "vm" {
  vpc_id = data.aws_vpc.lab.id
  name   = var.vm_security_group_name
}

# AZs that offer both instance types
data "aws_ec2_instance_type_offerings" "controller" {
  filter {
    name   = "instance-type"
    values = [var.controller_instance_type]
  }
  location_type = "availability-zone"
}

data "aws_ec2_instance_type_offerings" "broker" {
  filter {
    name   = "instance-type"
    values = [var.broker_instance_type]
  }
  location_type = "availability-zone"
}

data "aws_ami" "broker" {
  count       = var.deploy_nodes ? 1 : 0
  owners      = ["self"]
  most_recent = true
  filter {
    name   = "name"
    values = ["${var.ami_name_prefix}*"]
  }
}
