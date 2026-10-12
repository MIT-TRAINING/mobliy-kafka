terraform {
  required_version = ">= 1.6"

  required_providers {
    aws   = { source = "hashicorp/aws", version = "~> 6.0" }
    local = { source = "hashicorp/local", version = "~> 2.5" }
  }
}

provider "aws" {
  profile = var.aws_profile
  region  = var.aws_region

  default_tags {
    tags = {
      Project   = "kafka-training"
      ManagedBy = "terraform"
      Stack     = "confluent-platform"
    }
  }
}
