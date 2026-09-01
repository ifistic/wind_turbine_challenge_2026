terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Uncomment and configure once you have an S3 bucket for state
  # (recommended over local state once you're iterating on this repeatedly)
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "wind-turbine-pipeline/terraform.tfstate"
  #   region = "eu-west-2"
  # }
}

provider "aws" {
  region = var.aws_region
}
