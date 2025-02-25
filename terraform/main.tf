
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
backend "s3" {
    bucket = ""
    key = ""
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"

  default_tags {
    tags = {
      ProjectName = "Northcoders final project"
      Team = "Data Engineering"
      DeployedFrom = "Terraform"
      Repository = "northcoders_final_project_25"
      CostCentre = "DE"
      Environment = "dev"
    }
  }
}
