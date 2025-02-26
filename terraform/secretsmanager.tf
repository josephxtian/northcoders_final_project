provider "aws" {
  region = "eu-west-2"
}

#this gets the credentials from AWS Secrets Manager
data "aws_secretsmanager_secret" "db_credentials" {
  name = "totesys/db_credentials"
}

data "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = data.aws_secretsmanager_secret.db_credentials.id
}

#gets secrets value locally 
locals {
  db_credentials = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)
}

resource "aws_db_instance" "totesys_db" {
  identifier          = "totesys-db"
  engine              = "postgres"
  instance_class      = "db.t3.micro"
  allocated_storage   = 20 #minimum for postgres is 10
  username           = local.db_credentials["user"]
  password           = local.db_credentials["password"]
  port               = local.db_credentials["port"]
  publicly_accessible = false
}

resource "aws_iam_role" "rds_access_role" {
    name = 


}

resource "aws_iam_policy" "rds_access_policy" {


}
