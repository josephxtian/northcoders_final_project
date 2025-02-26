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
  username            = local.db_credentials["user"]
  password            = local.db_credentials["password"]
  port                = local.db_credentials["port"]
  publicly_accessible = false
}

resource "aws_iam_role" "rds_access_role" {
  name = "rds-access-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "rds.amazonaws.com"
      }
    }]
  })
}

#policy to grant access to secrets manager for RDS
resource "aws_iam_policy" "rds_access_policy" {
  name        = "rds-secrets-access-policy"
  description = "Allows access to RDS credentials stored in AWS Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ]
      Resource = data.aws_secretsmanager_secret.db_credentials.arn
    }]
  })
}

#attach the policy to the role
resource "aws_iam_role_policy_attachment" "rds_policy_attachment" {
  role       = aws_iam_role.rds_access_role.name
  policy_arn = aws_iam_policy.rds_access_policy.arn
}
