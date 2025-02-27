# NOTE the database logins have been put manually onto AWS secrets manager
# This code can only retrieve from AWS, it does not put anything there.
# It creates an IAM role and sets permissions to allow RDS to access secrets manager


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

output "db_credentials" {
  value = local.db_credentials
  sensitive = true
}

data "aws_iam_policy_document" "iam_secrets_access_doc" {
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [
      data.aws_secretsmanager_secret.db_credentials.arn
    ]
  }
}

resource "aws_iam_policy" "secrets_access_policy" {
  name        = "secrets-access-policy"
  description = "Allows access to database credentials in AWS Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      Resource = data.aws_secretsmanager_secret.db_credentials.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_secrets_policy" {
  role = aws_iam_role.lambda_1_role.name
  policy_arn = aws_iam_policy.secrets_access_policy.arn
}