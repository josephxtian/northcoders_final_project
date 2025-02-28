# NOTE the database logins have been put manually onto AWS secrets manager
# This code can only retrieve from AWS, it does not put anything there.
# It creates an IAM role and sets permissions to allow RDS to access secrets manager

#Do not need the util function, can delete once this is checked

#this gets the credentials from AWS Secrets Manager (gets arn ect not actual value)
data "aws_secretsmanager_secret" "db_credentials" {
  name = "totesys/db_credentials"
}

#gets the latest versiob of the secret 
data "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = data.aws_secretsmanager_secret.db_credentials.id
}

#stores secret in local variable for use in terraform
locals {
  db_credentials = jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)
}

#this is for debugging purposes
output "db_credentials" {
  value = local.db_credentials
  sensitive = true
}

#gives ccess to read credetnials from secrets manager
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

#attaches policy to role
resource "aws_iam_role_policy_attachment" "attach_secrets_policy" {
  role = aws_iam_role.lambda_1_role.name
  policy_arn = aws_iam_policy.secrets_access_policy.arn
}