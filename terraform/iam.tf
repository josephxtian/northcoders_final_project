/*
# ---------------
# Lambda IAM Role 
# ---------------
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}
# ------------------------------
# Lambda IAM Policy for S3 Write
# ------------------------------
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.data_bucket.arn}/*"
    ]
  }
}
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-write"
  policy      = data.aws_iam_policy_document.cw_document.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}
*/
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
# Lambda
# https://spacelift.io/blog/terraform-aws-lambda
# Create IAM role
# Add IAM policies - check the below policies might be needed
#   logs:CreateLogGroup
#   logs:CreateLogStream
#   logs:PutLogEvents
# Add IAM policies to the IAM role
#<<<<<<< working_on_terraform
#=======
#
#
#>>>>>>> main
resource "aws_iam_role" "lambda_1_role" {
  name_prefix        = "role-${var.lambda_1_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json # check this
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
resource "aws_iam_role" "lambda_2_role" {
  name_prefix        = "role-${var.lambda_2_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json # check this
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
resource "aws_iam_role" "lambda_3_role" {
  name_prefix        = "role-${var.lambda_3_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json # check this
}