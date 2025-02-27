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

# Lambda
# https://spacelift.io/blog/terraform-aws-lambda
# Create IAM role
# Add IAM policies - check the below policies might be needed
#   logs:CreateLogGroup
#   logs:CreateLogStream
#   logs:PutLogEvents
# Add IAM policies to the IAM role

data "aws_iam_policy_document" "example" {
  statement {
    actions = [
      # write our allowed actions here
      "s3:ListAllMyBuckets",
      "s3:GetBucketLocation",
    ]

    resources = [
      # resource ARNs this statement applies to
      "arn:aws:s3:::*",
    ]
  }
  # same again
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}",
    ]

    # condition allows us to do logic and boolean
    condition {
      test     = "StringLike"
      variable = "s3:prefix"

      values = [
        "",
        "home/",
        "home/&{aws:username}/",
      ]
    }
  }
  # another example
  statement {
    actions = [
      "s3:*",
    ]

    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}/home/&{aws:username}",
      "arn:aws:s3:::${var.s3_bucket_name}/home/&{aws:username}/*",
    ]
  }
}

resource "aws_iam_policy" "example" {
  name   = "example_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.example.json
}
*/