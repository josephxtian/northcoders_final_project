# policy_doc >>> policy >>> role <<< sts:AssumeRole
# aws_iam_policy_document ->aws_iam_policy-> aws_iam_role-> aws_iam_role_policy_attachment

# GLOBALLY USED
# allows roles to access each service with credentials
# indentifiers are called 'service principal name'
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com","s3.amazonaws.com","scheduler.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "iam_cloudwatch_log_doc" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
    ]
    resources = [
      "${aws_cloudwatch_log_group.lambda_log.arn}"
    ]
  }
}

resource "aws_iam_policy" "iam_cloudwatch_log" {
  name   = "cloudwatch_log_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.iam_cloudwatch_log_doc.json
}

# LAMBDA 1 - INGESTION
# writes to ingestion s3 and accesses lambda 1
data "aws_iam_policy_document" "iam_ingestion_write_policy_doc" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:UploadPart"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}"
    ]
  }
}

resource "aws_iam_role" "lambda_1_role" {
  name_prefix        = "role-${var.lambda_1_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_policy" "iam_ingestion_write_policy" {
  name   = "ingestion_write_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.iam_ingestion_write_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_1_write_policy_attachment" {
  role = aws_iam_role.lambda_1_role.name
  policy_arn = aws_iam_policy.iam_ingestion_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_1_cloudwatch_attachment" {
  role = aws_iam_role.lambda_1_role.name
  policy_arn = aws_iam_policy.iam_cloudwatch_log.arn
}

# LAMBDA 2 - INGESTION TO PROCESSED_BUCKET
# reads from ingestion_bucket and writes to processed_bucket

data "aws_iam_policy_document" "iam_processing_policy_doc" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:UploadPart",
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}",
      "${aws_s3_bucket.processed_bucket.arn}"
    ]
  }
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
    ]
    resources = [
      "${aws_cloudwatch_log_group.lambda_log.arn}"
    ]
  }
}

resource "aws_iam_role" "lambda_2_role" {
  name_prefix        = "role-${var.lambda_2_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_policy" "iam_processing_policy" {
  name   = "processing_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.iam_processing_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_2_read_write_policy_attachment" {
  role = aws_iam_role.lambda_2_role.name
  policy_arn = aws_iam_policy.iam_processing_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_2_cloudwatch_attachment" {
  role = aws_iam_role.lambda_2_role.name
  policy_arn = aws_iam_policy.iam_cloudwatch_log.arn
}

# LAMBDA 3 - PROCESSED_BUCKET TO DATA WAREHOUSE
# reads from processed_bucket
# write to data warehouse???

data "aws_iam_policy_document" "iam_processed_read_policy_doc" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:UploadPart"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}"
    ]
  }
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
    ]
    resources = [
      "${aws_cloudwatch_log_group.lambda_log.arn}"
    ]
  }
}

resource "aws_iam_role" "lambda_3_role" {
  name_prefix        = "role-${var.lambda_3_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_policy" "iam_processed_read_policy" {
  name   = "processed_read_policy"
  path   = "/"
  policy = data.aws_iam_policy_document.iam_processed_read_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_3_read_policy_attachment" {
  role = aws_iam_role.lambda_3_role.name
  policy_arn = aws_iam_policy.iam_processed_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_3_cloudwatch_attachment" {
  role = aws_iam_role.lambda_3_role.name
  policy_arn = aws_iam_policy.iam_cloudwatch_log.arn
}

# EVENTBRIDGE SCHEDULER - RUNS LAMBDA 1 <= EVERY 30 MINS
# accesses eventbridge scheduler

data "aws_iam_policy_document" "iam_event_scheduler_lambda_1_doc" {
  statement {
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      "${aws_lambda_function.lambda_raw_data_to_ingestion_bucket.arn}"
    ]
  }
}

resource "aws_iam_role" "eventbridge_scheduler_role" {
  name_prefix         = "role-lambda-1-event-scheduler"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_policy" "iam_scheduling_policy" {
  name = "event_scheduling_policy"
  path = "/"
  policy = data.aws_iam_policy_document.iam_event_scheduler_lambda_1_doc.json
}

resource "aws_iam_role_policy_attachment" "iam_event_scheduler_attachment" {
  role = aws_iam_role.eventbridge_scheduler_role.name
  policy_arn = aws_iam_policy.iam_scheduling_policy.arn
}