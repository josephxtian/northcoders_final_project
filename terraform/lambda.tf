# lambda function to import from database and put into ingestion s3 bucket
resource "aws_lambda_function" "lambda_raw_data_to_ingestion_bucket" {
  filename      = data.archive_file.zip_raw_data_to_ingestion_bucket.output_path
  function_name = "${var.lambda_1_name}-function"
  role          = aws_iam_role.lambda_1_role.arn
  handler       = "index.lambda_handler"
  runtime       = var.python_runtime
}

data "archive_file" "zip_raw_data_to_ingestion_bucket" {
type        = "zip"
source_dir  = "${path.module}/../${var.lambda_1_name}/"
output_path = "${path.module}/../python_zips/${var.lambda_1_name}.zip"
}

# lambda to process information between ingestion_bucket and processed_bucket
resource "aws_lambda_function" "lambda_ingestion_to_processed_bucket" {
  filename      = data.archive_file.zip_ingestion_to_processed_bucket.output_path
  function_name = "${var.lambda_2_name}-function"
  role          = aws_iam_role.lambda_2_role.arn
  handler       = "index.lambda_handler"
  runtime       = var.python_runtime
}

data "archive_file" "zip_ingestion_to_processed_bucket" {
type        = "zip"
source_dir  = "${path.module}/../${var.lambda_2_name}/"
output_path = "${path.module}/../python_zips/${var.lambda_2_name}.zip"
}

# lambda to move data from processed_bucket to data warehouse
resource "aws_lambda_function" "lambda_processed_bucket_to_warehouse" {
  filename      = data.archive_file.zip_processed_bucket_to_warehouse.output_path
  function_name = "${var.lambda_3_name}-function"
  role          = aws_iam_role.lambda_3_role.arn
  handler       = "index.lambda_handler"
  runtime       = var.python_runtime
}

data "archive_file" "zip_processed_bucket_to_warehouse" {
type        = "zip"
source_dir  = "${path.module}/../${var.lambda_3_name}/"
output_path = "${path.module}/../python_zips/${var.lambda_3_name}.zip"
}
