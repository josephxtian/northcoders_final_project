# s3 ingestion_bucket
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = var.data_ingestion_bucket

  tags = {
    Name = "bucket to hold raw data gathered by first lambda"
  }
}

# s3 processed_bucket
resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = var.data_processed_bucket

  tags = {
    Name = "bucket to hold processed data from second lambda"
  }
}

# bucket for lambda code itself
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket_prefix = var.lambda_code_bucket

  tags = {
    Name = "code bucket to hold all lambda code"
  }
}

# upload lambda function 1 object
resource "aws_s3_object" "lambda_1_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "${var.lambda_1_name}/index.py"
  source = "${path.module}/../${var.lambda_1_name}/index.py"
}

# upload lambda function 2 object
resource "aws_s3_object" "lambda_2_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "${var.lambda_2_name}/index.py"
  source = "${path.module}/../${var.lambda_2_name}/index.py"
}

# upload lambda function 3 object
resource "aws_s3_object" "lambda_3_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "${var.lambda_3_name}/index.py"
  source = "${path.module}/../${var.lambda_3_name}/index.py"
}

/*
lambda will need access to its dependencies in the layer folder to run the code in AWS - in the lambda.tf these dependencies are bundled into a .zip file 

# object for layer folder code
resource "aws_s3_object" "layer_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "quotes/layer.zip"
  source = "${path.module}/../layer.zip"
}
*/