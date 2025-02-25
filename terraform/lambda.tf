# copied from previous work

data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/quotes.py"
  output_path      = "${path.module}/../function.zip"
}
/*
The quotes.py lambda will need access to the dependencies in the layer folder to run the code in AWS, these dependencies are bundled into a .zip file and added to the s3 in s3.tf 
*/

data "archive_file" "layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../layer/"
  output_path = "${path.module}/../layer.zip"
}


resource "aws_lambda_layer_version" "requests_layer" {
  layer_name          = "requests_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  # this should be the code_bucket you provision in s3.tf
  s3_key              = "2-cloud/layer.zip"
  # this should be the key for your layer_code in s3.tf
}

resource "aws_lambda_function" "quote_handler" {
  #TODO: Provision the lambda
  function_name = "quote_handler"
  runtime = "python3.12"
  role = aws_iam_role.lambda_role.arn
  handler = "lambda_name.handler"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "2-cloud/function.zip"

  timeout = 60
  #TODO: Connect the layer which is outlined above
}

