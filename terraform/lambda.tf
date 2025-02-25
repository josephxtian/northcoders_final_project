/* # copied from previous work
resource "aws_lambda_function" "s3_file_reader" {
    function_name = "${var.lambda_name}"
    s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    s3_key = "s3_file_reader/function.zip"
    role = aws_iam_role.lambda_role.arn
    handler = "reader.lambda_handler"
    runtime = "python3.9"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/file_reader/reader.py"
  output_path = "${path.module}/../function.zip"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
resource "aws_lambda_permission" "allow_s3" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_file_reader.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}
*/
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
# lambda function to import from database and put into ingestion s3 bucket
resource "aws_lambda_function" "lambda_raw_data_to_ingestion_bucket" {
filename                       = "${data.archive_file.zip_raw_data_to_ingestion_bucket.output_path}"
function_name                  = "${var.lambda_1_name}-function"
role                           = aws_iam_role.lambda_1_role.arn
handler                        = "index.lambda_handler"
runtime                        = "${var.python_runtime}"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
data "archive_file" "zip_raw_data_to_ingestion_bucket" {
type        = "zip"
source_dir  = "${path.module}/${var.lambda_1_name}/"
output_path = "${path.module}/python_zips/${var.lambda_1_name}.zip"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
# lambda to process information between ingestion_bucket and processed_bucket
resource "aws_lambda_function" "lambda_ingestion_to_processed_bucket" {
filename                       = "${data.archive_file.zip_ingestion_to_processed_bucket.output_path}"
function_name                  = "${var.lambda_2_name}-function"
role                           = aws_iam_role.lambda_2_role.arn
handler                        = "index.lambda_handler"
runtime                        = "${var.python_runtime}"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
data "archive_file" "zip_ingestion_to_processed_bucket" {
type        = "zip"
source_dir  = "${path.module}/${var.lambda_2_name}/"
output_path = "${path.module}/python_zips/${var.lambda_2_name}.zip"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
# lambda to move data from processed_bucket to data warehouse
resource "aws_lambda_function" "lambda_processed_bucket_to_warehouse" {
filename                       = "${data.archive_file.zip_processed_bucket_to_warehouse.output_path}"
function_name                  = "${var.lambda_3_name}-function"
role                           = aws_iam_role.lambda_3_role.arn
handler                        = "index.lambda_handler"
runtime                        = "${var.python_runtime}"
}
#<<<<<<< working_on_terraform
#=======
#
#>>>>>>> main
data "archive_file" "zip_processed_bucket_to_warehouse" {
type        = "zip"
source_dir  = "${path.module}/${var.lambda_3_name}/"
output_path = "${path.module}/python_zips/${var.lambda_3_name}.zip"
#<<<<<<< working_on_terraform
}
#=======
}
#>>>>>>> main
