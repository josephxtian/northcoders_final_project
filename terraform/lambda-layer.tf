#lambda layer to import pg8000

data "archive_file" "lambda-layer" {
  type = "zip"
  source_dir = "${path.module}/../lambda-layer/python/"
  output_path = "${path.module}/../pg8000-layer.zip"
}

resource "aws_lambda_layer_version" "lambda_layer" {
  filename   = "${path.module}/../lambda-layer/pg8000-layer.zip"
  layer_name = "ingestion_bucket_dependencies"
}

resource "aws_lambda_layer_version_permission" "lambda_layer_permission" {
  layer_name     = aws_lambda_layer_version.lambda_layer.layer_name
  version_number = aws_lambda_layer_version.lambda_layer.version
  principal      = "*"
  action         = "lambda:GetLayerVersion"
  statement_id   = "test-pg8000-layer"
}

data "archive_file" "lambda-layer_warehouse" {
  type = "zip"
  source_dir = "${path.module}/../py_packages/python/"
  output_path = "${path.module}/../requirements-package.zip"
}


resource "aws_lambda_layer_version" "layer_write_to_warehouse" {
  layer_name = "lambda_layer_to_warehouse"
  filename = "${path.module}/../py_packages/requirements-package.zip"
}

resource "aws_lambda_layer_version_permission" "lambda_layer_write_to_warehouse_permission" {
  layer_name     = aws_lambda_layer_version.layer_write_to_warehouse.layer_name
  version_number = aws_lambda_layer_version.layer_write_to_warehouse.version
  principal      = "*"
  action         = "lambda:GetLayerVersion"
  statement_id   = "test-warehouse-layer"
}
