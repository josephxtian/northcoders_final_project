variable "ec2-instance" {
  type    = string
  default = "i-03e485bc335b9a58a"
}

variable "data_ingestion_bucket" {
  type    = string
  default = ""
}

variable "data_processed_bucket" {
  type    = string
  default = ""
}

variable "lambda_code_bucket" {
  type    = string
  default = ""
}

variable "lambda_1_name" {
  type    = string
  default = "raw_data_to_ingestion_bucket"
}

variable "lambda_2_name" {
  type    = string
  default = "ingestion_to_processed_bucket"
}

variable "lambda_3_name" {
  type    = string
  default = "processed_bucket_to_warehouse"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}