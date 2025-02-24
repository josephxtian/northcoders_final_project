variable "data_ingress_bucket" {
  type    = string
  default = ""
}


variable "data_process_bucket" {
  type    = string
  default = ""
}

variable "lambda_name" {
  type    = string
  default = ""
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}