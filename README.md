Northcoders Final project February/March 2025
==========

# Project Introduction
This project aims to take information from a tote bag store's database system, format it and organise it into a data warehouse. A dashboard will then use the information from the data warehouse to create summaries to aid business insights.

# Docs
## Terraform
Terraform is used to set up and configure the bulk of AWS infrastructure. All terraform files are stored in the terraform/ directory.

To build infrastructure

    `terraform init`
    `terraform plan`
    `terraform apply`

### Files
#### main.tf
initialises the terraform workspace and updates the backend file stored manually in the s3 bucket `tf-state-bucket-nc-project-352446`.

#### cloudwatch.tf
#### data.tf
sets up some commonly used data variables.

#### eventbridge.tf

#### iam.tf
creates IAM roles and permissions for lambda, cloudwatch logging, event bridge and statefunction.

#### lambda.tf
creates three lambda functions and uploads the default test file `index.py` from each of the folders: `raw_data_to_ingestion_bucket`, `ingestion_to_processed_bucket`, `processed_bucket_to_warehouse`

Note the lambda file automatically zips the python code into the folder `python_zips` which is then uploaded to the `lambda-code-store-bucket` s3 bucket for execution via lambda.

#### s3.tf
creates three s3 buckets 
`ingestion-bucket` - stores data brought in from raw database with lambda function 1.

`processed-bucket` - stores data which has beeen processed by lambda function 2

`lambda-code-store-bucket` - stores all lambda code executed on AWS.

#### secretsmanager.tf

#### stepfunction.tf
creates a step function state machine for the lambda 2 workflow. Currently set up in test mode to execute a dummy lambda 2 code. The stepfunction is built from the `pipeline.json` file stored in the terraform directory. To update `pipeline.json` use the code view created in the console based aws statemachine setup and paste it into the file.

#### vars.tf
directory of variables. Includes python runtime and naming conventions.