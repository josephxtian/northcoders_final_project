Northcoders Final project February/March 2025
==========

# Project Introduction
This project aims to take information from a tote bag store's database system, format it and organise it into a data warehouse. A dashboard will then use the information from the data warehouse to create summaries to aid business insights.

# Docs
## Terraform
Terraform is used to set up and configure the bulk of AWS infrastructure. All terraform files are stored in the terraform/ directory.

### Files
#### main.tf
initialises the terraform workspace and updates the backend file stored manually in the s3 bucket <code>tf-state-bucket-nc-project-352446</code>.

#### cloudwatch.tf
#### data.tf
sets up some commonly used data variables.

#### eventbridge.tf

#### iam.tf
creates IAM roles and permissions for lambda, cloudwatch logging, event bridge and statefunction.

#### lambda.tf
creates three lambda functions and uploads the default test file <code>index.py</code> from each of the folders: <code>raw_data_to_ingestion_bucket</code>, <code>ingestion_to_processed_bucket</code>, <code>processed_bucket_to_warehouse</code>

Note the lambda file automatically zips the python code into the folder <code>python_zips</code> which is then uploaded to the <code>lambda-code-store-bucket</code> s3 bucket for execution via lambda.

#### s3.tf
creates three s3 buckets 
<code>ingestion-bucket</code> - stores data brought in from raw database with lambda function 1.

<code>processed-bucket</code> - stores data which has beeen processed by lambda function 2

<code>lambda-code-store-bucket</code> - stores all lambda code executed on AWS.

#### secretsmanager.tf

#### stepfunction.tf
creates a step function state machine for the lambda 2 workflow. Currently set up in test mode to execute a dummy lambda 2 code. The stepfunction is built from the <code>pipeline.json</code> file stored in the terraform directory.

#### vars.tf
directory of variables. Includes python runtime and naming conventions.