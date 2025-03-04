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
creates a log group allowing lambda and eventbridge to log to cloudwatch
send emails for lambda errors and step function failures to Ben's email (for now)

#### data.tf
sets up some commonly used data variables.

#### eventbridge.tf
attachs a schedule to invoke the step function to feed into lambda 2 every 5 minutes - lambda 2 can then write to
`nc-project-schedule`
ingestion bucket if any changes made in rds
attaches iam policy to evenbtridge that allows lambda to be invoked

#### iam.tf
creates IAM roles and permissions for lambda, cloudwatch logging, event bridge and statefunction.

#### lambda.tf
creates three lambda functions and uploads the default test file `index.py` from each of the folders: `raw_data_to_ingestion_bucket`, `ingestion_to_processed_bucket`, `processed_bucket_to_warehouse`

Note the lambda file automatically zips the python code into the folder `python_zips` which is then uploaded to the `lambda-code-store-bucket` s3 bucket for execution via lambda.

#### s3.tf
creates three s3 buckets 

`ingestion-bucket<random_string>` - stores data brought in from raw database with lambda function 1.

`processed-bucket<random_string>` - stores data which has beeen processed by lambda function 2

`lambda-code-store-bucket<random_string>` - stores all lambda code executed on AWS.

#### secretsmanager.tf
gets the database credentials from AWS secrets manager and stores as a local variable to be used in terraform.

Has policy for read access for secrets manager and attaches the policy to the role.

#### stepfunction.tf
creates a step function state machine for the lambda 2 workflow. Currently set up in test mode to execute a dummy lambda 2 code. The stepfunction is built from the `pipeline.json` file stored in the terraform directory. To update `pipeline.json` use the code view created in the console based aws statemachine setup and paste it into the file.

#### vars.tf
directory of variables. Includes python runtime and naming conventions



#### s3_read_function.py

Reads data from the ingress bucket and sets as a variable for use in star schema. Currently a test-bucket because lambda 1 is not finalised.

#### password_manager.py

Can be deleted if all is running fine in terraform and buckets

### connection.py
creates connection to the database.

###  raw_data_to_ingestions_bucket.py

*`The get_secret():`* function retreieves database credentials from AWS secrets manager and the credentials are used to establish a connection to the database.

*`The lambda_handler(event, context):`* function serves as the entry point for AWS Lambda. It retrieves the credentials, establishes a database connection, calls functions to fetch and process the data, and uploads it to S3.A success message is returned after completing the process.

within the function it queries predefined list of database tables. *`list_of_tables():`* 
*`get_file_contents_of_last_uploaded():`* checks for the last_updated timestamp later than the most recently uploaded data (tracked via S3). For each table, it retrieves any new or updated data since the last upload and the *`reformat_data_to_json():`* function is used to reformat the data into a JSON format. 

The function reprocesses the data into a list of dictionaries, where each dictionary represents a row with column names as keys.
Timestamps are converted into a consistent format, and decimals are cast to float.

*`update_data_to_s3_bucket():`* function is used for each table, if new or updated data is found, it is uploaded to an S3 bucket.The data is organized in a folder structure based on the table name and the date/time of the upload. A *`last_updated.txt`* file is updated in each table folder to store the key of the most recently uploaded data.

### Json_data folder
This folder contains all the raw data in json format. Each table is in it's own file within the folder.

### updload_to_s3_bucket.py
*`write_to_s3_bucket():`* function is used to upload the data from the list of tables in a postrgeSQL database to an s3 bucket. It connects to the database, then uses the *`list_of_tables()`* function to fetch the list of tables from the database. 
SQL is used to fetch all the data from all the tables.
 *`reformat_data_to_json():`* is called to reformat the data into json files before uploading the data into the s3 bucket. The data is organised by year, month, day and time in the object keys.
 If an error occurs during the upload process, the function handles the error and returns an appropriate message.

### update_to_s3_bucket.py
*`get_file_contents_of_last_uploaded():`* function is called to check the most recent uploaded data for each of the tables in the s3 bucket. SQL is used to query the database for records with a last_updated timestamp greater than the most recent timestamp stored in S3. 
 *`reformat_data_to_json():`* is called to convert the database rows into Json files. If new or updated data exists, it uploads the data to the S3 bucket, organizing it by timestamp and creating the appropriate object keys. After each upload, it updates the *`last_updated.txt`* file in the S3 bucket to store the path to the latest uploaded data for each table.

### fact_sales_order.py
* create_fact_sales_order_table(): * function is called to create the fact_sales_order table and adds foreign key constraints that adhers to key relationships in raw and dim tables.
* transform_fact_data(): * function that transforms raw data from s3 ingestion bucket into star schema format. Pandas used to create a staging table to contain column values found in raw json data. SQL query is then executed which adds the data to the final fact_sales_order table

### write_schema_to_processed.py
* write_schema_to_processed(): * function currently uses hardcoded bucket name for processed s3 bucket. This will likely need changing after each terraform destroy. Function to dynamically fetch bucket based in prefix to be substituted. Pandas is used to create a DataFrame that can then be converted into parquet format. Boto3 then used to write to a file with a dynamic name depending on source_file metadata from respective file in s3 ingestion bucket.



