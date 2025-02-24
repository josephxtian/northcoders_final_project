# Northcoders_final_project_25
Northcoders grad project 

8 Steps to success

1. A job scheduler or orchestration process to run the ingestion job and subsequent processes.
2. An S3 bucket that will act as a "landing zone" for ingested data.
    1. Set up IAM user
    2. Set up IAM policies
    3. Create Ingestion Bucket
    4. Put data in Bucket
3. A Python application to check for changes to the database tables and ingest any new or updated data.
    1. Write python code
    2. upload to lambda
    3. Give lambda IAM permissions to s3 bucket
4. A CloudWatch alert should be generated in the event of a major error - this should be sent to email.
    1. enable logging for lambda
    2. set up CloudWatch alarm to trigger email
5. A second S3 bucket for "processed" data.
    1. Create processed Bucket in parquet format
    2. Put data in Bucket
6. A Python application to transform data landing in the "ingestion" S3 bucket and place the results in the "processed" S3 bucket.
    1. Write python code
    2. upload to lambda
    3. Give lambda IAM permissions to s3 bucket
7. A Python application that will periodically schedule an update of the data warehouse from the data in S3.
    1. Write python code
    2. upload to lambda - set to trigger automatically
    3. log processes and send an email
8. Business Insight Dashboard - Analytics phase
    1. Power BI? / SQL

