import boto3
import os

S3_BUCKET = os.getenv("S3_BUCKET", "mock-ingestion-bucket")
S3_KEY = os.getenv("S3_KEY", "sample-data.json")


def read_file_from_s3(bucket_name, object_key, client=None):
    
    if client is None:
        s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=object_key
                )
        
        data = response["Body"].read().decode("utf-8")
        print("File Content:\n", data)
        return "File Content:\n", data
      
    except Exception as e:
        print(f"Error reading from {bucket_name}: {e}")