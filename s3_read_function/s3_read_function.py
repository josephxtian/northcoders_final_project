import boto3
import os
from utils.get_bucket import get_bucket_name
import pprint

bucket_name = get_bucket_name("ingestion-bucket")

print(f"✅ Retrieved bucket name: {bucket_name}")

S3_BUCKET = os.getenv("S3_BUCKET", bucket_name)

def read_files_from_s3(bucket_name, client=None):
    if client is None:
        client = boto3.client("s3") 

    try:
        response = client.list_objects_v2(Bucket=bucket_name,MaxKeys=50)

        if "Contents" not in response:
            print(f"No files found in {bucket_name}")

        all_data = {} 

        for object in response["Contents"]:
            file_key = object["Key"] 
            

            file_response = client.get_object(Bucket=bucket_name, Key=file_key)
            
            if "Body" not in file_response:
                print(f"⚠️ Skipping {file_key} (no Body in response)")
                continue
            
            file_data = file_response["Body"].read().decode("utf-8")
            

            all_data[file_key.split("/",1)[0]] = file_data
        
        print(" Successfully retrieved all files.")
        pprint.pprint(all_data)
        return all_data  

    except Exception as e:
        print(f" Error reading files from {bucket_name}: {e}")
        return {"error": str(e)}    

if __name__ == "__main__":
    all_files_data = read_files_from_s3(S3_BUCKET)
    
    for file_name, content in all_files_data.items():
        print(f"\n File: {file_name}\n{content}")