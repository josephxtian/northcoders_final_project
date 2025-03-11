import boto3
import os
from utils.get_bucket import get_bucket_name
import json
from datetime import datetime
from pprint import pprint

bucket_name = get_bucket_name("ingestion-bucket")

print(f"✅ Retrieved bucket name: {bucket_name}")

S3_BUCKET = os.getenv("S3_BUCKET", bucket_name)

def read_files_from_s3(bucket_name, client=None):
    tables=["address", "counterparty", "currency","department", "design","payment", "payment_type", "purchase_order",
             "sales_order", "staff", "transaction",
        ]
    if client is None:
        client = boto3.client("s3") 
    try:
        all_data = {} 
        for table in tables:
            try:
                response = client.list_objects_v2(Prefix=table ,Bucket=bucket_name,MaxKeys=50)
                #lists the next 50 files to process, from after the last file processed
                if "Contents" not in response:
                    print(f"No files found in {bucket_name}")
                table_data = []
                print(response["Contents"])
                for object in response["Contents"]:
                    
                    file_key = object["Key"] 
                    #pprint(file_key)
                    file_response = client.get_object(Bucket=bucket_name, Key=file_key)
                
                    if "Body" not in file_response:
                        print(f"⚠️ Skipping {file_key} (no Body in response)")
                        continue
                    #read and load the file into json
                    file_data = file_response["Body"].read().decode("utf-8")
                    file_data = json.loads(file_data)
                    #if table_data exists, add the dict entries to the list of the associated table, if it doesn't create a key value pair
                    for dict in file_data:
                        table_data.append(dict)
                    #assign the file to the file last processed 
                    last_file_processed = file_key
                all_data[table] = table_data  #append the table_data to the list of all data
            except:
                print(f"No files to process from {table}")
                continue
            #create/overwrite the txt file that had the last_processed date for that table
            client.put_object(
                            Bucket=bucket_name, Key=f'last_processed/{table}.txt',
                            Body=last_file_processed
                        )  
        print(" Successfully retrieved all files.")
        #pprint(all_data)
        return all_data  

    except Exception as e:
        print(f" Error reading files from {bucket_name}: {e}")
        return {"error": str(e)}    

# if __name__ == "__main__":
    
# start_time = datetime.now()
# print(start_time)
# all_files_data = read_files_from_s3(S3_BUCKET)
# end_time = datetime.now()
# print(end_time)
# for file_name, content in all_files_data.items():
#     print(f"\n File: {file_name}\n{content}")