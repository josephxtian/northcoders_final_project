from boto3 import client
from botocore.exceptions import ClientError
from src.converts_data_to_json import reformat_data_to_json
from datetime import datetime 
import json 

def the_list_of_tables():
    return [
        "address",
        "staff",
        "department",
        "design",
        "counterparty",
        "sales_order",
        "transaction",
        "payment",
        "purchase_order",
        "payment_type",
        "currency",
    ]

def write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables):
    list_of_tables = the_list_of_tables()
    try:

        if s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] ==0:
            count = 0
            for table in list_of_tables:
                current_timestamp = datetime.now()
                formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                object_key = f"{table}/seed.json"
                json_formatted_data_to_upload = reformat_data_to_json(table)
                s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(json_formatted_data_to_upload))
                count+=1
                #s3_client.upload_file(file, bucket_name , object_key)
            
            return f"success - {count} files have been written to {bucket_name}!"

    except ClientError:
        return {'result': 'FAILURE' ,"message":"file could not be uploaded"}