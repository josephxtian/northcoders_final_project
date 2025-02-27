from boto3 import client
from src.upload_to_s3_bucket import the_list_of_tables
from src.converts_data_to_json import reformat_data_to_json
from datetime import datetime 
import json

def update_data_to_s3_bucket(s3_client, bucket_name, the_list_of_tables,reformat_data_to_json):
    
    
    for table in the_list_of_tables():
        current_operational_data = reformat_data_to_json(table)
        no_of_entries_in_operational_database = len(current_operational_data)
        no_of_entries_in_s3 = 0
        data_on_files_from_s3 = s3_client.list_objects_v2(Bucket= bucket_name, Prefix=f'{table}')
        list_of_files =[]
        for i in range(len(data_on_files_from_s3['Contents'])):
            list_of_files.append(data_on_files_from_s3['Contents'][i]['Key'])


        for file in list_of_files:
            file_data = s3_client.get_object(Bucket=bucket_name, Key=file)
            data = file_data['Body'].read().decode('utf-8') 
            file_content = json.loads(data)
            no_of_entries_in_s3 += len(file_content)
        
        additional_entries = no_of_entries_in_operational_database - no_of_entries_in_s3
        
        if additional_entries:
            additional_data = current_operational_data[-additional_entries:]
            current_timestamp = datetime.now()
            formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            object_key = f"{table}/{formatted_timestamp}.json"
            s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(additional_data))

    return f"data has been added to {bucket_name}"


