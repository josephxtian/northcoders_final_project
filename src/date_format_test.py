from boto3 import client
from src.upload_to_s3_bucket import the_list_of_tables
from src.converts_data_to_json import reformat_data_to_json
from datetime import datetime, date
import json
from pprint import pprint

def date():
    last_updated_date = datetime.strptime("2022-11-03T14:20:52.186000", '%Y-%m-%dT%H:%M:%S.%f')
    
    print(last_updated_date)

date()

def update_data_to_s3_bucket(s3_client, bucket_name, the_list_of_tables,reformat_data_to_json):
    
    for table in the_list_of_tables():
        current_operational_data = reformat_data_to_json(table)
        data_on_files_from_s3 = s3_client.list_objects_v2(Bucket= bucket_name, Prefix=f'{table}')
        list_of_files =[]
        
        # instead think we should get last modified to get the latest last_updated date then just look in that one file
        for i in range(len(data_on_files_from_s3['Contents'])):
            list_of_files.append(data_on_files_from_s3['Contents'][i]['Key'])

        #looping over the files in the s3 bucket 
        last_updated_date = datetime.strptime('2022-11-03T14:20:52.186000', '%Y-%m-%dT%H:%M:%S.%f')
        for file in list_of_files:
            file_data = s3_client.get_object(Bucket=bucket_name, Key=file)
            data = file_data['Body'].read().decode('utf-8') 
            file_content = json.loads(data)

            #prints out twice from here
            #looping through the contents of files in s3, to check last_updated datestamp
            for content in file_content:
                date_str = content['last_updated']
                last_updated_as_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
                if last_updated_as_date > last_updated_date:
                    last_updated_date = last_updated_as_date
            # checking for content in correct format to check which date is newer(bigger)
            #looping through operational data to add files datestamped to list 
        additional_entries = []
        for data in current_operational_data:
            data_str = data['last_updated']
            data_last_update = datetime.strptime(data_str, '%Y-%m-%dT%H:%M:%S.%f')
            if data_last_update > last_updated_date:
                additional_entries.append(data)
                print(additional_entries)


        # checking if there's addtional entries to add, a file is created with datestamp
        if additional_entries:
            current_timestamp = datetime.now()
            formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            object_key = f"{table}/{formatted_timestamp}.json"
            s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(additional_entries))
            print("update!")
        return f"data has been added to {bucket_name}"
    print("hello")
    return "no updates"

update_data_to_s3_bucket(client("s3"), 'ingestion-bucket20250228065732358000000006', the_list_of_tables,reformat_data_to_json)