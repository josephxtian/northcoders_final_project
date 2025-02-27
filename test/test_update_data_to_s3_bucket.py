from src.update_data_to_s3_bucket import update_data_to_s3_bucket
from src.upload_to_s3_bucket import write_to_s3_bucket, the_list_of_tables
import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch
from pprint import pprint
import json
from datetime import datetime

@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestUploadsDataWithTimeStamp:
    
    def test_retreives_list_of_files(self):
        with mock_aws():
            def test_list():
                return ["address"]
            def test_func(test_list):
                return [{'address_id': 1,'address_line_1': '6826 Herzog Via', 'last_updated': '2025-02-27 9:13:32' },
                        {'address_id': 2,'address_line_1': '6827 Herzog Via', 'last_updated': '2025-02-27 10:13:32' },
                        {'address_id': 3,'address_line_1': '6828 Herzog Via', 'last_updated': '2025-02-27 12:13:32' }]
            
            bucket_name = 'test_bucket'
            object_key1 = "address/seed.json"
            object_key2 = "address/2025-02-27 12:13:32.json"
            file1 = '[{"address_id": 1,"address_line_1": "6826 Herzog Via", "last_updated": "2025-02-27 9:13:32"}]'
            file2 = '[{"address_id": 2,"address_line_1": "6827 Herzog Via", "last_updated": "2025-02-27 10:13:32"}]'
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            s3_client.put_object(Bucket=bucket_name,Key=object_key1,Body=file1)
            s3_client.put_object(Bucket=bucket_name,Key=object_key2,Body=file2)
            update_data_to_s3_bucket(s3_client, bucket_name, test_list, test_func)
            data_on_files_from_s3 = s3_client.list_objects_v2(Bucket= bucket_name, Prefix="address")
            assert data_on_files_from_s3['KeyCount'] ==3
            
            data_from_s3_bucket = []
            for i in range(len(data_on_files_from_s3['Contents'])):
                added_file = data_on_files_from_s3["Contents"][i]["Key"]
                file_data = s3_client.get_object(Bucket=bucket_name, Key=added_file)
                data = file_data['Body'].read().decode('utf-8')
                file_content = json.loads(data)
                for dict in file_content:
                    data_from_s3_bucket.append(dict)
            
            assert sorted(data_from_s3_bucket, key=lambda d: d["address_id"]) == \
                sorted(test_func(test_list), key=lambda d: d["address_id"])
            objs = data_on_files_from_s3['Contents']
            get_last_modified = get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
            added_file = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)][1]
            current_timestamp = datetime.now()
            formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            assert added_file == f"address/{formatted_timestamp}.json"