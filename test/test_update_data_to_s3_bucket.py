from src.update_data_to_s3_bucket import update_data_to_s3_bucket
from src.upload_to_s3_bucket import write_to_s3_bucket
from utils.utils_for_ingestion import list_of_tables, get_file_contents_of_last_uploaded,\
reformat_data_to_json
import uuid
import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch, Mock
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
    def test_contents_of_file_test_data_in_s3(self):
        with mock_aws():
            bucket_name = f"test-bucket-{uuid.uuid4().hex}"
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            
            def mock_list_of_tables():
                return["counterparty"]
            mock_reformated_data_from_db = Mock()

            with open('test/test_data/test_data.json','r') as f:
                data = json.load(f)
            mock_reformated_data_from_db = Mock(return_value=data["counterparty"])

            write_to_s3_bucket(s3_client, bucket_name, mock_list_of_tables, mock_reformated_data_from_db)
            
            
            no_of_files_before_update = s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount']
            additional_data = [{
            "counterparty_id": 21,
            "counterparty_legal_name": "Fahey and Sons",
            "legal_address_id": 15,
            "commercial_contact": "Micheal Toy",
            "delivery_contact": "Mrs. Lucy Runolfsdottir",
            "created_at": "2025-02-03T14:20:51.563000",
            "last_updated": "2025-02-03T14:20:51.563000"
            }]

            mock_additional_data_last_uploaded = Mock(return_value=[{
                "last_updated": "2025-02-01T14:20:51.563000"
            }])
          
            update_data_to_s3_bucket(s3_client, bucket_name, mock_list_of_tables, mock_additional_data_last_uploaded, 
                                mock_reformated_data_from_db)
            assert s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] == (no_of_files_before_update +1)
            
            
            data_from_s3_bucket = []
            json_file_on_s3 = s3_client.list_objects_v2(Bucket=bucket_name)
            
            for i in range(json_file_on_s3['KeyCount']):
                file_data_to_add = json_file_on_s3["Contents"][i]["Key"]
                file_data = s3_client.get_object(Bucket=bucket_name, Key=file_data_to_add)
                data = file_data['Body'].read().decode('utf-8')
                file_content = json.loads(data)
                for dict_item in file_content:
                    data_from_s3_bucket.append(dict_item)

            with open('test/test_data/test_data.json','r') as f:
                db_data = json.load(f)["counterparty"]

            db_data.append(additional_data[0])
            expected = db_data   
            
            assert sorted(data_from_s3_bucket, key=lambda d: d["counterparty_id"]) == \
                   sorted(expected, key=lambda d: d["counterparty_id"])
            txt_file = json_file_on_s3["Contents"][-1]["Key"]
            txt_file_data = s3_client.get_object(Bucket=bucket_name, Key=txt_file)
            json_file_last_updated = txt_file_data['Body'].read().decode('utf-8') 
            assert json_file_last_updated == 'counterparty/2025/2/3/14:20:51.563000.json'