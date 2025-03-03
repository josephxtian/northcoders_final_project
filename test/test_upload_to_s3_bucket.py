from src.upload_to_s3_bucket import write_to_s3_bucket
from src.connection import connect_to_db, close_db_connection
from utils.utils_for_ingestion import reformat_data_to_json, list_of_tables
import pytest
from unittest.mock import patch, Mock
import os
import boto3
from moto import mock_aws
from pprint import pprint
import re
import json
from pprint import pprint

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def bucket(s3):
    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

class TestWriteToFile:
    def test_return_error_when_no_bucket(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket="wrong_bucket",
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            res = write_to_s3_bucket(s3_client, bucket_name, list_of_tables, reformat_data_to_json)
        assert res["result"] == "FAILURE"
        assert res["message"] == "file could not be uploaded"

    def test_does_not_run_with_bucket_has_contents(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            file = "hello"
            object_key = "test.json"
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=file)
            res = write_to_s3_bucket(s3_client, bucket_name, list_of_tables, reformat_data_to_json)

        assert res == None

    def test_creates_correct_no_of_files_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            def list_of_tables():
                return["counterparty"]
            mock_reformated_data_from_db = Mock()
            with open('test/test_data/test_data.json','r') as f:
                data = json.load(f)
            mock_reformated_data_from_db.return_value = data["counterparty"]
            res = write_to_s3_bucket(s3_client, bucket_name, list_of_tables, mock_reformated_data_from_db)
            
            assert s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] ==5  #note - this is 4 json files and one txt
            assert res == 'success - 1 database tables have been written to test_bucket!'

class TestFileContent:
    def test_contents_of_file_test_data_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
    
            def list_of_tables():
                return["counterparty"]
            mock_reformated_data_from_db = Mock()
            with open('test/test_data/test_data.json','r') as f:
                data = json.load(f)
            mock_reformated_data_from_db.return_value = data["counterparty"]
            write_to_s3_bucket(s3_client, bucket_name, list_of_tables, mock_reformated_data_from_db)
            list_of_data_in_s3 = []
            for i in range(s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount']-1):  #not the last file as that is the txt
                file_name = s3_client.list_objects_v2(Bucket=bucket_name)["Contents"][i]["Key"]
                res = s3_client.get_object(Bucket=bucket_name,Key=file_name)
                data = res['Body'].read().decode('utf-8') 
                file_content = json.loads(data)
                list_of_data_in_s3.append(file_content)
            result = [data for lists in list_of_data_in_s3 for data in lists]
            print(result)
            with open('test/test_data/test_data.json','r') as f:
                expected = json.load(f)["counterparty"]

            assert result == expected

           
           