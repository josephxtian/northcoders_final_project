from src.upload_to_s3_bucket import write_to_s3_bucket, the_list_of_tables
from src.converts_data_to_json import table_reformat
import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch
from pprint import pprint
import re
import json


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
            res = write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables)
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
            res = write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables)

        assert res == None

    def test_creates_correct_no_of_files_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            res = write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables)
            print(res)
            assert s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] ==11
            assert res == 'success - 11 files have been written to test_bucket!'

class TestFileContent:
    def test_contents_of_file_design_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
    
            def the_list_of_tables():
                return ["design"]
            
            with patch("src.upload_to_s3_bucket.the_list_of_tables") as mock_choice:
                mock_choice.return_value = the_list_of_tables
                
                write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables)
            word = "design/seed.json"
            res = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=word)
        
            data = res['Body'].read().decode('utf-8') 
            file_content = json.loads(data)
            with open("json_data/json-design.json", 'r') as file:
                expected = json.load(file)["design"]

                # pprint(expected)
                pprint(file_content)
            assert file_content == expected

    def test_contents_of_all_files__in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            
            table_reformat()
            write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables)

            for table in the_list_of_tables():

                word = f"{table}/seed.json"
                res = s3_client.get_object(
                        Bucket=bucket_name,
                        Key=word)
            
                data = res['Body'].read().decode('utf-8') 
                file_content = json.loads(data)
                with open(f"json_data/json-{table}.json", 'r') as file:
                    expected = json.load(file)[f"{table}"]
                   
                assert file_content == expected
           
           