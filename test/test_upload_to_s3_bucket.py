from src.upload_to_s3_bucket import write_to_s3_bucket
import pytest
import os
import boto3
from moto import mock_aws


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
            res = write_to_s3_bucket(s3_client,bucket_name)
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
            res = write_to_s3_bucket(s3_client,bucket_name)

        assert res == None

    def test_creates_correct_no_of_files_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client =boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            res = write_to_s3_bucket(s3_client,bucket_name)
            print(res)
            assert s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] ==11
            assert res == 'success - 11 files have been written to test_bucket!'

    def test_contents_of_file_in_s3(self):
        with mock_aws():
            bucket_name = 'test_bucket'
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )