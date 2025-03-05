import boto3
import pytest
from s3_read_function import read_file_from_s3
from moto import mock_aws
import uuid

class TestReadFromS3:
    @pytest.fixture
    def setup_s3(self):
        bucket_name = f"test-bucket-{uuid.uuid4().hex}"
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        return bucket_name, s3_client

    def test_get_returns_successful_response_message(self,setup_s3):
        with mock_aws():
            bucket_name, s3_client = setup_s3

            test_data = "Hello, this is a test file"
            s3_client.put_object(
                Bucket=bucket_name,
                Key="file_1.txt", Body=test_data)

            response = read_file_from_s3(bucket_name, "file_1.txt", client=s3_client)

            assert response == "File Content: Hello, this is a test file"

    def test_get_returns_error_when_file_not_found(self, setup_s3):
        with mock_aws():
            bucket_name, s3_client = setup_s3

            response = read_file_from_s3(bucket_name, "non_existent_file.txt", client=s3_client)
            assert "Error reading from test-bucket" in response 

    def test_get_returns_error_when_bucket_not_found(self, setup_s3):
        with mock_aws():
            bucket_name, s3_client = setup_s3

            s3_client.put_object(Bucket=bucket_name, Key="file_1.txt", Body="Test content")

            invalid_client = boto3.client("s3", region_name="eu-west-2")

            response = read_file_from_s3("invalid_bucket", "file_1.txt", client=s3_client)
            assert "Error reading from test-bucket" in response 



    
