import boto3
import pytest
from s3_read_function import read_file_from_s3
from moto import mock_aws

class TestReadFromS3:
    def test_get_returns_successful_response_message(self):
        with mock_aws():

            bucket_name = "test-bucket"
            s3_client = boto3.client("s3", region_name="eu-west-2")

            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )

            test_data = "Hello, this is a test file"
            s3_client.put_object(
                Bucket=bucket_name,
                Key="file_1.txt", Body=test_data)

            response = read_file_from_s3(bucket_name, "file_1.txt", client=s3_client)

            assert response == "File Content: Hello, this is a test file"

