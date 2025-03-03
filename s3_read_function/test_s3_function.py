import boto3
import pytest
from s3_read_function import read_files_from_s3
from moto import mock_aws  

class TestReadFromS3:
    @pytest.fixture
    def setup_s3(self):
        with mock_aws():  
            bucket_name = "test-bucket"
            s3_client = boto3.client("s3", region_name="eu-west-2")

            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            yield bucket_name, s3_client  

    def test_get_returns_all_files(self, setup_s3):
        bucket_name, s3_client = setup_s3

        
        file_contents = {
            "file_1.txt": "Hello, this is file 1",
            "file_2.txt": "This is file 2",
            "file_3.txt": "File 3 content here"
        }

        for filename, content in file_contents.items():
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=content)

        response = read_files_from_s3(bucket_name, client=s3_client)

        assert response == file_contents

    def test_get_returns_error_when_bucket_not_found(self):
        with mock_aws():  
            response = read_files_from_s3("invalid_bucket")
            assert "NoSuchBucket" in response["error"]

    
