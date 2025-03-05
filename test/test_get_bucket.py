from utils.get_bucket import get_bucket_name
import pytest
import boto3
from moto import mock_aws
import os


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def test_get_bucket_name_returns_bucket_name():
    with mock_aws():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="ingestion-bucket20250228065732358000000006",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="process-bucket20250228065732358000000006",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        assert (
            get_bucket_name("ingestion-bucket")
            == "ingestion-bucket20250228065732358000000006"
        )


def test_get_bucket_returns_error_if_no_bucket_found():
    with mock_aws():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="ingestion-bucket20250228065732358000000006",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
    with pytest.raises(Exception):
        get_bucket_name("wrong_prefix")
