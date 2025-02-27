from src.store_secret import store_secret
import boto3
from unittest.mock import Mock
import os
import pytest
from moto import mock_aws
from pprint import pprint
import json

@pytest.fixture(scope="function")
def mock_secrets_manager_client():
    with mock_aws():
        client = boto3.client("secretsmanager", "eu-west-2")
        yield client

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# test 1 - secret successfully stored

def test_secret_success(mock_secrets_manager_client, aws_credentials):
    secret_name = "test_secrets1"
    aws_access_key_id = "test1"
    aws_secret_access_key = "password"

    response = store_secret(mock_secrets_manager_client, secret_name, aws_access_key_id, aws_secret_access_key)

    assert response == f"Secret {secret_name} saved"

    stored_secret = mock_secrets_manager_client.get_secret_value(
        SecretId=secret_name
    )
    secret_data = json.loads(stored_secret["SecretString"])

    assert secret_data["aws_access_key_id"] == aws_access_key_id
    assert secret_data["aws_secret_access_key"] == aws_secret_access_key

# test 2 - error is returned when secret data incorrect
def test_for_exception_when_input_incorrect(mock_secrets_manager_client, aws_credentials):
    secret_name = ""
    aws_access_key_id = "test1"
    aws_secret_access_key = "password"

    response = store_secret(mock_secrets_manager_client, secret_name, aws_access_key_id, aws_secret_access_key)

    assert "Error storing secret" in response
