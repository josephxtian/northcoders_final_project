# import json
# import boto3
# import pytest
# from moto import mock_aws
# from botocore.exceptions import ClientError
# from password_manager import get_secret

# #mock credetnials for moto
# @pytest.fixture
# def aws_credentials():
#     return {
#         "aws_access_key_id": "test",
#         "aws_secret_access_key": "test",
#         "aws_session_token": "test",
#     }

# #test valid input retrieves secret
# @mock_aws
# def test_get_secret_successful():
#     client = boto3.client("secretsmanager")
    
#     secret_name = "totesys/db_credentials"
#     secret_value = {
#         "user": "test_user",
#         "password":"test_password",
#         "host": "test_host",
#         "database": "test_db",
#         "port": 5432
#     }

#     client.create_secret(
#         Name=secret_name,
#         SecretString = json.dumps(secret_value)
#     )

#     retrieved_secret = get_secret(secret_name, "eu-west-2")

#     assert retrieved_secret == secret_value

# #test get secret when the input doesn't match a secret stored
# @mock_aws
# def test_secret_input_not_found():
#     client = boto3.client("secretsmanager")
    
#     secret_name = "totesys/db_credentials"
#     secret_value = {
#         "user": "test_user",
#         "password":"test_password",
#         "host": "test_host",
#         "database": "test_db",
#         "port": 5432
#     }

#     client.create_secret(
#         Name=secret_name,
#         SecretString = json.dumps(secret_value)
#     )

#     retrieved_secret = get_secret("not_valid_secret", "eu-west-2")

#     assert "Error retrieving secret" in retrieved_secret

# #Test get secret when some keys are missing secret manager
# @mock_aws
# def test_get_secret_missing_key():
#     client = boto3.client("secretsmanager", region_name="eu-west-2")
#     secret_name = "totesys/db_credentials"

#     partial_secret_value = {
#         "user": "test_user",
#         "password": "test_pass",
#     }

#     client.create_secret(
#         Name=secret_name,
#         SecretString=json.dumps(partial_secret_value),
#     )

#     retrieved_secret = get_secret(secret_name, "eu-west-2")

#     assert retrieved_secret.get("user") == "test_user"
#     assert retrieved_secret.get("password") == "test_pass"
#     assert retrieved_secret.get("host", "default_host") == "default_host"
#     assert retrieved_secret.get("port", 5432) == 5432
#     assert retrieved_secret.get("database", "totesys") == "totesys"

# #tests an error is given when secrets manager is down
# @mock_aws
# def test_secrets_manager_unavailable():
#     client = boto3.client("secretsmanager", region_name="eu-west-2")

#     with pytest.raises(ClientError):
#         client.get_secret_value(SecretId="totesys/db_credentials")

# #test that it will not fail if the secret is empty 
# @mock_aws
# def test_empty_secret():
#     client = boto3.client("secretsmanager", region_name="eu-west-2")
#     secret_name = "totesys/db_credentials"

#     client.create_secret(
#         Name=secret_name,
#         SecretString=json.dumps({})
#     )

#     retrieved_secret = get_secret(secret_name, "eu-west-2")

#     assert retrieved_secret == {}
