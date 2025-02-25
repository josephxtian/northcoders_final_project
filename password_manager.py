import boto3
from botocore.exceptions import ClientError
import json


def get_secret(secret_name, region_name="eu-west-2", client=None):
    """Retrieve secrets from AWS Secrets Manager"""
    if client is None:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return f"Error retrieving secret: {e}"


if __name__ == "__main__":
    secret_name = "totesys/db_credentials"
    region_name = "eu-west-2"

    credentials = get_secret(secret_name, region_name)

    if not credentials:
        raise ValueError("Error: No credentials retrieved from AWS Secrets Manager!")

    try:
        db_username = credentials["user"]
        db_password = credentials["password"]
        db_host = credentials["host"]
        db_name = credentials["database"]
        db_port = credentials["port"]
    except KeyError as e:
        raise ValueError(f"Missing required credential: {e}")

    print(f"Successfully retrieved credentials for {db_name} at {db_host}:{db_port}")



# if secret_choice == "totesys":
#         try:
#             db_username = credentials["user"]
#             db_password = credentials["password"]
#             db_host = credentials["host"]
#             db_name = credentials["database"]
#             db_port = credentials["port"]
#             print(f"Successfully retrieved Totesys DB credentials: {db_name} at {db_host}:{db_port}")
#         except KeyError as e:
#             raise ValueError(f"Missing required DB credential: {e}")

#     elif secret_choice == "aws":
#         try:
#             aws_access_key = credentials["aws_access_key_id"]
#             aws_secret_key = credentials["aws_secret_access_key"]
#             aws_session_token = credentials.get("aws_session_token", None)  # Optional

#             session = boto3.Session(
#                 aws_access_key_id=aws_access_key,
#                 aws_secret_access_key=aws_secret_key,
#                 aws_session_token=aws_session_token,
#                 region_name="eu-west-2"
#             )
        


