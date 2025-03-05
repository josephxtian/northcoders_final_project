import boto3
from botocore.exceptions import ClientError
import json

def get_db_credentials(region_name="eu-west-2"):
    """Retrieve database credentials from AWS Secrets Manager"""
    secret_name = "totesys/db_credentials"
    client = boto3.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        db_credentials = json.loads(response["SecretString"])

        
        required_keys = {"user", "password", "host", "database", "port"}
        if not required_keys.issubset(db_credentials.keys()):
            missing_keys = required_keys - db_credentials.keys()
            raise KeyError(f"Missing required credentials: {missing_keys}")

        return db_credentials

    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return None


if __name__ == "__main__":
    credentials = get_db_credentials()

    if credentials is None:
        raise ValueError("Error: No credentials retrieved from AWS Secrets Manager!")

    db_username = credentials["user"]
    db_password = credentials["password"]
    db_host = credentials["host"]
    db_name = credentials["database"]
    db_port = credentials["port"]

    print(f"Successfully retrieved credentials for {db_name} at {db_host}:{db_port}")

#i then ran in terminal eval "$(python password_manager.py)" to 
# export the prints to avoid using vars for the password

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
        


