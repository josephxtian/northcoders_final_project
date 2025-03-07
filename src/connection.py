from pg8000.native import Connection
from password_manager.password_manager import get_db_credentials
import boto3
import os
from dotenv import load_dotenv

credentials = get_db_credentials(region_name="eu-west-2")
for key, value in credentials.items():os.environ[key] = str(value)
load_dotenv()

def connect_to_db():
    return Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT")),
    )


def close_db_connection(conn):
    conn.close()

conn = connect_to_db()
close_db_connection(conn)