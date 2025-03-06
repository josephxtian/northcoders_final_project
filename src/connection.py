from pg8000.native import Connection
from password_manager import get_db_credentials
import os


def connect_to_db():
    # credentials = get_db_credentials(region_name="eu-west-2")
    # for key, value in credentials.items():os.environ[key] = str(value)

    return Connection(
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=os.environ["PG_DATABASE"],
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
    )


def close_db_connection(conn):
    conn.close()

conn = connect_to_db()
close_db_connection(conn)