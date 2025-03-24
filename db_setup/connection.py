from pg8000.native import Connection
from dotenv import load_dotenv
import os

env_path = '.env.test' if os.environ.get("TESTING") == "True" else '.env.development'

load_dotenv(dotenv_path = env_path, override = True)


def connect_to_db():
    return Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )
    

def close_db(conn):
    conn.close()