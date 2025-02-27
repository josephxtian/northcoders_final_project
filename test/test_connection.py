from src.connection import connect_to_db, close_db_connection
import pytest
from pg8000.native import Connection


@pytest.fixture()
def db():
    db = connect_to_db()
    yield db
    close_db_connection(db)


def test_connects_to_database():
    conn = connect_to_db()
    assert isinstance(conn, Connection)
    close_db_connection(conn)


# def test_raises_error_if_can_not_connect():
#     try:
#         conn = connect_to_db()
#         assert isinstance(conn, Conection)
#         close_db_connection(conn)
#     except Exception as error:
#         print(f"Database is not connected error:{error}")
#         pytest.fail(f"Database is not connected error:{error}")
