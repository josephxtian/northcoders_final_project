from db_setup.connection import connect_db, close_db
import pytest
from pg8000 import Connection


@pytest.fixture()
def db():
    db = connect_db()
    yield db
    close_db(db)


def test_connects_to_database():
    conn = connect_db()
    assert isinstance(conn, Connection)
    close_db(conn)

