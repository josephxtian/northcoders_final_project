from db_setup.connection import connect_db, close_db
import pytest
from pg8000.native import Connection


@pytest.fixture()
def db():
    db = connect_db()
    yield db
    close_db(db)


def test_connects_to_database():
    db = connect_db()
    assert isinstance(db, Connection)
    close_db(db)

