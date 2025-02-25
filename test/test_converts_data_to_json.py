from src.converts_data_to_json import reformat_data_to_json
from src.connection import connect_to_db, close_db_connection
from pg8000.native import literal, identifier
import datetime
import pytest

@pytest.fixture(autouse=True)
def db():
    db = connect_to_db()
    yield db
    close_db_connection(db)


class TestDataIntegrity:
    def test_retreval_of_data(self,db):
        db.run("SELECT * FROM ADDRESS WHERE address_id = 1")
        result = reformat_data_to_json("address")[0]
        assert result['city'] == 'New Patienceburgh'
        assert result['country'] == 'Turkey'
        assert result['district'] == 'Avon'
        
    def test_number_of_returns_is_correct(self,db):
        list_of_tables = ['address', 'staff', 'department', 'design', 
                    'counterparty', 'sales_order', 'transaction', 'payment',
                    'purchase_order', 'payment_type', 'currency']
        
        for table in list_of_tables:
            expected = db.run(f"SELECT * FROM {identifier(table)}")
            result = reformat_data_to_json(table)
            assert len(result) == len(expected)     
        
    