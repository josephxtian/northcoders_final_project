from src.create_dim_tables import set_up_dims_table, put_info_into_dims_schema
import pg8000.native
from src.connection import connect_to_db, close_db_connection
from src.create_temporary_tables import make_temporary_tables
import pytest
from unittest import mock
from ingestion_to_processed_bucket.dim_date_function import extract_date_info_from_dim_date
from src.get_currency_name import get_currency_details

# Table headers defined at bottom of document

test_db = connect_to_db()
result = test_db.run("SELECT current_database();")
print(f"Raw query result : {result}")
database_name = result[0][0]
print(f"Connected to database: {database_name}")

@pytest.fixture 
def date_id():
    return "2022-11-03T14:20:51.563000"

@pytest.fixture
def currency_id():
    return 1

@pytest.fixture
def mock_db_connection():
    mock_db = mock.MagicMock()
    return mock_db


class TestSetUpDimsTable:
    #test 6 tables have been created
    def test_six_tables_created(self):
        test_db = connect_to_db()
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        set_up_dims_table(test_db,test_input)

        print(dim_tables_column_headers, 'huna<<<<<<<<<<<<<<<<<')

        for table in dim_tables_column_headers:
            print(f"Querying table: {table}")
            test_db.run(f"SELECT * FROM {table};")
            column_headers = [c['name'] for c in test_db.columns]
            assert column_headers == dim_tables_column_headers[table]
        close_db_connection(test_db)

    def test_tables_are_empty(self):
        test_db = connect_to_db()
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        set_up_dims_table(test_db,test_input)

        for table in dim_tables_column_headers:
            result = test_db.run(f"SELECT * FROM {table};")
            assert result == []
        close_db_connection(test_db)


class TestPutInformationIntoDimsSchema():
    # test 
 
    def test_insertion_for_one_table(self, date_id, currency_id):
        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 2,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"}],
         "department": [
        {"department_id": 1,
         "department_name": "Sales",
         "location": "Manchester",
         "manager": "Richard Roma",
         "created_at": "2022-11-03T14:20:49.962000",
         "last_updated": "2022-11-03T14:20:49.962000"
        }]}

        test_db = connect_to_db()

        func_result = make_temporary_tables(test_db, test_input)
        table_names = func_result[0]
        set_up_dims_table(test_db, table_names)

        result = put_info_into_dims_schema(test_db, table_names, date_id, currency_id)
        print(result)

        assert result == {}

        for table in test_input:
            query = f"SELECT * FROM {table};"
            query_result = test_db.run(query)
            assert len(query_result) == len(test_input[table])
        close_db_connection(test_db)


# Mocked database connection and relevant inputs
@pytest.fixture
def mock_db_connection():
    mock_db = mock.MagicMock()
    return mock_db


def test_process_dim_date(mock_db_connection):
    # Test case for 'dim_date'
    
    # Mock the function 'extract_date_info_from_dim_date' to return dummy data
    with mock.patch('ingestion_to_processed_bucket.dim_date_function.extract_date_info_from_dim_date') as mock_extract_date:
        mock_extract_date.return_value = {"year": 2022, "month": 11, "day": 3}  # mock output

        # Call the method to test
        date_str = '2022-11-03T14:20:51.563000'  # Example date string
        dim_tables_created = ["dim_date", "dim_currency"]  # Only test for 'dim_date' here
        currency_id = 1  # dummy currency_id
        
        # Call the function you want to test (you'll have to pass this to your actual function)
        result = put_info_into_dims_schema(mock_db_connection, dim_tables_created, date_str, currency_id)

        # Check if the 'extract_date_info_from_dim_date' function was called with the correct argument
        mock_extract_date.assert_called_once_with(date_str)

        # Test that the correct result is inserted into 'dim_date' (i.e., dimension_value_rows contains data for 'dim_date')
        assert 'dim_date' in result
        assert result['dim_date'] == mock_extract_date.return_value
        print(f"Test Passed for dim_date: {result}")


def test_process_dim_currency(mock_db_connection):
    # Test case for 'dim_currency'
    
    # Mock the function 'get_currency_details' to return dummy data
    with mock.patch('src.get_currency_name.get_currency_details') as mock_get_currency:
        mock_get_currency.return_value = {"currency_code": "USD", "currency_name": "US Dollar"}  # mock output

        # Call the method to test
        date_str = '2022-11-03T14:20:51.563000'  # Example date string
        dim_tables_created = ["dim_currency", "dim_date"]  # Only test for 'dim_currency' here
        currency_id = 1  # dummy currency_id
        
        # Call the function you want to test (you'll have to pass this to your actual function)
        result = put_info_into_dims_schema(mock_db_connection, dim_tables_created, date_str, currency_id)

        # Check if the 'get_currency_details' function was called with the correct argument
        mock_get_currency.assert_called_once_with(currency_id)

        # Test that the correct result is inserted into 'dim_currency' (i.e., dimension_value_rows contains data for 'dim_currency')
        assert 'dim_currency' in result
        assert result['dim_currency'] == mock_get_currency.return_value
        print(f"Test Passed for dim_currency: {result}")


# Table headers
dim_tables_column_headers = {"dim_date":
  ["date_id",
  "year",
  "month",
  "day",
  "day_of_week",
  "day_name",
  "month_name",
  "quarter"],

  "dim_staff":
  ["staff_id",
  "first_name",
  "last_name" ,
  "department_name",
  "location",
  "email_address"],

  "dim_location":
  ["location_id",
  "address_line_1",
  "address_line_2",
  "district",
  "city",
  "postal_code",
  "country",
  "phone"],

  "dim_currency":
  ["currency_id",
  "currency_code",
  "currency_name"],


  "dim_design":
  ["design_id",
  "design_name",
  "file_location",
  "file_name"],

  "dim_counterparty":
  ["counterparty_id",
  "counterparty_legal_name",
  "counterparty_legal_address_line_1",
  "counterparty_legal_address_line_2",
  "counterparty_legal_district",
  "counterparty_legal_city",
  "counterparty_legal_postal_code",
  "counterparty_legal_country",
  "counterparty_legal_phone_number"]
}