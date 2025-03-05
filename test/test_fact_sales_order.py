import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import StringIO
from src.fact_sales_order import transform_fact_data, create_fact_sales_order_table

TEST_JSON_DATA = [
    {
        "sales_order_id": 1001,
        "created_date": "2024-02-20",
        "created_time": "10:15:00",
        "last_updated_date": "2024-02-21",
        "last_updated_time": "12:30:00",
        "sales_staff_id": 501,
        "counterparty_id": 301,
        "units_sold": 5,
        "unit_price": 25.50,
        "currency_id": 1,
        "design_id": 1001,
        "agreed_payment_date": "2024-02-25",
        "agreed_delivery_date": "2024-02-27",
        "agreed_delivery_location_id": 2001,
        "source_file": "sales_orders_20240304.json"
    }
]

@pytest.fixture
def mock_db_connection():
    with patch("src.fact_sales_order.connect_to_db") as mock_connect, patch("src.fact_sales_order.close_db_connection") as mock_close:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_conn
        # mock_close.assert_called_once_with(mock_conn)

class TestCreateTable:
    # test 1 - test table creation function
    @patch("src.fact_sales_order.connect_to_db")
    def test_fact_sales_order_table(self, mock_connect, mock_db_connection):

        mock_connect.return_value = mock_db_connection 

        create_fact_sales_order_table()

        mock_db_connection.execute.assert_called()

    # test 2 - test table creation function still executes when table already exists
    @patch("src.fact_sales_order.connect_to_db")
    def test_create_fact_sales_order_if_exists(self, mock_connect):
        
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        create_fact_sales_order_table()

        mock_conn.execute.assert_called()

    # test 3 - test table create function fails
    @patch("src.fact_sales_order.connect_to_db")
    def test_create_fact_sales_order_failure(self, mock_connect):

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.side_effect = Exception("SQL execution error")

        with pytest.raises(Exception, match="SQL execution error"):
            create_fact_sales_order_table()

class TestTransformFactDataFunction:
    # test - is of type dataframe
    @patch("src.fact_sales_order.connect_to_db")
    def test_return_is_of_type_dataframe(self, mock_connect, mock_db_connection):
        mock_connect.return_value = MagicMock()

        df = transform_fact_data(TEST_JSON_DATA)

        assert isinstance(df, pd.DataFrame)
    # test - dataframe is not empty
    @patch("src.fact_sales_order.connect_to_db")
    def test_dataframe_is_not_empty(self, mock_connect, mock_db_connection):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.mappings.return_value = [
    {
        "sales_order_id": 1001,
        "created_date": "2024-02-20",
        "created_time": "10:15:00",
        "last_updated_date": "2024-02-21",
        "last_updated_time": "12:30:00",
        "sales_staff_id": 501,
        "counterparty_id": 301,
        "units_sold": 5,
        "unit_price": 25.50,
        "currency_id": 1,
        "design_id": 1001,
        "agreed_payment_date": "2024-02-25",
        "agreed_delivery_date": "2024-02-27",
        "agreed_delivery_location_id": 2001,
        "source_file": "sales_orders_20240304.json"
    }
]

        df = transform_fact_data(TEST_JSON_DATA)

        assert not df.empty

        expected_columns = {"sales_order_id", "created_date", "created_time", "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id", "source_file"}

        for item in expected_columns:
            assert item in set(df.columns)

    # test for foreign key constraints related to dim tables

    @patch("src.fact_sales_order.connect_to_db")
    def test_for_foreign_keys(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.mappings.return_value = [
    {
        "sales_order_id": 1001,
        "created_date": "2024-02-20",
        "created_time": "10:15:00",
        "last_updated_date": "2024-02-21",
        "last_updated_time": "12:30:00",
        "sales_staff_id": 501,
        "counterparty_id": 301,
        "units_sold": 5,
        "unit_price": 25.50,
        "currency_id": 1,
        "design_id": 1001,
        "agreed_payment_date": "2024-02-25",
        "agreed_delivery_date": "2024-02-27",
        "agreed_delivery_location_id": 2001,
        "source_file": "sales_orders_20240304.json"
    }
]


        df = transform_fact_data(TEST_JSON_DATA)

        assert df["sales_staff_id"].isnull().sum() == 0
        assert df["counterparty_id"].isnull().sum() == 0





    # test 1 - empty list is returned when no data passed in
    # @patch("src.fact_sales_order.connect_to_db")
    # # def test_for_no_data(self, mock_connect):

    # #     mock_conn = MagicMock()
    # #     mock_connect.return_value = mock_conn
    # #     mock_input = []

    # #     result = transform_fact_data(mock_input)

    # #     assert result == []

    # test 2 - sql exception error raised if function fails

    @patch("src.fact_sales_order.connect_to_db")
    def test_for_sql_exception(self, mock_connect):

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.side_effect = Exception("SQL execution error")

        with pytest.raises(Exception, match="SQL execution error"):
            transform_fact_data(TEST_JSON_DATA)

    # test 3 - transformation function executed successfully
    @patch("src.fact_sales_order.connect_to_db")
    def test_for_transformation_success(self, mock_connect):

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.mappings.return_value = [
            {
            "sales_order_id": 1001,
            "created_date": "2024-02-20",
            "created_time": "10:15:00",
            "last_updated_date": "2024-02-21",
            "last_updated_time": "12:30:00",
            "sales_staff_id": 501,
            "counterparty_id": 301,
            "units_sold": 5,
            "unit_price": 25.50,
            "currency_id": 1,
            "design_id": 1001,
            "agreed_payment_date": "2024-02-25",
            "agreed_delivery_date": "2024-02-27",
            "agreed_delivery_location_id": 2001
            }
        ]

        result = transform_fact_data(TEST_JSON_DATA)

        # assert isinstance(result, list)
        # assert len(result) == 1
        assert result.iloc[0]["sales_order_id"] == 1001
        assert result.iloc[0]["created_date"] == "2024-02-20"
        assert result.iloc[0]["created_time"] == "10:15:00"
        assert result.iloc[0]["last_updated_date"] == "2024-02-21"
        assert result.iloc[0]["last_updated_time"] == "12:30:00"
        # assert result[0]["sales_staff_id"] == 501
        # assert result[0]["counterparty_id"] == 301
        # assert result[0]["units_sold"] == 5
        # assert result[0]["unit_price"] == 25.50
        # assert result[0]["currency_id"] == 1
        # assert result[0]["design_id"] == 1001
        # assert result[0]["agreed_payment_date"] == "2024-02-25"
        # assert result[0]["agreed_delivery_date"] == "2024-02-27"
        # assert result[0]["agreed_delivery_location_id"] == 2001







        
        




        
















# test 3 - data with missing columns raises keyerror
# test 4 - 