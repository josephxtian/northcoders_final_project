import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from io import StringIO
from moto import mock_aws
from src.write_schema_to_processed import write_schema_to_processed
import os
import io


TEST_PROCESSED_BUCKET = "test-processed-bucket"
TEST_SOURCE_FILE = "raw_data_2024-02-25.json"
TEST_DATA = [
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
        "source_file": TEST_SOURCE_FILE  # Ensuring traceability the will be used for eventual filename
    }
]

@pytest.fixture
def set_env():
    """Fixture to set environment variables for testing."""
    os.environ["processed-bucket20250303162226216400000005"] = TEST_PROCESSED_BUCKET
    yield
    del os.environ["processed-bucket20250303162226216400000005"]





# test 1 - print "file successfully added" is outputted when try block of write function is caught
# @patch("boto3.client")
# def test_for_success_message(mock_boto_client, set_env):
#     mock_s3 = MagicMock()
#     mock_boto_client.return_value = mock_s3

#     result = write_schema_to_processed(TEST_DATA)

#     assert "Data successfully written to s3 in parquet format" in result



@patch("boto3.client")
def test_file_successfully_added(mock_boto_client, set_env):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    write_schema_to_processed(TEST_DATA)

    args, kwargs = mock_s3.put_object.call_args

    assert kwargs["Bucket"] == TEST_PROCESSED_BUCKET
    # test 2 - each of the processed files share a unique identifier with what identified them in the s3 ingestion bucket
    assert kwargs["Key"].startswith("fact_sales_orders/raw_data_2024-02-25")

    buffer = io.BytesIO(kwargs["Body"])
    df = pd.read_parquet(buffer, engine="pyarrow")

    assert "source_file" in df.columns
    assert df["source_file"].iloc[0] == TEST_SOURCE_FILE


# test 3 - correct exception captures relevant error
@patch("boto3.client")
def test_relevant_exception_is_raised(mock_boto_client, set_env):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.put_object.side_effect = Exception("S3 Upload Failed")

    with pytest.raises(Exception, match="S3 Upload Failed"):
        write_schema_to_processed(TEST_DATA)

    
    # assert put object only called once for each file
    mock_s3.put_object.assert_called_once()

    





    

