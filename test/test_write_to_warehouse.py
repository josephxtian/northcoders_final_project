import pytest
import pandas as pd
import boto3
from unittest.mock import patch, MagicMock
from io import StringIO
from moto import mock_aws
from src.write_to_warehouse import read_from_s3_processed_bucket, write_to_warehouse
import os
import io
import psycopg2


@pytest.fixture
def mock_db_connection():
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_connect.return_value = mock_cur
        yield mock_conn, mock_cur
       

# test 1 - reads file from s3 processed bucket
@patch("boto3.client")
def test_s3_read_from_bucket(mock_boto_client):

    mock_s3 = MagicMock()
    
    mock_boto_client.return_value = mock_s3
    
    mock_s3.list_objects_v2.return_value = [{"Contents": [{"Key": "fact_sales_order.parquet"}]}]

    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"dog")} 
    
    output = read_from_s3_processed_bucket()
    
    assert output["fact_sales_order"] == "dog"


# test 2 - retrieving data from s3 processed bucket
# test 3 - data successfully populated to warehouse
# test 4 - failure exceptions are captured and printed