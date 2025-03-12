from src.s3_read_from_ingestion_bucket import read_files_from_s3
import pytest
import boto3
import os
from moto import mock_aws
import time
import json
from pprint import pprint
import uuid

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def test_put_file_with_last_processed():
    
    with mock_aws():
        bucket_name = f"test-bucket-{uuid.uuid4().hex}"
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        json_bucket_contents = {
        "address/2022/11/03/14:20:51.563000.json":[{"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "address/2023/11/03/14:20:51.563000.json" : [{"id": 18414, "x": "y", "z": "a", "b": 5374, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "address/2024/11/03/14:20:51.563000.json" : [{"id": 18415, "x": "y", "z": "a", "b": 5375, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "counterparty/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "currency/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "department/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "design/2023/10/11/09:22:09.924000.json" :[{"id": 18413, "x": "y"}],
        "payment/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "purchase_order/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "sales_order/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "staff/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}],
        "transaction/2023/10/11/09:22:09.924000.json":[{"id": 18413, "x": "y"}]
        }
        txt_bucket_contents = {
        "last_processed/address.txt": "address/2022/11/03/14:20:51.563000.json",
        "last_processed/counterparty.txt": "counterparty/2023/10/11/09:22:09.924000.json",
        "last_processed/currency.txt": "currency/2023/10/11/09:22:09.924000.json",
        "last_processed/department.txt": "department/2023/10/11/09:22:09.924000.json",
        "last_processed/design.txt": "design/2023/10/11/09:22:09.924000.json",
        "last_processed/payment.txt" : "payment/2023/10/11/09:22:09.924000.json",
        "last_processed/purchase_order.txt" : "purchase_order/2023/10/11/09:22:09.924000.json",
        "last_processed/sales_order.txt" : "sales_order/2023/10/11/09:22:09.924000.json",
        "last_processed/staff.txt" : "staff/2023/10/11/09:22:09.924000.json",
        "last_processed/transaction.txt" : "transaction/2023/10/11/09:22:09.924000.json"
        }
        
        for filename, content in json_bucket_contents.items():
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=json.dumps(content))
        
        for filename, content in txt_bucket_contents.items():
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=content)
        
        response =read_files_from_s3(bucket_name,s3_client)
        assert response == {"address":[
                            {"id": 18414, "x": "y", "z": "a", "b": 5374, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"},
                            {"id": 18415, "x": "y", "z": "a", "b": 5375, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}                      
                            ]}
        response =read_files_from_s3(bucket_name,s3_client)
        assert response == {}