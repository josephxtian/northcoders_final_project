from s3_read_from_injestion_bucket import read_files_from_s3
import pytest
import boto3
import os
from moto import mock_aws
import time
import json
import uuid

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def test_put_file_with_last_processed():
    bucket_name = f"test-bucket-{uuid.uuid4().hex}"
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        json_bucket_contents = {
        "address/2022/11/3/14:20:51.563000.json":[{"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "address/2023/11/3/14:20:51.563000.json" : [{"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "address/2024/11/3/14:20:51.563000.json" : [{"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}],
        "design/2023-10-11T09:22:09.924000.json" :[{"design_id": 224, "created_at": "2023-10-11T09:22:09.924000", "design_name": "Soft", "file_location": "/usr/bin", "file_name": "soft-20220614-1sgl.json", "last_updated": "2023-10-11T09:22:09.924000"}],
        "design/2023-10-11T14:51:09.697000.json" : [{"design_id": 226, "created_at": "2023-10-11T14:51:09.697000", "design_name": "Steel", "file_location": "/Library", "file_name": "steel-20230311-iys7.json", "last_updated": "2023-10-11T14:51:09.697000"}],
        "counterparty/2023-10-11T09:22:09.924000.json":"",
        "currency/2023-10-11T09:22:09.924000.json":"",
        "department/2023-10-11T09:22:09.924000.json":"",
        "payment_type/2023-10-11T09:22:09.924000.json":"",
        "payment/2023-10-11T09:22:09.924000.json":"",
        "purchase_order/2023-10-11T09:22:09.924000.json":"",
        "sales_order/2023-10-11T09:22:09.924000.json":"",
        "staff/2023-10-11T09:22:09.924000.json":"",
        "transaction/2023-10-11T09:22:09.924000.json":"",
        }
        txt_bucket_contents = {
        "last_processed/address.txt": "address/2022/11/3/14:20:51.563000.json",
        "last_processed/design.txt": "design/2023/10/11/09:22:09.924000.json"
        }
        for filename, content in json_bucket_contents.items():
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=json.dumps(content))
        
        for filename, content in txt_bucket_contents.items():
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=content)
        s3_client.list_objects_v2(Bucket=bucket_name)
        
        response =read_files_from_s3(bucket_name,s3_client)
        assert response == {"address":[{"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"},
                            {"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"},
                            {"id": 18413, "x": "y", "z": "a", "b": 5373, "created_at": "2025-03-07T13:57:09.788000", "last_updated": "2025-03-07T13:57:09.788000"}                      
                            ], "design":[{"design_id": 224, "created_at": "2023-10-11T09:22:09.924000", "design_name": "Soft", "file_location": "/usr/bin", "file_name": "soft-20220614-1sgl.json", "last_updated": "2023-10-11T09:22:09.924000"},
                                         {"design_id": 226, "created_at": "2023-10-11T14:51:09.697000", "design_name": "Steel", "file_location": "/Library", "file_name": "steel-20230311-iys7.json", "last_updated": "2023-10-11T14:51:09.697000"}
                                         ]}
        