import json
import botocore
from datetime import datetime
import decimal
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def reformat_data_to_json(operational_data, column_headings):
    
    try:
        format_data = [dict(zip(column_headings, row)) for row in operational_data]
        for row in format_data:
            for key in row:
                if isinstance(row[key], datetime):
                    row[key] = row[key].strftime('%Y-%m-%dT%H:%M:%S.%f')
                if isinstance(row[key], decimal.Decimal):
                    row[key] = float(row[key])
        return sorted(format_data, key=lambda x: x['last_updated'])

    except Exception as e:
        print(e)
        logger.error(f"Error formatting the data from db: {e}")


def list_of_tables():
    return [
        "address",
        "staff",
        "department",
        "design",
        "counterparty",
        "sales_order",
        "transaction",
        "payment",
        "purchase_order",
        "payment_type",
        "currency",
    ]


def get_file_contents_of_last_uploaded(s3_client, bucket_name, table):
    
    try:
        file_data = s3_client.get_object(Bucket=bucket_name,
                                        Key=f'last_updated/{table}.txt')
        file_with_latest_data = file_data['Body'].read().decode('utf-8')
        file_data = s3_client.get_object(Bucket=bucket_name,
                                        Key=file_with_latest_data)
        data = file_data['Body'].read().decode('utf-8')
        file_content = json.loads(data)
        return (file_content)
    except Exception as e:
        logger.error(f"Error retrieving last_updated_file: {e}")
        
