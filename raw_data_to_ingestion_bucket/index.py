import boto3
from src.upload_to_s3_bucket import the_list_of_tables
from botocore.exceptions import ClientError
from src.converts_data_to_json import reformat_data_to_json
from datetime import datetime
import json
import decimal
from pg8000.native import literal, identifier,Connection


def get_secret():

    secret_name = "totesys/db_credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']


s3_client = boto3.client("s3")
def lambda_handler(event,context):
    credentials = get_secret()
    db = Connection(
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["database"],
        host=credentials["host"],
        port=credentials["port"])
    
    
    update_data_to_s3_bucket(s3_client, 'ingestion-bucket20250228065732358000000006', the_list_of_tables,reformat_data_to_json)

    Connection.close(db)    
    message = 'Hello, we\'re in raw_data_to_ingestion_bucket {} !'.format(event['key1'])
    return {
        'message' : message
    }

def the_list_of_tables():
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


def reformat_data_to_json(table,db):
    db_run_column = db.run(f" SELECT * FROM {identifier(table)};")
    columns = [col["name"] for col in db.columns]
    format_data = [dict(zip(columns, row)) for row in db_run_column]
    for row in format_data:
        for key in row:
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime('%Y-%m-%dT%H:%M:%S.%f')
            if isinstance(row[key], decimal.Decimal):
                row[key] = float(row[key])
    return format_data

def update_data_to_s3_bucket(s3_client, bucket_name, the_list_of_tables,reformat_data_to_json,db):
    
    for table in the_list_of_tables():
        list_of_s3_file_metadata = []
        current_operational_data = reformat_data_to_json(table,db)
        list_of_s3_file_metadata.append(s3_client.list_objects_v2(Bucket= bucket_name, Prefix=f'{table}'))
        list_of_files =[]
        # instead think we should get last modified to get the latest last_updated date then just look in that one file
        #going to check in list of latest updated files and write to s3 bucket
        for file_data_s3 in list_of_s3_file_metadata:
            for i in range(len(file_data_s3['Contents'])):
                list_of_files.append(file_data_s3['Contents'][i]['Key'])
        #looping over the files in the s3 bucket 
        last_updated_date = datetime.strptime('1900-11-03T14:20:52.186000', '%Y-%m-%dT%H:%M:%S.%f')
        for file in list_of_files:
            file_data = s3_client.get_object(Bucket=bucket_name, Key=file)
            data = file_data['Body'].read().decode('utf-8') 
            file_content = json.loads(data)
            #looping through the contents of files in s3, to check last_updated datestamp
            for content in file_content:
                date_str = content['last_updated']
                last_updated_as_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
                if last_updated_as_date > last_updated_date:
                    last_updated_date = last_updated_as_date
            # checking for content in correct format to check which date is newer(bigger)
            #looping through operational data to add files datestamped to list 
            additional_entries = []
        for data in current_operational_data:
            data_str = data['last_updated']
            data_last_update = datetime.strptime(data_str, '%Y-%m-%dT%H:%M:%S.%f')
            if data_last_update > last_updated_date:
                additional_entries.append(data)
        # checking if there's addtional entries to add, a file is created with datestamp
        if additional_entries:
            current_timestamp = datetime.now()
            formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            object_key = f"{table}/{formatted_timestamp}.json"
            s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(additional_entries))
    return f"data has been added to {bucket_name}"
    



