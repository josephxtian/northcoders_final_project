import boto3
from botocore.exceptions import ClientError
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
        print(f"Error retrieving secret: {e}")
        raise e

    secret = get_secret_value_response.get('SecretString')

    #return secret

    try:
        db_credentials = json.loads(secret)
        print(db_credentials)
    except json.JSONDecodeError as e:
        print(f"Error parsing secret JSON: {e}")
        raise

    return db_credentials

s3_client = boto3.client("s3")
def lambda_handler(event,context):
    db_credentials = get_secret()

    if not isinstance(db_credentials, dict):
        raise ValueError("The credentials not in  expected dict format")

    db = Connection(
        user=db_credentials.get("user"),
        password=db_credentials.get("password"),
        database=db_credentials.get("database"),
        host=db_credentials.get("host"),
        port=db_credentials.get("port"))
    
    update_data_to_s3_bucket(s3_client, 'ingestion-bucket20250228065732358000000006', list_of_tables, reformat_data_to_json, get_file_contents_of_last_uploaded, db)

    Connection.close(db)    
    message = 'Hello, we\'re in raw_data_to_ingestion_bucket {} !'.format(event['key1'])
    return {
        'message' : message
    }

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

def reformat_data_to_json(operational_data, column_headings):
    
    format_data = [dict(zip(column_headings, row)) for row in operational_data]
    for row in format_data:
        for key in row:
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime('%Y-%m-%dT%H:%M:%S.%f')
            if isinstance(row[key], decimal.Decimal):
                row[key] = float(row[key])
    
    return sorted(format_data, key=lambda x: x['last_updated'])
   
def get_file_contents_of_last_uploaded(s3_client,bucket_name, table):
    file_data = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/last_updated.txt')
    file_with_latest_data = file_data['Body'].read().decode('utf-8')
    file_data = s3_client.get_object(Bucket=bucket_name, Key=file_with_latest_data) 
    data = file_data['Body'].read().decode('utf-8')
    file_content = json.loads(data)
    return(file_content)

def update_data_to_s3_bucket(s3_client, bucket_name, list_of_tables, reformat_data_to_json, 
                             get_file_contents_of_last_uploaded, db):
    list_of_table_data_uploaded = []
    for table in list_of_tables():
        last_data_uploaded = get_file_contents_of_last_uploaded(s3_client, bucket_name,table)
        last_updated_date =datetime.strptime('1900-11-03T14:20:52.186000', '%Y-%m-%dT%H:%M:%S.%f') 
        for row in last_data_uploaded:
            data_from_s3 = datetime.strptime(row["last_updated"], '%Y-%m-%dT%H:%M:%S.%f')
            if data_from_s3 > last_updated_date:
                last_updated_date = data_from_s3
        db_run_column = db.run(f""" SELECT * FROM {identifier(table)} 
                               WHERE last_updated > {literal(last_updated_date)};""")
        columns = [col["name"] for col in db.columns]
        additional_data_from_op_db = reformat_data_to_json(db_run_column,columns)

        if additional_data_from_op_db:
            data_to_upload = [additional_data_from_op_db[0]]
            if len(additional_data_from_op_db) == 1:
                date_updated = datetime.strptime(additional_data_from_op_db[0]
                                                 ["last_updated"], '%Y-%m-%dT%H:%M:%S.%f')
                year=date_updated.year
                month=date_updated.month
                day=date_updated.day
                time=date_updated.time()
                object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                s3_client.put_object(Bucket=bucket_name,Key=object_key,
                                            Body=json.dumps(data_to_upload))
                s3_client.put_object(Bucket=bucket_name,Key=f'{table}/last_updated.txt',
                                            Body=object_key)
            for i in range(1,len(additional_data_from_op_db)):
                    if i==len(additional_data_from_op_db)-1:
                        data_to_upload.append(additional_data_from_op_db[i])
                        date_updated = datetime.strptime(additional_data_from_op_db[i]
                                                         ["last_updated"], '%Y-%m-%dT%H:%M:%S.%f')
                        year=date_updated.year
                        month=date_updated.month
                        day=date_updated.day
                        time=date_updated.time()
                        object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                        s3_client.put_object(Bucket=bucket_name,Key=object_key,
                                             Body=json.dumps(data_to_upload))
                        s3_client.put_object(Bucket=bucket_name,Key=f'{table}/last_updated.txt',
                                             Body=object_key)
 
                    elif additional_data_from_op_db[i]["last_updated"]==(additional_data_from_op_db
                                                                         [i-1]["last_updated"]):
                        data_to_upload.append(additional_data_from_op_db[i])
                    elif additional_data_from_op_db[i]["last_updated"]!=(additional_data_from_op_db[i-1]
                                                                         ["last_updated"]):
                        date_updated = (datetime.strptime(additional_data_from_op_db[i-1]
                                                          ["last_updated"], '%Y-%m-%dT%H:%M:%S.%f'))
                        year=date_updated.year
                        month=date_updated.month
                        day=date_updated.day
                        time=date_updated.time()
                        object_key = f"{table}/{year}/{month}/{day}/{time}.json"
                        s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(data_to_upload))
                        data_to_upload = [additional_data_from_op_db[i]]
            list_of_table_data_uploaded.append(table)
    return f"data has been added to {bucket_name}, in files {list_of_table_data_uploaded}"
