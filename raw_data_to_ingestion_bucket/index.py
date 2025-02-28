
# from boto3 import client
# from botocore.exceptions import ClientError
# from datetime import datetime 
# import json 
# from pg8000.native import literal, identifier,Connection
# import decimal



def lambda_handler(event,context):
    # s3_client = client("s3")
    # if s3_client.list_objects_v2(Bucket='ingestion-bucket20250227163725841100000004')['KeyCount'] ==0:
    #     write_to_s3_bucket(s3_client,'ingestion-bucket20250227163725841100000004',the_list_of_tables)
        
    message = 'Hello, we\'re in raw_data_to_ingestion_bucket {} !'.format(event['key1'])
    return {
        'message' : message
    }

# def write_to_s3_bucket(s3_client, bucket_name, the_list_of_tables):
#     list_of_tables = the_list_of_tables()
#     try:
#         count = 0
#         for table in list_of_tables:
#             object_key = f"{table}/seed.json"
#             json_formatted_data_to_upload = reformat_data_to_json(table)
#             s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=json.dumps(json_formatted_data_to_upload))
#             count+=1
#             #s3_client.upload_file(file, bucket_name , object_key)
        
#         return f"success - {count} files have been written to {bucket_name}!"

#     except ClientError:
#         return {'result': 'FAILURE' ,"message":"file could not be uploaded"}
    
# def the_list_of_tables():
#     return [
#         "address",
#         "staff",
#         "department",
#         "design",
#         "counterparty",
#         "sales_order",
#         "transaction",
#         "payment",
#         "purchase_order",
#         "payment_type",
#         "currency",
#     ]

# def reformat_data_to_json(table):
#     db = connect_to_db()
#     db_run_column = db.run(f" SELECT * FROM {identifier(table)};")
#     columns = [col["name"] for col in db.columns]
#     format_data = [dict(zip(columns, row)) for row in db_run_column]
#     for row in format_data:
#         for key in row:
#             if isinstance(row[key], datetime.datetime):
#                 row[key] = row[key].isoformat()
#             if isinstance(row[key], decimal.Decimal):
#                 row[key] = float(row[key])
#     close_db_connection(db)
#     return format_data

# def connect_to_db():
#     return Connection(
#         user=,
#         password=,
#         database=,
#         host=,
#         port=,
#     ) get values from secrets manager


# def close_db_connection(conn):
#     conn.close()
=======
import json

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello World!')
    }

