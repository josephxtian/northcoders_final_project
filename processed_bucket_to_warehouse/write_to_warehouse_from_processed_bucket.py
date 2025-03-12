
import psycopg2
import boto3
import pandas as pd
from datetime import datetime
import io
from pprint import pprint
from contextlib import closing

"""
This function should read from the s3 processed bucket then send the data to the data warehouse.

Possibly separate out into:
- s3 read function that reads from processed bucket
- create a warehouse?
- populate warehouse with processed files
"""
def get_bucket_name(bucket_prefix):
    s3_client = boto3.client('s3')
    list_of_buckets_in_s3 = s3_client.list_buckets(
        BucketRegion="eu-west-2")["Buckets"]
    for bucket in list_of_buckets_in_s3:
        if str(bucket["Name"]).startswith(f"{bucket_prefix}"):
            return bucket["Name"]
    raise Exception(f"No bucket found with prefix {bucket_prefix}")

PG_USER = "project_team_4"
PG_PASSWORD = "9oOBwGHBqmo161l"
PG_HOST = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
PG_DATABASE = "postgres"
PG_PORT = 5432

# read from s3 processed bucket

table_names = ["fact_sales_order", "dim_staff", "dim_date", "dim_counterparty", "dim_location", "dim_currency", "dim_design"]

def read_from_s3_processed_bucket(s3_client=None):

    if not s3_client:
        s3_client = boto3.client("s3")

    data_frames_dict = {}

    for table in table_names:
        file_dates_list = []

        objects = s3_client.list_objects_v2(Bucket="processed-bucket20250303162226216400000005", Prefix=f"{table}/")
        pprint(objects)

        if "Contents" not in objects or not objects["Contents"]:
            print(f"No objects found for {table} in S3. Skipping...")
            continue

        for object in objects["Contents"]:
            key = object["Key"]
            filename_timestamp_format = "%Y-%m-%d_%H-%M-%S"

            try:
                filename_timestamp_str = key.split(f"{table}_")[1].split(".parquet")[0] 
                timestamp = datetime.strptime(filename_timestamp_str, filename_timestamp_format)
                file_dates_list.append((timestamp, key))
            except (IndexError, ValueError):
                print("Skipping file {key} due to unexpected naming format")
                continue

        if not file_dates_list:
            print(f"No valid files found for {table}. Skipping...")

        file_dates_list.sort(key=lambda tup: tup[0], reverse=True)

        print(file_dates_list)

        latest_file = file_dates_list[0][1]
        
        latest_file_object = s3_client.get_object(Bucket="processed-bucket20250303162226216400000005", Key=latest_file)

        buffer = io.BytesIO(latest_file_object["Body"].read())
        dataframe = pd.read_parquet(buffer, engine="pyarrow")
        data_frames_dict[table] = dataframe

    return data_frames_dict

# connect to the (redshift?) warehouse, conn=

def write_to_warehouse(data_frames_dict):

    conn = None 
# convert from parquet back to schema
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()

        with closing(conn.cursor()) as cur:
            for table_name in data_frames_dict.keys():
                query = f"SELECT * FROM {table_name}" 
                cur.execute(query)
                query_results = cur.fetchall()
                print(query_results)
    except Exception as e:
        raise RuntimeError(f"Warehouse database operation failed: {e}")
    finally:
        if conn:
            conn.close()  
# Insert data into redshift via postgres query
# upload to warehouse in defined intervals
# must be adequately logged in cloudwatch
