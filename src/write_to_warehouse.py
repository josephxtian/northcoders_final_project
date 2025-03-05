
import psycopg2
import sys
import boto3
import os
import pandas as pd
import re
import datetime
"""
This function should read from the s3 processed bucket then send the data to the data warehouse.

Possibly separate out into:
- s3 read function that reads from processed bucket
- create a warehouse?
- populate warehouse with processed files
"""

PG_USER = "project_team_4"
PG_PASSWORD = "9oOBwGHBqmo161l"
PG_HOST = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
PG_DATABASE = "postgres"
PG_PORT = 5432

# read from s3 processed bucket

table_names = ["fact_sales_order", "dim_staff", "dim_date", "dim_counterparty", "dim_location", "dim_currency", "dim_design"]

def read_from_s3_processed_bucket():
    s3_client = boto3.client("s3")

    data_frames_dict = {}
    

    for table in table_names:
        file_dates_list = []
        objects = s3_client.list_objects_v2(bucket="processed-bucket20250303162226216400000005", prefix=f"{table}/")
        for object in objects["Contents"]:
            key = object["key"]
            filename_timestamp_format = "%Y-%m-%d_%H-%M-%S"
            filename_timestamp_str = key.split(f"{table}_")[1].split(".parquet")[0] 
            timestamp = datetime.strptime(filename_timestamp_str, filename_timestamp_format)
            file_dates_list.append((timestamp, key))
        file_dates_list.sort(key=lambda tup: tup[0], reverse=True)

        latest_file = file_dates_list[0][1]
        
        latest_file_object = s3_client.get_object(bucket="processed-bucket20250303162226216400000005", key=latest_file)

        buffer = io.BytesIO()
        data.to_parquet(buffer, engine="pyarrow", index=False)

   





# connect to the (redshift?) warehouse, conn=

def write_to_warehouse(file):


# convert from parquet back to schema
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        for file 
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))  
# Insert data into redshift via postgres query
# upload to warehouse in defined intervals
# must be adequately logged in cloudwatch
