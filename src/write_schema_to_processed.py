import boto3
import pandas as pd
import io
import os
from datetime import datetime

"""

This function should take the ouput of the transform data function (list of dictionaries) and write each piece of data to the s3 processed bucket

The files should be saved with a name that makes them easily comparable and traceable respective to themselves when they were in the s3 ingestion bucket

The files should be converted and saved in the s3 processed bucket in parquet format

"""

def write_schema_to_processed(data):

    s3_client = boto3.client("s3")

    bucket_name = os.getenv("processed-bucket20250303162226216400000005")

    if not bucket_name:
        raise ValueError("processed-bucket20250303162226216400000005 environment variable is not set")
    
    # source_file = data[0].get("source_file", "unknown_source.json")

    if "source_file" in data.columns:
        source_file = data["source_file"].iloc[0]
        file_basename = os.path.splitext(os.path.basename(source_file))[0]
    else:
        file_basename = "unknown_source"

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    
# Set a variable key which dynamically changes the name of each of the pieces of processed data based on one of their unique elements
    key = f"fact_sales_orders/{file_basename}_processed_{timestamp}.parquet"

    try:
        # Convert the files into parquet format

        buffer = io.BytesIO()
        data.to_parquet(buffer, engine="pyarrow", index=False)

        # Write each file into the S3 processed bucket
        s3_client.put_object(
            Bucket = bucket_name,
            Key = key,
            Body = buffer.getvalue(),
            ContentType = "application/octet-stream"
        )
        print(f"Data successfully written to s3 in parquet format: s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error writing to s3: {e}")
        raise
# Iterate over the list of dictionaries and dump each individual element into a json object

