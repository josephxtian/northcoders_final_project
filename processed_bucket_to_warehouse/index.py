import json
from write_to_warehouse_from_processed_bucket import read_from_s3_processed_bucket,write_to_warehouse,get_bucket_name
import logging

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        get_bucket_name("processed_bucket")
        data_frames_dict = read_from_s3_processed_bucket()
        write_to_warehouse(data_frames_dict)
        logger.info("Data has been written to warehouse")
    except:
        logger.error("Data has not been ")
