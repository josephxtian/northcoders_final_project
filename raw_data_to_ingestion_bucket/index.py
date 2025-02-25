def lambda_handler(event,context):
    message = 'Hello, we\'re in raw_data_to_ingestion_bucket {} !'.format(event['key1'])
    return {
        'message' : message
    }