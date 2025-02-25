def lambda_handler(event,context):
    message = 'Hello, we\'re in ingestion_to_processed_bucket{} !'.format(event['key1'])
    return {
        'message' : message
    }
