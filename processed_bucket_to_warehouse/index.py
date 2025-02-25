def lambda_handler(event,context):
    message = 'Hello, we\'re in processed_bucket_to_warehouse {} !'.format(event['key1'])
    return {
        'message' : message
    }