import boto3


def get_bucket_name(bucket_prefix):
    s3_client = boto3.client('s3')
    list_of_buckets_in_s3 = s3_client.list_buckets(
        BucketRegion="eu-west-2")["Buckets"]
    for bucket in list_of_buckets_in_s3:
        if str(bucket["Name"]).startswith(f"{bucket_prefix}"):
            return bucket["Name"]
    raise Exception(f"No bucket found with prefix {bucket_prefix}")
