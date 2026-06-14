import boto3

s3_client = boto3.client("s3")

buckets = s3_client.list_buckets()

for bucket in buckets["Buckets"]:
    print(f"Bucket Name: {bucket['Name']}, Creation Date: {bucket['CreationDate']}")
