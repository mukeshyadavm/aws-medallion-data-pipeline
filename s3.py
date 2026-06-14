import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
)

buckets = s3_client.list_buckets()

for bucket in buckets["Buckets"]:
    print(f"Bucket Name: {bucket['Name']}, Creation Date: {bucket['CreationDate']}")