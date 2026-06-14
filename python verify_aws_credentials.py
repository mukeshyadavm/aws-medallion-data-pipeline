import boto3
import os
from dotenv import load_dotenv

load_dotenv()

sts = boto3.client(
    'sts',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

print(sts.get_caller_identity())