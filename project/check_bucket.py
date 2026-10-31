import os
import boto3
from dotenv import load_dotenv

# Load credentials from your existing .env file
load_dotenv()

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")

# Initialize the R2 client
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

# Query the bucket
response = s3_client.list_objects_v2(Bucket=R2_BUCKET)

print(f"--- Contents of {R2_BUCKET} ---")

if 'Contents' in response:
    for item in response['Contents']:
        print(f"- {item['Key']} (Size: {item['Size']} bytes)")
else:
    print("The bucket is currently empty.")
