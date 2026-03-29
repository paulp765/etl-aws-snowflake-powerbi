import boto3, os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name           = os.getenv("AWS_REGION"),
)

BUCKET = os.getenv("S3_BUCKET_NAME")

def download_file(s3_key, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"⬇  Downloading s3://{BUCKET}/{s3_key}")
    s3.download_file(BUCKET, s3_key, local_path)
    print(f"   ✅ Saved to {local_path}")

if __name__ == "__main__":
    download_file("raw/sales_transactions.csv",  "data/raw/sales_transactions_s3.csv")
    download_file("raw/customer_profiles.json",  "data/raw/customer_profiles_s3.json")
    print("\n✅ Downloads complete!")