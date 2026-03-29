import boto3
import os
from dotenv import load_dotenv
from pathlib import Path
import json

# ── Load credentials from .env ───────────────────────
load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")

# ── Connect to S3 ────────────────────────────────────
s3_client = boto3.client(
    "s3",
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = AWS_REGION,
)

# ── Upload function with progress feedback ────────────
def upload_file(local_path, s3_key):
    file_size = Path(local_path).stat().st_size / 1024  # KB
    print(f"⬆  Uploading: {local_path}  ({file_size:.1f} KB)")
    try:
        s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)
        print(f"   ✅ Success → s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        raise

# ── Verify bucket exists ──────────────────────────────
def verify_bucket():
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f"✅ Bucket found: {S3_BUCKET_NAME}\n")
    except Exception as e:
        print(f"❌ Bucket not found or no access: {e}")
        raise

# ── List what's in S3 after upload ───────────────────
def list_s3_files(prefix="raw/"):
    print(f"\n── Files in s3://{S3_BUCKET_NAME}/{prefix} ──────────")
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=prefix
    )
    if "Contents" not in response:
        print("  (empty)")
        return
    for obj in response["Contents"]:
        size_kb = obj["Size"] / 1024
        print(f"  📄 {obj['Key']}  ({size_kb:.1f} KB)  "
              f"Last modified: {obj['LastModified'].strftime('%Y-%m-%d %H:%M')}")

# ════════════════════════════════════════════════════
#  MAIN — upload all raw files
# ════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 55)
    print("  ETL Project — S3 Upload")
    print("=" * 55)

    # Step 1: Verify connection
    verify_bucket()

    # Step 2: Upload raw files
    files_to_upload = [
        {
            "local":  "data/raw/sales_transactions.csv",
            "s3_key": "raw/sales_transactions.csv",
        },
        {
            "local":  "data/raw/customer_profiles.json",
            "s3_key": "raw/customer_profiles.json",
        },
    ]

    for file in files_to_upload:
        upload_file(file["local"], file["s3_key"])

    # Step 3: Confirm what's in S3
    list_s3_files(prefix="raw/")

    # Step 4: Save upload log
    log = {
        "uploaded_at": str(__import__("datetime").datetime.now()),
        "bucket":      S3_BUCKET_NAME,
        "files":       [f["s3_key"] for f in files_to_upload],
        "status":      "success",
    }
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/upload_log.json", "w") as f:
        json.dump(log, f, indent=2)
    print("\n📋 Upload log saved → data/processed/upload_log.json")
    print("\n🎉 All files uploaded successfully to S3!")