# ======================================================
# src/aws_utils.py
# ======================================================
import boto3
from typing import List
import os

def validate_aws_env_vars() -> bool:
    """
    Validates if the required AWS environment variables are set for boto3 authentication.
    Returns True if minimum credentials are set, otherwise False.
    """
    # There are several methods boto3 can use, but for env vars we check:
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")  # optional

    # Region is often, but not always required
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")

    # Minimum required: access key and secret key
    if access_key and secret_key:
        return True
    return False

assert validate_aws_env_vars()

def get_s3_client():
    return boto3.client("s3")


def list_s3_files(bucket: str, prefix: str) -> List[str]:
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]


def copy_s3_object(src_bucket, src_key, tgt_bucket, tgt_key):
    s3 = get_s3_client()
    s3.copy_object(
    CopySource={"Bucket": src_bucket, "Key": src_key},
    Bucket=tgt_bucket,
    Key=tgt_key,
    )


def get_glue_client():
    return boto3.client("glue")


def start_glue_job(job_name: str, args: dict = None):
    glue = get_glue_client()
    return glue.start_job_run(JobName=job_name, Arguments=args or {})