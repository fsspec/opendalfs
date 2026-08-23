import os
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class S3Config:
    endpoint: str
    region: str
    bucket: str
    access_key_id: str
    secret_access_key: str

    @classmethod
    def from_env(cls):
        return cls(
            endpoint=os.getenv("OPENDAL_S3_ENDPOINT", "http://127.0.0.1:9000"),
            region=os.getenv("OPENDAL_S3_REGION", "us-east-1"),
            bucket=os.getenv("OPENDAL_S3_BUCKET", "test-bucket"),
            access_key_id=os.getenv("OPENDAL_S3_ACCESS_KEY_ID", "minioadmin"),
            secret_access_key=os.getenv("OPENDAL_S3_SECRET_ACCESS_KEY", "minioadmin"),
        )


def get_s3_client(config: S3Config):
    """Get a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        region_name=config.region,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )


def create_test_bucket(config: S3Config):
    """Create test bucket if it doesn't exist."""
    s3 = get_s3_client(config)
    try:
        s3.create_bucket(Bucket=config.bucket)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]:
            raise


def cleanup_bucket(config: S3Config):
    """Clean up all objects in test bucket."""
    s3 = get_s3_client(config)
    try:
        objects = s3.list_objects_v2(Bucket=config.bucket)
        for item in objects.get("Contents", []):
            s3.delete_object(Bucket=config.bucket, Key=item["Key"])
    except Exception as error:
        print(f"Warning: Cleanup failed: {error}")
