import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """Get a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )


def create_test_bucket():
    """Create test bucket if it doesn't exist."""
    s3 = get_s3_client()
    try:
        s3.create_bucket(Bucket="test-bucket")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code not in ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]:
            raise


def cleanup_bucket():
    """Clean up all objects in test bucket."""
    s3 = get_s3_client()
    try:
        objects = s3.list_objects_v2(Bucket="test-bucket")
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3.delete_object(Bucket="test-bucket", Key=obj["Key"])
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")


def verify_bucket():
    """Verify the test bucket is properly set up."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )

    try:
        s3.head_bucket(Bucket="test-bucket")
        print("Bucket exists and is accessible")
    except Exception as e:
        print(f"Error accessing bucket: {e}")
        raise
