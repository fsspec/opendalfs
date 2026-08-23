import asyncio

import boto3
import pytest

from opendalfs import OpendalFileSystem
from opendalfs.registry import OpendalS3FileSystem


@pytest.fixture(scope="session")
def minio_server():
    """Ensure MinIO server is available for testing."""
    import socket
    import time

    # Check if MinIO is accessible
    retries = 3
    while retries > 0:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(("localhost", 9000))
            if result == 0:
                sock.close()
                return  # MinIO is available
        except Exception as e:
            print(f"DEBUG: Error connecting to MinIO: {e}, retrying...")
            pass
        retries -= 1
        time.sleep(1)

    raise pytest.skip.Exception(
        "MinIO is not available. Run the integration suite with: make integration"
    )


@pytest.fixture
def s3_fs(minio_server):
    """Create an S3 filesystem for testing sync operations."""
    from .utils.s3 import cleanup_bucket, create_test_bucket, verify_bucket

    fs = OpendalS3FileSystem(
        bucket="test-bucket",
        endpoint="http://localhost:9000",
        region="us-east-1",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        asynchronous=False,
    )

    create_test_bucket()
    verify_bucket()

    # Verify we can write directly with boto3
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    s3.put_object(Bucket="test-bucket", Key="test.txt", Body=b"test")
    print("DEBUG: Wrote test file directly with boto3")

    yield fs
    cleanup_bucket()


@pytest.fixture
def s3fs_fs(s3_fs):
    """Create the s3fs reference implementation against the same bucket."""
    from s3fs import S3FileSystem

    return S3FileSystem(
        key="minioadmin",
        secret="minioadmin",
        client_kwargs={
            "endpoint_url": "http://localhost:9000",
            "region_name": "us-east-1",
        },
        use_listings_cache=False,
        skip_instance_cache=True,
    )


@pytest.fixture
def memory_fs():
    """Create an in-memory filesystem for tests that don't require external services."""
    return OpendalFileSystem(
        scheme="memory",
        asynchronous=False,
        skip_instance_cache=True,
    )


@pytest.fixture(params=["memory", "s3"])
def any_fs(request):
    if request.param == "memory":
        return OpendalFileSystem(
            scheme="memory", asynchronous=False, skip_instance_cache=True
        )
    return request.getfixturevalue("s3_fs")


@pytest.fixture(scope="function")
async def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_write_read(s3_fs):
    """Test basic write and read operations."""
    for fs in [s3_fs]:
        content = b"test content"
        await fs._pipe_file("test.txt", content)
        result = await fs._cat_file("test.txt")
        assert result == content
