import pytest
from opendalfs import OpendalFileSystem
import boto3
import asyncio


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

    pytest.skip(
        "MinIO is not available. Please start it with: docker-compose -f tests/docker/docker-compose.yml up -d"
    )


@pytest.fixture
def memory_fs():
    """Create a memory filesystem for testing sync operations."""
    return OpendalFileSystem("memory", asynchronous=False)


@pytest.fixture
def s3_fs(minio_server):
    """Create an S3 filesystem for testing sync operations."""
    from .utils.s3 import create_test_bucket, cleanup_bucket, verify_bucket

    fs = OpendalFileSystem(
        scheme="s3",
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


@pytest.fixture(scope="function")
async def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_write_read(memory_fs, s3_fs):
    """Test basic write and read operations."""
    for fs in [memory_fs, s3_fs]:
        content = b"test content"
        await fs._write("test.txt", content)
        result = await fs._read("test.txt")
        assert result == content
