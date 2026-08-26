import asyncio

import pytest
from botocore.exceptions import EndpointConnectionError

from opendalfs import OpendalFileSystem
from opendalfs.registry import OpendalS3FileSystem
from tests.utils.s3 import S3Config, cleanup_bucket, create_test_bucket, get_s3_client


@pytest.fixture(scope="session")
def s3_config():
    return S3Config.from_env()


@pytest.fixture(scope="session")
def minio_server(s3_config):
    """Skip S3 tests when their externally managed service is unavailable."""
    try:
        get_s3_client(s3_config).list_buckets()
    except EndpointConnectionError:
        raise pytest.skip.Exception(
            "S3 is unavailable; run the integration suite with just integration"
        ) from None


@pytest.fixture
def s3_fs(minio_server, s3_config):
    """Create the canonical-path S3 service adapter."""
    fs = OpendalS3FileSystem(
        bucket=s3_config.bucket,
        endpoint=s3_config.endpoint,
        region=s3_config.region,
        access_key_id=s3_config.access_key_id,
        secret_access_key=s3_config.secret_access_key,
        asynchronous=False,
    )

    create_test_bucket(s3_config)

    yield fs
    cleanup_bucket(s3_config)


@pytest.fixture
def s3fs_fs(s3_fs, s3_config):
    """Create the s3fs reference implementation against the same bucket."""
    from s3fs import S3FileSystem

    return S3FileSystem(
        key=s3_config.access_key_id,
        secret=s3_config.secret_access_key,
        client_kwargs={
            "endpoint_url": s3_config.endpoint,
            "region_name": s3_config.region,
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
    s3_fs = request.getfixturevalue("s3_fs")
    return OpendalFileSystem("s3", **s3_fs.storage_options, skip_instance_cache=True)


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
