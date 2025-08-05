import pytest
from opendalfs import OpendalFileSystem
import asyncio
from pytest_lazy_fixtures import lf
from typing import Generator

@pytest.fixture(scope="function")
def s3() -> Generator[OpendalFileSystem, None, None]:
    """Create an S3 filesystem for testing sync operations."""
    fs = OpendalFileSystem(
        scheme="s3",
        bucket="test",
        endpoint="http://localhost:9000",
        region="us-east-1",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        asynchronous=False,
    )

    yield fs
    fs.rmdir("/")


FILE_SYSTEMS = [
    lf("s3"),
]


@pytest.fixture(scope="function")
async def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
