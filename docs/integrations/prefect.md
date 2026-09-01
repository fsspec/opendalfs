# Prefect

<!-- docs-example-group: integration-prefect -->

Prefect's `RemoteFileSystem` block delegates storage access to fsspec and can
therefore use the installed `opendal+s3` protocol.

## Read and write remote data

```python
import asyncio

from prefect.filesystems import RemoteFileSystem


async def main():
    basepath = "opendal+s3://test-bucket/prefect/storage"
    storage_options = {
        "bucket": "test-bucket",
        "endpoint": "http://127.0.0.1:9000",
        "region": "us-east-1",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
    }
    remote = RemoteFileSystem(basepath=basepath, settings=storage_options)

    path = await remote.write_path("result.txt", b"hello from Prefect")
    assert path == f"{basepath}/result.txt"
    assert await remote.read_path("result.txt") == b"hello from Prefect"


asyncio.run(main())
```

## Test coverage

The repository writes and reads a nested path through `RemoteFileSystem`. The
test runs against each configured OpenDAL test backend.

See
[`tests/integration/prefect/test_remote_filesystem.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/prefect/test_remote_filesystem.py).
