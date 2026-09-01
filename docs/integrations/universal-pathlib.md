# universal-pathlib

`universal-pathlib` exposes fsspec filesystems through a familiar `pathlib`
interface.

## Work with remote paths

```python
import fsspec
from opendalfs import S3FileSystem
from upath import UPath

fsspec.register_implementation("s3", S3FileSystem, clobber=True)
fsspec.config.conf["s3"] = {
    "endpoint_url": "http://127.0.0.1:9000",
    "client_kwargs": {"region_name": "us-east-1"},
    "key": "minioadmin",
    "secret": "minioadmin",
}
root = UPath("s3://test-bucket/universal-pathlib")

folder = root / "results"
folder.mkdir(parents=True)
text_path = folder / "result.txt"
text_path.write_text("hello from UPath")

assert text_path.read_text() == "hello from UPath"
assert list(folder.iterdir()) == [text_path]
assert text_path.exists()
```

## Test coverage

The repository tests text and byte I/O, directory listing, and path existence.

See
[`tests/integration/universal_pathlib/test_upath.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/universal_pathlib/test_upath.py).
