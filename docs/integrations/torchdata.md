# TorchData

<!-- docs-example-group: integration-torchdata -->

TorchData's fsspec DataPipes can list and open files through the installed `opendal+s3` protocol.
TorchData 0.9 publishes compatible wheels for Python 3.12.

## List and open files

```python
import fsspec
from torchdata.datapipes import iter as torchdata_iter

protocol = "opendal+s3"
fsspec.config.conf[protocol] = {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
}
root = f"{protocol}://test-bucket/torchdata"
expected = {"one.txt": "one", "two.txt": "two"}

for name, content in expected.items():
    with fsspec.open(f"{root}/{name}", "w") as stream:
        stream.write(content)

files = torchdata_iter.FSSpecFileLister(root=root, masks="*.txt")
opened_files = torchdata_iter.FSSpecFileOpener(files)
result = {path.rsplit("/", 1)[-1]: stream.read() for path, stream in opened_files}

assert result == expected
```

## Test coverage

The repository writes two files, lists them through `FSSpecFileLister`, and
reads them through `FSSpecFileOpener`.

See
[`tests/integration/torchdata/test_fsspec_datapipes.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/torchdata/test_fsspec_datapipes.py).
