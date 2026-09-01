# pandas

pandas accepts file-like objects and explicit fsspec filesystems. Its URL APIs
can also use the installed `opendal+s3`, `opendal+gcs`, and `opendal+azblob`
protocols.

## Read a CSV from a file-like object

Construct any OpenDAL service directly and pass its opened file to pandas:

```python
import pandas as pd
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
fs.pipe_file("data/events.csv", b"name,value\nalice,1\nbob,2\n")

with fs.open("data/events.csv", "rb") as stream:
    frame = pd.read_csv(stream)
assert frame["value"].tolist() == [1, 2]
```

## Read and write Parquet with a filesystem

```python
path = "data/events.parquet"
frame.to_parquet(path, filesystem=fs, engine="pyarrow")
result = pd.read_parquet(path, filesystem=fs, engine="pyarrow")

pd.testing.assert_frame_equal(result, frame)
```

See {doc}`../user-guide/connecting-to-storage` for path and authority handling
with bucket-scoped services.

## Test coverage

The repository tests:

- CSV and Parquet URL operations through `opendal+s3`
- Parquet round trips with an explicit filesystem

The test runs against memory, local filesystem, and MinIO-backed S3 fixtures.
See
[`tests/integration/pandas/test_pandas.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pandas/test_pandas.py).
