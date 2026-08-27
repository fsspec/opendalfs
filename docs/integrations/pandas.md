# pandas

pandas accepts fsspec URLs through `storage_options`. Its Parquet methods can
also accept an explicit fsspec filesystem.

## Read a CSV from a URL

Register the memory service and create a small input file. The same URL pattern
works for configured storage services.

```python
import fsspec
import pandas as pd

from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
fs.pipe_file("data/events.csv", b"name,value\nalice,1\nbob,2\n")

frame = pd.read_csv("opendal+memory:///data/events.csv")
assert frame["value"].tolist() == [1, 2]
```

For object stores, the bucket comes from the URL and other service options pass
through `storage_options` to OpenDAL.

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

- CSV reads from an `opendal+` URL
- Parquet URL round trips through PyArrow
- Parquet round trips with an explicit filesystem

The test runs against memory, local filesystem, and MinIO-backed S3 fixtures.
See
[`tests/integration/pandas/test_pandas.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pandas/test_pandas.py).
