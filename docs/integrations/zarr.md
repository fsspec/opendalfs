# Zarr

Zarr 3 provides `FsspecStore` for asynchronous access to any fsspec URL.

## Create a store from a URL

```python
import zarr

from opendalfs import register_opendal_service

register_opendal_service("memory")
store = zarr.storage.FsspecStore.from_url(
    "opendal+memory:///arrays/example",
)
```

## Write and read an array

```python
import numpy as np

expected = np.arange(6)
zarr.create_array(store=store, data=expected, chunks=3)

array = zarr.open_array(store=store)
np.testing.assert_array_equal(array[:], expected)
```

Zarr controls the object layout under the URL. Give each array or group its own
prefix.

## Test coverage

The repository creates and reopens a chunked Zarr array through
`FsspecStore.from_url`. The test runs against memory, local filesystem, and
MinIO-backed S3 fixtures.

See
[`tests/integration/zarr/test_fsspec_store.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/zarr/test_fsspec_store.py).
