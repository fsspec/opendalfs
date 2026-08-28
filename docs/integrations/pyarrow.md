# PyArrow

PyArrow has its own filesystem interface. Wrap an `opendalfs` instance with
PyArrow's `FSSpecHandler` before passing it to Parquet or Dataset APIs.

## Create the adapter

```python
import pyarrow.fs as pafs

from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "memory",
    skip_instance_cache=True,
)
arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(fs))
```

## Read a Parquet dataset

```python
import pyarrow as pa
import pyarrow.parquet as pq

expected = pa.table({"value": [1, 2, 3]})
directory = "datasets/events"
pq.write_table(
    expected,
    f"{directory}/part-0.parquet",
    filesystem=arrow_fs,
)

table = pq.read_table(
    directory,
    filesystem=arrow_fs,
)

assert table.equals(expected)
```

Use paths in the form expected by the fsspec filesystem. See
{doc}`../user-guide/connecting-to-storage` when adapting the example to a
bucket-scoped service.

## Test coverage

The repository writes a Parquet file through `FSSpecHandler`, then reads the
containing directory as a dataset. This exercises directory discovery as well
as file I/O.

See
[`tests/integration/pyarrow/test_dataset.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pyarrow/test_dataset.py).
