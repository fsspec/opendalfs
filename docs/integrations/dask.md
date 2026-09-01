# Dask

Dask DataFrame resolves installed fsspec URL protocols for CSV and Parquet
input. Its Parquet APIs also accept an explicit filesystem, which works with
every OpenDAL service.

## Read Parquet with a filesystem

```python
import dask.dataframe as dd
import pandas as pd
from opendalfs import OpendalFileSystem

expected = pd.DataFrame({"name": ["alice", "bob"], "value": [1, 2]})
fs = OpendalFileSystem("memory")
path = "tables/events.parquet"
expected.to_parquet(path, filesystem=fs)

frame = dd.read_parquet(path, filesystem=fs)
result = frame.compute()

pd.testing.assert_frame_equal(result, expected, check_dtype=False)
```

## Test coverage

The repository tests:

- CSV URL glob expansion and deterministic tokenization through `opendal+s3`
- Parquet reads from an `opendal+s3` URL
- Parquet reads with an explicit filesystem

See
[`tests/integration/dask/test_dask.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/dask/test_dask.py).
