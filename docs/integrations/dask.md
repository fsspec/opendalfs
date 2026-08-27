# Dask

Dask DataFrame resolves fsspec URLs for CSV and Parquet input. It also accepts
an explicit filesystem for Parquet datasets.

## Read a CSV glob

```python
import dask.dataframe as dd
import fsspec

from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
fs.pipe_file("events/2026-01.csv", b"name,value\nalice,1\n")
fs.pipe_file("events/2026-02.csv", b"name,value\nbob,2\n")

frame = dd.read_csv("opendal+memory:///events/2026-*.csv")
result = frame.compute()
assert result["value"].tolist() == [1, 2]
```

The filesystem must support glob expansion and deterministic tokenization.
Dask uses both while constructing the task graph.

## Read Parquet with a filesystem

```python
import dask.dataframe as dd
import pandas as pd

expected = pd.DataFrame({"name": ["alice", "bob"], "value": [1, 2]})
path = "tables/events.parquet"
expected.to_parquet(path, filesystem=fs)

frame = dd.read_parquet(path, filesystem=fs)
result = frame.compute()

pd.testing.assert_frame_equal(result, expected, check_dtype=False)
```

## Test coverage

The repository tests:

- CSV URL glob expansion
- deterministic tokenization of repeated reads
- Parquet reads from a URL
- Parquet reads with an explicit filesystem

See
[`tests/integration/dask/test_dask.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/dask/test_dask.py).
