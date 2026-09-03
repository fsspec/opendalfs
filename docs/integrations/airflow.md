# Apache Airflow

<!-- docs-example-group: integration-airflow -->

Airflow's Task SDK can attach an existing fsspec filesystem to an
`ObjectStoragePath`. This keeps OpenDAL service configuration outside the path.

## Read and write an object

```python
from airflow.sdk import ObjectStoragePath
from airflow.sdk.io import store as airflow_store
from opendalfs import OpendalFileSystem

protocol = "memory"
fs = OpendalFileSystem("memory", skip_instance_cache=True)
conn_id = "opendal-docs"
airflow_store.attach(protocol, conn_id=conn_id, fs=fs)

path = ObjectStoragePath(
    "airflow/result.txt",
    protocol=protocol,
    conn_id=conn_id,
)

with path.open("wb") as stream:
    stream.write(b"hello from Airflow")

with path.open("rb") as stream:
    assert stream.read() == b"hello from Airflow"

path.unlink()
```

## Test coverage

The repository attaches an `opendalfs` instance to Airflow, writes and reads a
binary file, then removes it. The test runs against each configured OpenDAL
test backend.

See
[`tests/integration/airflow/test_object_storage_path.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/airflow/test_object_storage_path.py).
