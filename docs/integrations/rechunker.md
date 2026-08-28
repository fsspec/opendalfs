# Rechunker

<!-- docs-example-group: integration-rechunker -->

Rechunker accepts fsspec mappings for its source, target, and temporary Zarr
stores.

## Rechunk an array

```python
import numpy as np
import zarr
from rechunker.api import rechunk

from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory", skip_instance_cache=True)
source_store = fs.get_mapper("rechunker/source.zarr")
target_store = fs.get_mapper("rechunker/target.zarr")
temp_store = fs.get_mapper("rechunker/temp.zarr")

source = zarr.ones(
    (20, 10),
    chunks=(5, 10),
    dtype="f4",
    store=source_store,
    overwrite=True,
)
plan = rechunk(
    source,
    target_chunks=(10, 5),
    max_mem="1MB",
    target_store=target_store,
    temp_store=temp_store,
)
result = plan.execute()

assert result.chunks == (10, 5)
np.testing.assert_equal(np.asarray(result), 1)
```

## Test coverage

The repository verifies the target chunks, array values, and attribute
preservation.

See
[`tests/integration/rechunker/test_rechunker.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/rechunker/test_rechunker.py).
