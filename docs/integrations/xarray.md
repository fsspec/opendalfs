# Xarray

Xarray can read and write Zarr datasets through an fsspec mapping.

## Create a mapper

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "memory",
    skip_instance_cache=True,
)
store = fs.get_mapper("xarray/weather.zarr")
```

## Write and open a dataset

```python
import numpy as np
import xarray as xr

dataset = xr.Dataset({"temperature": ("time", np.array([10, 20, 30]))})
dataset.to_zarr(store, mode="w", consolidated=False)

result = xr.open_dataset(
    store,
    engine="zarr",
    backend_kwargs={"consolidated": False},
)

xr.testing.assert_equal(result, dataset)
```

Keep the mapper alive for as long as the dataset needs to access the store.

## Test coverage

The repository writes an Xarray dataset to an `opendalfs` mapper and opens it
through the Zarr engine with unconsolidated metadata.

See
[`tests/integration/xarray/test_zarr.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/xarray/test_zarr.py).
