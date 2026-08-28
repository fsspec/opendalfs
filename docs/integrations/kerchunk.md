# Kerchunk

<!-- docs-example-group: integration-kerchunk -->

Kerchunk can read an HDF5 file through `opendalfs` and expose its generated
references as a Zarr-compatible store.

## Generate and read references

```python
from pathlib import Path

import fsspec
import numpy as np
import xarray as xr
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.utils import refs_as_store

from opendalfs import register_opendal_service

expected = xr.DataArray(
    np.arange(6, dtype="float64").reshape(2, 3),
    dims=["x", "y"],
    name="value",
).to_dataset()
local_path = Path("source.nc")
expected.to_netcdf(local_path, engine="h5netcdf")

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
path = "kerchunk/source.nc"
url = fs.unstrip_protocol(path)
fs.pipe_file(path, local_path.read_bytes())

translator = SingleHdf5ToZarr(url)
try:
    references = translator.translate()
finally:
    translator.close()

result = xr.open_dataset(
    refs_as_store(references, fs=fs),
    engine="zarr",
    zarr_format=2,
    backend_kwargs={"consolidated": False},
)
xr.testing.assert_equal(result, expected)
```

## Test coverage

The repository tests URL and file-like object inputs, reference generation,
and reading the result through Xarray.

See
[`tests/integration/kerchunk/test_kerchunk.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/kerchunk/test_kerchunk.py).
