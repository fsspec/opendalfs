"""Xarray fsspec mapper coverage adapted from Xarray 2026.7.0."""

import numpy as np
import pytest

xr = pytest.importorskip("xarray")


@pytest.mark.parametrize("entry_point", ["open-dataset", "open-zarr"])
def test_open_zarr_dataset_from_opendal_mapper(entry_point, opendal_fs, opendal_root):
    store = opendal_fs.get_mapper(f"{opendal_root}/xarray/{entry_point}.zarr")
    expected = xr.Dataset({"temperature": ("time", np.array([10, 20, 30]))})
    expected.to_zarr(store, mode="w", consolidated=False)

    if entry_point == "open-dataset":
        result = xr.open_dataset(
            store,
            engine="zarr",
            backend_kwargs={"consolidated": False},
        )
    else:
        result = xr.open_zarr(store, consolidated=False)

    xr.testing.assert_equal(result, expected)
