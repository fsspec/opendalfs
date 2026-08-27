"""Xarray fsspec mapper coverage adapted from Xarray 2026.7.0."""

import numpy as np
import pytest

xr = pytest.importorskip("xarray")


def test_open_dataset_from_opendal_mapper(opendal_fs, opendal_root):
    store = opendal_fs.get_mapper(f"{opendal_root}/xarray/out.zarr")
    expected = xr.Dataset({"temperature": ("time", np.array([10, 20, 30]))})
    expected.to_zarr(store, mode="w", consolidated=False)

    result = xr.open_dataset(
        store,
        engine="zarr",
        backend_kwargs={"consolidated": False},
    )

    xr.testing.assert_equal(result, expected)
