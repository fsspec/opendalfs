"""Zarr FsspecStore coverage adapted from Zarr 3.3.0."""

import numpy as np
import pytest

zarr = pytest.importorskip("zarr", minversion="3.3.0")


def test_array_roundtrip_through_fsspec_store_url(opendal_url, opendal_storage_options):
    store = zarr.storage.FsspecStore.from_url(
        f"{opendal_url}/zarr/array",
        storage_options=opendal_storage_options,
    )
    expected = np.arange(6)

    zarr.create_array(store=store, data=expected, chunks=3)
    result = zarr.open_array(store=store)

    np.testing.assert_array_equal(result[:], expected)
