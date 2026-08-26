import fsspec
import pytest

from opendalfs import register_opendal_service

np = pytest.importorskip("numpy")
zarr = pytest.importorskip("zarr")
rechunker_api = pytest.importorskip("rechunker.api")


def test_rechunk_between_opendalfs_mappers():
    """Rechunk an array between stores created by ``fs.get_mapper()``.

    Adapted from rechunker 0.5.4
    ``tests/test_rechunk.py::test_rechunk_array`` and its mapper-backed store
    fixtures.
    """
    register_opendal_service("memory")
    fs = fsspec.filesystem("opendal+memory", skip_instance_cache=True)
    source_store = fs.get_mapper("source.zarr")
    target_store = fs.get_mapper("target.zarr")
    temp_store = fs.get_mapper("temp.zarr")

    source = zarr.ones(
        (100, 50), chunks=(10, 50), dtype="f4", store=source_store, overwrite=True
    )
    source.attrs["foo"] = "bar"

    rechunked = rechunker_api.rechunk(
        source,
        target_chunks=(20, 10),
        max_mem="10MB",
        target_store=target_store,
        temp_store=temp_store,
    )
    result = rechunked.execute()

    assert result.chunks == (20, 10)
    assert dict(result.attrs) == {"foo": "bar"}
    np.testing.assert_equal(np.asarray(result), 1)
