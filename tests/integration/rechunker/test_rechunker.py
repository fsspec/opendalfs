import pytest

np = pytest.importorskip("numpy")
zarr = pytest.importorskip("zarr")
rechunker_api = pytest.importorskip("rechunker.api")


def test_rechunk_between_opendalfs_mappers(opendal_storage):
    """Rechunk an array between stores created by ``fs.get_mapper()``.

    Adapted from rechunker 0.5.4
    ``tests/test_rechunk.py::test_rechunk_array`` and its mapper-backed store
    fixtures.
    """
    source_store = opendal_storage.fs.get_mapper(
        opendal_storage.path("rechunker/source.zarr")
    )
    target_store = opendal_storage.fs.get_mapper(
        opendal_storage.path("rechunker/target.zarr")
    )
    temp_store = opendal_storage.fs.get_mapper(
        opendal_storage.path("rechunker/temp.zarr")
    )

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
