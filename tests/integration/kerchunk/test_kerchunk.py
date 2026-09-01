import pytest

np = pytest.importorskip("numpy")
xr = pytest.importorskip("xarray")
kerchunk_hdf = pytest.importorskip("kerchunk.hdf")
kerchunk_utils = pytest.importorskip("kerchunk.utils")


@pytest.fixture
def hdf_dataset(tmp_path):
    """Small version of kerchunk's ``times_data`` HDF5 fixture."""
    expected = xr.DataArray(
        np.arange(24, dtype="float64").reshape(2, 3, 4),
        dims=["time", "lat", "lon"],
        name="value",
    ).to_dataset()
    local_path = tmp_path / "source.nc"
    expected.to_netcdf(local_path, engine="h5netcdf")
    return expected, local_path.read_bytes()


def test_single_hdf5_to_zarr_from_url(
    hdf_dataset,
    s3_fs,
    opendal_s3_root,
    opendal_s3_url,
):
    """Build and read references through an installed OpenDAL URL.

    Adapted from kerchunk 0.2.10 ``tests/test_hdf.py::test_times_str``.
    """
    expected, payload = hdf_dataset
    path = f"{opendal_s3_root}/kerchunk-url/source.nc"
    url = f"{opendal_s3_url}/kerchunk-url/source.nc"
    s3_fs.pipe_file(path, payload)

    translator = kerchunk_hdf.SingleHdf5ToZarr(url)

    try:
        references = translator.translate()
    finally:
        translator.close()

    result = xr.open_dataset(
        kerchunk_utils.refs_as_store(references, fs=s3_fs),
        engine="zarr",
        zarr_format=2,
        backend_kwargs={"consolidated": False},
    )
    xr.testing.assert_equal(result, expected)


def test_single_hdf5_to_zarr_from_open_file(
    hdf_dataset,
    opendal_fs,
    opendal_root,
):
    """Build and read references through an explicit OpenDAL filesystem.

    Adapted from kerchunk 0.2.10 ``tests/test_hdf.py::test_times``.
    """
    expected, payload = hdf_dataset
    path = f"{opendal_root}/kerchunk-open-file/source.nc"
    opendal_fs.pipe_file(path, payload)
    source = opendal_fs.open(path, "rb")
    translator = kerchunk_hdf.SingleHdf5ToZarr(source, path)

    try:
        references = translator.translate()
    finally:
        translator.close()

    result = xr.open_dataset(
        kerchunk_utils.refs_as_store(references, fs=opendal_fs),
        engine="zarr",
        zarr_format=2,
        backend_kwargs={"consolidated": False},
    )
    xr.testing.assert_equal(result, expected)
