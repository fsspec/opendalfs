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


@pytest.mark.parametrize("entry_style", ["url", "open-file"])
def test_single_hdf5_to_zarr_from_opendalfs(
    hdf_dataset,
    entry_style,
    opendal_fs,
    opendal_root,
    opendal_url,
):
    """Build and read references using kerchunk's two supported HDF inputs.

    Adapted from kerchunk 0.2.10 ``tests/test_hdf.py::test_times`` and
    ``test_times_str``.
    """
    expected, payload = hdf_dataset
    path = f"{opendal_root}/kerchunk-{entry_style}/source.nc"
    url = f"{opendal_url}/kerchunk-{entry_style}/source.nc"
    opendal_fs.pipe_file(path, payload)

    if entry_style == "url":
        translator = kerchunk_hdf.SingleHdf5ToZarr(url)
    else:
        source = opendal_fs.open(path, "rb")
        translator = kerchunk_hdf.SingleHdf5ToZarr(source, url)

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
