"""pandas fsspec entry points against OpenDAL's memory service.

Adapted from pandas ``test_read_csv`` and ``test_arrowparquet_options`` in
``pandas/tests/io/test_fsspec.py``, plus ``test_s3_roundtrip_explicit_fs`` in
``pandas/tests/io/test_parquet.py``, from the pandas 3.0.5 release.
"""

import pandas as pd
import pandas.testing as tm
import pytest

from opendalfs import register_opendal_service


@pytest.fixture
def opendal_memory_url():
    import fsspec

    register_opendal_service("memory")
    url = "opendal+memory://test/pandas"
    fs, path = fsspec.core.url_to_fs(url)
    if fs.exists(path):
        fs.rm(path, recursive=True)
    yield fs, url, path
    if fs.exists(path):
        fs.rm(path, recursive=True)


def test_read_csv_url(opendal_memory_url):
    fs, base_url, base_path = opendal_memory_url
    expected = pd.DataFrame({
        "int": [1, 3],
        "float": [2.0, None],
        "str": ["t", "s"],
        "dt": pd.date_range("2018-06-18", periods=2),
    })
    fs.pipe_file(
        f"{base_path}/test.csv",
        expected.to_csv(index=False).encode(),
    )

    result = pd.read_csv(f"{base_url}/test.csv", parse_dates=["dt"])

    tm.assert_frame_equal(result, expected)


def test_parquet_url_roundtrip(opendal_memory_url):
    _, base_url, _ = opendal_memory_url
    expected = pd.DataFrame({"a": [0, 1], "b": ["x", "y"]})
    url = f"{base_url}/test.parquet"

    expected.to_parquet(url, engine="pyarrow", compression=None)
    result = pd.read_parquet(url, engine="pyarrow")

    tm.assert_frame_equal(result, expected)


def test_parquet_filesystem_roundtrip(opendal_memory_url):
    fs, _, base_path = opendal_memory_url
    expected = pd.DataFrame({"a": [0, 1], "b": ["x", "y"]})
    path = f"{base_path}/filesystem.parquet"

    expected.to_parquet(path, engine="pyarrow", filesystem=fs, compression=None)
    result = pd.read_parquet(path, engine="pyarrow", filesystem=fs)

    tm.assert_frame_equal(result, expected)
