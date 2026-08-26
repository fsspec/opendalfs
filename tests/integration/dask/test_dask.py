"""Dask dataframe fsspec entry points against OpenDAL services.

Adapted from Dask ``test_read_csv_files`` and
``test_multiple_read_csv_has_deterministic_name`` in
``dask/dataframe/io/tests/test_csv.py``, plus
``test_fsspec_to_parquet_filesystem_option`` in
``dask/dataframe/io/tests/test_parquet.py``, from the Dask 2026.8.0 release.
"""

import dask.dataframe as dd
import pandas as pd
import pandas.testing as tm
import pytest

from opendalfs import register_opendal_service


@pytest.fixture
def opendal_memory_url():
    import fsspec

    register_opendal_service("memory")
    url = "opendal+memory://test/dask"
    fs, path = fsspec.core.url_to_fs(url)
    if fs.exists(path):
        fs.rm(path, recursive=True)
    yield fs, url, path
    if fs.exists(path):
        fs.rm(path, recursive=True)


def test_read_csv_url_glob_and_tokenization(opendal_memory_url):
    fs, base_url, base_path = opendal_memory_url
    files = {
        "2014-01-01.csv": b"name,amount,id\nAlice,100,1\nBob,200,2\n",
        "2014-01-02.csv": b"name,amount,id\nCharlie,300,3\n",
    }
    for name, content in files.items():
        fs.pipe_file(f"{base_path}/{name}", content)

    first = dd.read_csv(f"{base_url}/2014-01-*.csv")
    second = dd.read_csv(f"{base_url}/2014-01-*.csv")
    expected = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "amount": [100, 200, 300],
        "id": [1, 2, 3],
    })

    assert first._name == second._name
    assert sorted(first.dask.keys(), key=str) == sorted(second.dask.keys(), key=str)
    tm.assert_frame_equal(
        first.compute().reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_read_parquet_url(s3_fs, s3_config):
    expected = pd.DataFrame({"a": range(10)})
    path = "dask/url.parquet"
    expected.to_parquet(path, filesystem=s3_fs)
    url = f"opendal+s3://{s3_config.bucket}/{path}"
    storage_options = {
        "endpoint": s3_config.endpoint,
        "region": s3_config.region,
        "access_key_id": s3_config.access_key_id,
        "secret_access_key": s3_config.secret_access_key,
    }

    result = dd.read_parquet(url, storage_options=storage_options).compute()

    tm.assert_frame_equal(result, expected)


def test_read_parquet_filesystem(s3_fs):
    expected = pd.DataFrame({"a": range(10)})
    path = "dask/filesystem.parquet"
    expected.to_parquet(path, filesystem=s3_fs)

    result = dd.read_parquet(path, filesystem=s3_fs).compute()

    tm.assert_frame_equal(result, expected)
