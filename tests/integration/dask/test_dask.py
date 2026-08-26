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


def test_read_csv_url_glob_and_tokenization(opendal_storage):
    files = {
        "2014-01-01.csv": b"name,amount,id\nAlice,100,1\nBob,200,2\n",
        "2014-01-02.csv": b"name,amount,id\nCharlie,300,3\n",
    }
    for name, content in files.items():
        opendal_storage.fs.pipe_file(opendal_storage.path(f"dask/{name}"), content)

    url = opendal_storage.url("dask/2014-01-*.csv")
    first = dd.read_csv(url, storage_options=opendal_storage.storage_options)
    second = dd.read_csv(url, storage_options=opendal_storage.storage_options)
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


def test_read_parquet_url(opendal_storage):
    expected = pd.DataFrame({"a": range(10)})
    path = opendal_storage.path("dask/url.parquet")
    expected.to_parquet(path, filesystem=opendal_storage.fs)

    result = dd.read_parquet(
        opendal_storage.url("dask/url.parquet"),
        storage_options=opendal_storage.storage_options,
    ).compute()

    tm.assert_frame_equal(result, expected)


def test_read_parquet_filesystem(opendal_storage):
    expected = pd.DataFrame({"a": range(10)})
    path = opendal_storage.path("dask/filesystem.parquet")
    expected.to_parquet(path, filesystem=opendal_storage.fs)

    result = dd.read_parquet(path, filesystem=opendal_storage.fs).compute()

    tm.assert_frame_equal(result, expected)
