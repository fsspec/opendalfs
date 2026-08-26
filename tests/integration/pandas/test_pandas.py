"""pandas fsspec entry points against OpenDAL services.

Adapted from pandas ``test_read_csv`` and ``test_arrowparquet_options`` in
``pandas/tests/io/test_fsspec.py``, plus ``test_s3_roundtrip_explicit_fs`` in
``pandas/tests/io/test_parquet.py``, from the pandas 3.0.5 release.
"""

import pandas as pd
import pandas.testing as tm


def test_read_csv_url(opendal_fs, opendal_root, opendal_url):
    expected = pd.DataFrame({
        "int": [1, 3],
        "float": [2.0, None],
        "str": ["t", "s"],
        "dt": pd.date_range("2018-06-18", periods=2),
    })
    opendal_fs.pipe_file(
        f"{opendal_root}/pandas/test.csv",
        expected.to_csv(index=False).encode(),
    )

    result = pd.read_csv(f"{opendal_url}/pandas/test.csv", parse_dates=["dt"])

    tm.assert_frame_equal(result, expected)


def test_parquet_url_roundtrip(opendal_url):
    expected = pd.DataFrame({"a": [0, 1], "b": ["x", "y"]})
    url = f"{opendal_url}/pandas/test.parquet"

    expected.to_parquet(url, engine="pyarrow", compression=None)
    result = pd.read_parquet(url, engine="pyarrow")

    tm.assert_frame_equal(result, expected)


def test_parquet_filesystem_roundtrip(opendal_fs, opendal_root):
    expected = pd.DataFrame({"a": [0, 1], "b": ["x", "y"]})
    path = f"{opendal_root}/pandas/filesystem.parquet"

    expected.to_parquet(
        path,
        engine="pyarrow",
        filesystem=opendal_fs,
        compression=None,
    )
    result = pd.read_parquet(
        path,
        engine="pyarrow",
        filesystem=opendal_fs,
    )

    tm.assert_frame_equal(result, expected)
