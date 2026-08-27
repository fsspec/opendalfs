"""PyArrow FSSpecHandler coverage adapted from Arrow 25.0.1."""

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


def test_read_parquet_directory_through_fsspec_handler(opendal_fs, opendal_root):
    arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(opendal_fs))
    directory = f"{opendal_root}/pyarrow/arrow-dataset"
    expected = pa.table({"value": [1, 2, 3]})
    pq.write_table(
        expected,
        f"{directory}/part-0.parquet",
        filesystem=arrow_fs,
    )

    result = pq.read_table(directory, filesystem=arrow_fs)

    assert result.equals(expected)
