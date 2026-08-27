"""Fsspec DataPipe coverage adapted from torchdata 0.9.0."""

import fsspec
import pytest

torchdata_iter = pytest.importorskip(
    "torchdata.datapipes.iter",
    reason="torchdata 0.9.0 only publishes wheels through Python 3.12",
)


def test_list_and_open_files_by_fsspec(opendal_url):
    root = f"{opendal_url}/torchdata"
    expected = {"one.txt": "one", "two.txt": "two"}

    for name, content in expected.items():
        with fsspec.open(f"{root}/{name}", "w") as stream:
            stream.write(content)

    files = torchdata_iter.FSSpecFileLister(root=root, masks="*.txt")
    opened_files = torchdata_iter.FSSpecFileOpener(files)

    assert {
        path.rsplit("/", 1)[-1]: stream.read() for path, stream in opened_files
    } == expected
