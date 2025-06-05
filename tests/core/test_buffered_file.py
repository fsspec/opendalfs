import pytest
from opendalfs.file import OpendalBufferedFile
from opendalfs.fs import OpendalFileSystem


@pytest.fixture
def fs_dir():
    return OpendalFileSystem("fs", root="/tmp/")


@pytest.fixture
def file_path():
    return "testdir/testfile.txt"


def test_fetch_range_basic(fs_dir, file_path):
    fs_dir.write(file_path, b"Hello, world!")
    f = OpendalBufferedFile(fs_dir, file_path, mode="rb")
    assert f._fetch_range(0, 5) == b"Hello"
    assert f._fetch_range(7, 12) == b"world"


def test_fetch_range_out_of_bounds(fs_dir, file_path):
    fs_dir.write(file_path, b"short")
    f = OpendalBufferedFile(fs_dir, file_path, mode="rb")
    assert f._fetch_range(0, 100) == b"short"
    assert f._fetch_range(10, 20) == b""


def test_initiate_upload(fs_dir, file_path):
    f = OpendalBufferedFile(fs_dir, file_path, mode="wb")
    f._initiate_upload()
    assert f.buffer is not None


def test_commit_upload(fs_dir, file_path):
    f = OpendalBufferedFile(fs_dir, file_path, mode="wb")
    f.buffer.write(b"OpenDAL test")
    f._commit_upload()
    assert fs_dir.read(file_path) == b"OpenDAL test"


def test_commit_upload_empty_buffer(fs_dir, file_path):
    f = OpendalBufferedFile(fs_dir, file_path, mode="wb")
    f._commit_upload()  # should not raise even if buffer is empty
    assert fs_dir.exists(file_path) is True
    assert fs_dir.read(file_path) == b""


def test_upload_chunk_noop(fs_dir, file_path):
    f = OpendalBufferedFile(fs_dir, file_path, mode="wb")
    f._upload_chunk()  # should not error
    f._upload_chunk(final=True)  # should not error


def test_close_writes_buffer(fs_dir, file_path):
    with OpendalBufferedFile(fs_dir, file_path, mode="wb") as f:
        f.buffer.write(b"closing buffer test")
    assert fs_dir.read(file_path) == b"closing buffer test"


def test_close_does_not_write_on_read_mode(fs_dir, file_path):
    fs_dir.write(file_path, b"read test")
    f = OpendalBufferedFile(fs_dir, file_path, mode="rb")
    f.close()
    assert fs_dir.read(file_path) == b"read test"
