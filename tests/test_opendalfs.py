import pytest
from fsspec import AbstractFileSystem

from opendalfs import OpendalFileSystem


def test_memory_fs():
    # Default
    OpendalFileSystem("memory")
    # With root
    OpendalFileSystem("memory", root="/tmp")


def test_s3_fs():
    # Default
    OpendalFileSystem("s3", bucket="test", region="us-east-1")
    # With root
    OpendalFileSystem("s3", root="/tmp", bucket="test", region="us-east-1")


@pytest.fixture
def opendal_fs():
    return OpendalFileSystem("memory", root="/tmp")


def test_inheritance(opendal_fs):
    assert isinstance(opendal_fs, AbstractFileSystem)


def test_ls(opendal_fs):
    result = opendal_fs.ls("/test/path")
    assert result == []


def test_mkdir(opendal_fs):
    with pytest.raises(ValueError):
        opendal_fs.mkdir("/test/path/", create_parents=False)
    opendal_fs.mkdir("/test/path/", create_parents=True)
    assert opendal_fs.ls("/test/") == ["test/path/"]
    # Test without trailing slash, will fail
    with pytest.raises(ValueError):
        opendal_fs.mkdir("/test/path/subpath")
    assert opendal_fs.ls("/test/") == ["test/path/"]
    assert opendal_fs.ls("/test/path/") == ["test/path/"]


def test_mkdirs(opendal_fs):
    with pytest.raises(FileExistsError):
        opendal_fs.mkdirs("/test/path/")
    opendal_fs.mkdirs("/test/path/", exist_ok=True)
    assert opendal_fs.ls("/test/") == ["test/path/"]


def test_rmdir(opendal_fs):
    opendal_fs.mkdirs("/test/path/", exist_ok=True)
    opendal_fs.mkdir("/test/another/path/", create_parents=True)
    assert opendal_fs.ls("/test/") == ["test/another/", "test/path/"]
    # Test without trailing slash, will fail
    with pytest.raises(FileNotFoundError):
        opendal_fs.rmdir("/test/path")
    opendal_fs.rmdir("/test/path/", recursive=False)
    with pytest.raises(FileExistsError):
        opendal_fs.rmdir("/test/another/", recursive=False)
    assert opendal_fs.ls("/test/") == ["test/another/"]
    opendal_fs.rmdir("/test/another/", recursive=True)
    assert opendal_fs.ls("/test/") == []


def test_info(opendal_fs):
    result = opendal_fs.info("/")
    assert result == {"name": "/tmp/", "size": 0, "type": "directory"}

    with pytest.raises(FileNotFoundError):
        opendal_fs.info("/test_file")
