import pytest
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


# TODO: we need to find a way to make it work
# def test_inheritance(opendal_fs):
#    assert isinstance(opendal_fs, AbstractFileSystem)


def test_ls(opendal_fs):
    result = opendal_fs.ls("/test/path")
    assert result == []
