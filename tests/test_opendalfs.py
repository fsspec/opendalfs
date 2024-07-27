import pytest
from opendalfs_service_memory import MemoryFileSystem
from opendalfs_service_s3 import S3FileSystem




def test_memory_fs():
    # Default
    MemoryFileSystem()
    # With root
    MemoryFileSystem(root="/tmp")


def test_s3_fs():
    # Default
    S3FileSystem(bucket="test", region="us-east-1")
    # With root
    S3FileSystem(root="/tmp", bucket="test", region="us-east-1")



@pytest.fixture
def opendal_fs():
    return MemoryFileSystem(root="/tmp")

# TODO: we need to find a way to make it work
# def test_inheritance(opendal_fs):
#    assert isinstance(opendal_fs, AbstractFileSystem)


def test_ls(opendal_fs):
    result = opendal_fs.ls("/test/path")
    assert result == []
