import pytest
from opendalfs_service_memory import MemoryConfig


@pytest.fixture
def opendal_fs():
    return MemoryConfig().build()


# TODO: we need to find a way to make it work
# def test_inheritance(opendal_fs):
#    assert isinstance(opendal_fs, AbstractFileSystem)


def test_ls(opendal_fs):
    result = opendal_fs.ls("/test/path")
    assert result == []
