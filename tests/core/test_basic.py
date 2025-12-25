"""Core functionality tests for filesystem creation and basic operations."""

import logging

import pytest

logger = logging.getLogger(__name__)


def test_write_read(s3_fs):
    """Test basic write and read operations."""
    for fs in [s3_fs]:
        content = b"test content"
        fs.pipe_file("test.txt", content)
        assert fs.cat_file("test.txt") == content


@pytest.mark.asyncio
async def test_ls_and_info_fsspec_shape(memory_fs):
    await memory_fs._pipe_file("a/b.txt", b"hello")
    await memory_fs._pipe_file("a/c/d.txt", b"x")

    file_info = await memory_fs._info("a/b.txt")
    assert set(file_info) == {"name", "size", "type"}
    assert file_info["name"] == "a/b.txt"
    assert file_info["size"] == 5
    assert file_info["type"] == "file"

    dir_info = await memory_fs._info("a/")
    assert set(dir_info) == {"name", "size", "type"}
    assert dir_info["name"] == "a/"
    assert dir_info["size"] == 0
    assert dir_info["type"] == "directory"

    paths = await memory_fs._ls("a", detail=False)
    assert set(paths) == {"a/b.txt", "a/c/"}

    detailed = await memory_fs._ls("a", detail=True)
    assert all(set(item) == {"name", "size", "type"} for item in detailed)
    by_name = {item["name"]: item for item in detailed}
    assert by_name["a/b.txt"]["size"] == 5
    assert by_name["a/b.txt"]["type"] == "file"
    assert by_name["a/c/"]["size"] == 0
    assert by_name["a/c/"]["type"] == "directory"
