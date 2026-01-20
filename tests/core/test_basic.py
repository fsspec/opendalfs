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


def test_pipe_file_with_write_options(memory_fs):
    data = b"hello-world"
    memory_fs.pipe_file(
        "pipe-write.txt",
        data,
        opendal_write_options={"chunk": 4, "concurrent": 2},
    )
    assert memory_fs.cat_file("pipe-write.txt") == data


def test_cat_file_ranges(any_fs):
    data = b"0123456789"
    any_fs.pipe_file("range.txt", data)

    assert any_fs.cat_file("range.txt", start=2, end=5) == b"234"
    assert any_fs.cat_file("range.txt", start=-4) == b"6789"
    assert any_fs.cat_file("range.txt", end=-1) == b"012345678"
    assert any_fs.cat_file("range.txt", start=-4, end=-1) == b"678"
    assert any_fs.cat_file("range.txt", start=5, end=5) == b""


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


def test_copy_and_mv_sync(memory_fs):
    content = b"hello"
    memory_fs.pipe_file("src.txt", content)

    memory_fs.cp_file("src.txt", "copied.txt")
    assert memory_fs.cat_file("src.txt") == content
    assert memory_fs.cat_file("copied.txt") == content

    memory_fs.mv("src.txt", "moved.txt")
    assert not memory_fs.exists("src.txt")
    assert memory_fs.cat_file("moved.txt") == content


@pytest.mark.asyncio
async def test_invalidate_cache_after_mutations(memory_fs):
    await memory_fs._pipe_file("a/one.txt", b"1")
    first = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in first} == {"a/one.txt"}
    assert "a" in memory_fs.dircache

    await memory_fs._pipe_file("a/two.txt", b"2")
    assert "a" not in memory_fs.dircache
    second = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in second} == {"a/one.txt", "a/two.txt"}

    await memory_fs._rm_file("a/one.txt")
    assert "a" not in memory_fs.dircache
    third = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in third} == {"a/two.txt"}
