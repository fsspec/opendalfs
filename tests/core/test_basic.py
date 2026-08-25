"""Core functionality tests for filesystem creation and basic operations."""

import logging
from pathlib import PurePosixPath

import pytest

logger = logging.getLogger(__name__)


def test_fsspec_configuration_is_accepted():
    from opendalfs import OpendalFileSystem

    fs = OpendalFileSystem(
        scheme="memory",
        batch_size=2,
        use_listings_cache=False,
        listings_expiry_time=10,
        max_paths=2,
        skip_instance_cache=True,
    )

    fs.pipe_file("configured.txt", b"works")

    assert fs.cat_file("configured.txt") == b"works"


def test_pathlike_inputs_follow_fsspec_path_conventions(memory_fs):
    path = PurePosixPath("pathlike/file.txt")

    memory_fs.pipe_file(path, b"works")

    assert memory_fs.cat_file(path) == b"works"


def test_write_read(s3_fs):
    """Test basic write and read operations."""
    for fs in [s3_fs]:
        content = b"test content"
        fs.pipe_file("test.txt", content)
        assert fs.cat_file("test.txt") == content


def test_file_transfer_accepts_block_size(memory_fs, tmp_path):
    source = tmp_path / "source.bin"
    downloaded = tmp_path / "downloaded.bin"
    content = b"transfer buffer boundary"
    source.write_bytes(content)

    memory_fs.put_file(source, "target.bin", block_size=3)
    memory_fs.get_file("target.bin", downloaded, block_size=5)

    assert downloaded.read_bytes() == content


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
    assert file_info["name"] == "a/b.txt"
    assert file_info["size"] == 5
    assert file_info["type"] == "file"

    dir_info = await memory_fs._info("a/")
    assert dir_info["name"] == "a"
    assert dir_info["size"] == 0
    assert dir_info["type"] == "directory"

    paths = await memory_fs._ls("a", detail=False)
    assert set(paths) == {"a/b.txt", "a/c"}

    detailed = await memory_fs._ls("a", detail=True)
    by_name = {item["name"]: item for item in detailed}
    assert by_name["a/b.txt"]["size"] == 5
    assert by_name["a/b.txt"]["type"] == "file"
    assert by_name["a/c"]["size"] == 0
    assert by_name["a/c"]["type"] == "directory"


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
async def test_directory_listing_reflects_mutations(memory_fs):
    await memory_fs._pipe_file("a/one.txt", b"1")
    first = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in first} == {"a/one.txt"}

    await memory_fs._pipe_file("a/two.txt", b"2")
    second = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in second} == {"a/one.txt", "a/two.txt"}

    await memory_fs._rm_file("a/one.txt")
    third = await memory_fs._ls("a", detail=True)
    assert {item["name"] for item in third} == {"a/two.txt"}


def test_recursive_listing_descends_into_cached_directories(memory_fs):
    memory_fs.pipe_file("seed/ds/part=p0/data.parquet", b"p0")
    memory_fs.pipe_file("seed/ds/part=p1/data.parquet", b"p1")

    memory_fs.ls("seed/ds", detail=True)
    assert memory_fs.ls("seed/ds/part=p0", detail=False) == [
        "seed/ds/part=p0/data.parquet"
    ]
    assert set(memory_fs.find("seed/ds", withdirs=True)) == {
        "seed/ds",
        "seed/ds/part=p0",
        "seed/ds/part=p0/data.parquet",
        "seed/ds/part=p1",
        "seed/ds/part=p1/data.parquet",
    }
