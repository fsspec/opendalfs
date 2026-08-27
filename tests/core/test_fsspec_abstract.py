"""Behavior contracts shared by fsspec and OpenDAL-backed filesystems."""

import posixpath

import pytest
from fsspec.tests.abstract import AbstractFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.open import AbstractOpenTests
from fsspec.tests.abstract.pipe import AbstractPipeTests
from fsspec.tests.abstract.put import AbstractPutTests

from opendalfs import OpendalFileSystem


class _OpendalFixtures(AbstractFixtures):
    """Shared fixtures for fsspec contracts backed by OpenDAL."""

    @pytest.fixture
    def fs_join(self):
        return posixpath.join

    @pytest.fixture
    def fs_path(self):
        return "contract"


class _MemoryFixtures(_OpendalFixtures):
    """Configure the OpenDAL memory backend for acceptance tests."""

    @pytest.fixture
    def fs(self):
        return OpendalFileSystem(
            scheme="memory",
            asynchronous=False,
            skip_instance_cache=True,
        )


class _S3Fixtures(_OpendalFixtures):
    """Configure the OpenDAL S3 backend against MinIO for acceptance tests."""

    @pytest.fixture
    def fs_path(self, s3_config):
        return f"{s3_config.bucket}/contract"

    @pytest.fixture
    def fs(self, s3_fs):
        return s3_fs


class TestMemoryCopy(AbstractCopyTests, _MemoryFixtures):
    pass


class TestMemoryGet(AbstractGetTests, _MemoryFixtures):
    pass


class TestMemoryPut(AbstractPutTests, _MemoryFixtures):
    pass


class TestMemoryPipe(AbstractPipeTests, _MemoryFixtures):
    pass


class TestMemoryOpen(AbstractOpenTests, _MemoryFixtures):
    pass


class TestS3Copy(AbstractCopyTests, _S3Fixtures):
    pass


class TestS3Get(AbstractGetTests, _S3Fixtures):
    pass


class TestS3Put(AbstractPutTests, _S3Fixtures):
    pass


class TestS3Pipe(AbstractPipeTests, _S3Fixtures):
    pass


class TestS3Open(AbstractOpenTests, _S3Fixtures):
    pass


def test_empty_directory_behavior(any_fs):
    any_fs.makedirs("empty/nested")

    assert any_fs.isdir("empty")
    assert any_fs.isdir("empty/nested")
    assert any_fs.ls("empty/nested") == []

    any_fs.touch("empty/file")
    with pytest.raises(OSError, match="Directory not empty"):
        any_fs.rmdir("empty")
    assert any_fs.isfile("empty/file")


def test_makedirs_rejects_existing_file(any_fs):
    any_fs.pipe_file("already-a-file", b"data")

    with pytest.raises(FileExistsError):
        any_fs.makedirs("already-a-file", exist_ok=True)


def test_s3_change_tokens_reflect_same_size_overwrite_after_listing(s3_fs):
    s3_fs.pipe_file("cached", b"old")
    s3_fs.ls("")
    old_checksum = s3_fs.checksum("cached")
    old_ukey = s3_fs.ukey("cached")

    s3_fs.pipe_file("cached", b"new")

    assert s3_fs.cat_file("cached") == b"new"
    assert s3_fs.checksum("cached") != old_checksum
    assert s3_fs.ukey("cached") != old_ukey


def test_checksum_is_available_without_backend_etag(memory_fs):
    memory_fs.pipe_file("data", b"content")

    assert memory_fs.checksum("data") == memory_fs.checksum("data")


def test_recursive_copy_preserves_empty_directories(any_fs):
    any_fs.makedirs("source/empty")

    any_fs.copy("source", "target", recursive=True)

    assert any_fs.isdir("target/empty")
