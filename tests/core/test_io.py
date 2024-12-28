"""Core functionality tests for IO operations."""

import pytest


def test_write_read(memory_fs, s3_fs):
    """Test basic write and read operations."""
    for fs in [memory_fs, s3_fs]:
        content = b"test content"
        fs.fs._write("test.txt", content)

        with fs.open("test.txt", "rb") as f:
            assert f.read() == content


def test_write_errors(memory_fs, s3_fs):
    """Test error cases for write operations."""
    for fs in [memory_fs, s3_fs]:
        with pytest.raises(Exception):
            fs.fs._write("", b"test")  # Empty path
