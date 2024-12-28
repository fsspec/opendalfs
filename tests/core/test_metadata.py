"""Core functionality tests for metadata operations."""

import pytest
from datetime import datetime


def test_info(memory_fs, s3_fs):
    """Test file info retrieval."""
    for fs in [memory_fs, s3_fs]:
        content = b"test content"
        fs.fs._write("test.txt", content)

        info = fs.info("test.txt")
        assert info["type"] == "file"
        assert info["size"] == len(content)


def test_timestamps(s3_fs, memory_fs):
    """Test timestamp operations."""
    # Note: memory_fs does not support last_modified().
    for fs in [s3_fs, memory_fs]:
        fs.fs._write("test.txt", b"test")
        try:
            modified = fs.modified("test.txt")
            assert isinstance(modified, datetime)
        except NotImplementedError:
            pytest.skip(
                f"Modified time not supported by {fs.__class__.__name__} on service: {fs.fs.__class__.__name__}"
            )
