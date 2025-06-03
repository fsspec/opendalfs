"""Core functionality tests for filesystem creation and basic operations."""

import logging

logger = logging.getLogger(__name__)


def test_write_read(s3_fs):
    """Test basic write and read operations."""
    for fs in [s3_fs]:
        content = b"test content"
        fs.write("test.txt", content)
        assert fs.read("test.txt") == content
