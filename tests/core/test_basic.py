"""Core functionality tests for filesystem creation and basic operations."""

import logging

logger = logging.getLogger(__name__)


def test_write_read(s3_fs):
    """Test basic write and read operations."""
    for fs in [s3_fs]:
        content = b"test content"
        fs.pipe_file("test.txt", content)
        assert fs.cat_file("test.txt") == content
