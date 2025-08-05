"""Core functionality tests for filesystem creation and basic operations."""

import pytest
from tests.conftest import FILE_SYSTEMS
from opendalfs import OpendalFileSystem


@pytest.mark.parametrize("fs", FILE_SYSTEMS)
def test_write_read(fs: OpendalFileSystem) -> None:
    """Test basic write and read operations."""
    with fs.open("foo.bar", "wb") as f:
        f.write("baz")

    with fs.open("foo.bar", "rb") as f:
        assert f.read() == "baz"
