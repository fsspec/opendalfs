"""Core functionality tests for filesystem creation and basic operations."""


def test_filesystem_creation(memory_fs, s3_fs):
    """Test filesystem creation."""
    assert memory_fs is not None
    assert s3_fs is not None


def test_exists(memory_fs, s3_fs):
    """Test path existence checks."""
    for fs in [memory_fs, s3_fs]:
        # File existence
        with fs.open("test.txt", "wb") as f:
            f.write(b"test")
        assert fs.exists("test.txt")
        assert not fs.exists("nonexistent.txt")

        # Directory existence
        fs.mkdir("testdir/")
        assert fs.exists("testdir/")
