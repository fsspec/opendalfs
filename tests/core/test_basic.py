"""Core functionality tests for filesystem creation and basic operations."""

import pytest
import logging

logger = logging.getLogger(__name__)


def test_filesystem_creation(memory_fs, s3_fs):
    """Test filesystem creation."""
    assert memory_fs is not None
    assert s3_fs is not None


def test_file_operations(memory_fs, s3_fs):
    """Test basic file operations"""
    for fs in [memory_fs, s3_fs]:
        # Write and read
        with fs.open("test.txt", "wb") as f:
            f.write(b"test data")

        with fs.open("test.txt", "rb") as f:
            assert f.read() == b"test data"

        # Check exists and info
        assert fs.exists("test.txt")
        info = fs.info("test.txt")
        assert info["type"] == "file"
        assert info["size"] == 9

        # Test non-existent file
        assert not fs.exists("nonexistent.txt")
        with pytest.raises(FileNotFoundError):
            fs.info("nonexistent.txt")


def test_directory_operations(memory_fs, s3_fs):
    """Test directory operations"""
    for fs in [memory_fs, s3_fs]:
        # Create directory
        fs.mkdir("testdir/")
        assert fs.exists("testdir/")
        assert fs.isdir("testdir/")

        # Create nested directories
        fs.makedirs("nested/dirs/here/", exist_ok=True)
        assert fs.exists("nested/dirs/here/")

        # Write file in directory
        with fs.open("testdir/file.txt", "wb") as f:
            f.write(b"file in dir")
        assert fs.exists("testdir/file.txt")

        # Test rmdir on non-empty directory
        with pytest.raises(OSError, match="Directory not empty"):
            fs.rmdir("testdir/")

        # Clean up file and try again
        fs.rm_file("testdir/file.txt")
        fs.rmdir("testdir/")  # Should succeed now
        assert not fs.exists("testdir/")


def test_file_modes(memory_fs, s3_fs):
    """Test different file open modes"""
    for fs in [memory_fs, s3_fs]:
        # Write text mode
        with fs.open("text.txt", "w") as f:
            f.write("text data")

        # Read text mode
        with fs.open("text.txt", "r") as f:
            assert f.read() == "text data"

        # Append mode
        with fs.open("text.txt", "a") as f:
            f.write(" appended")

        with fs.open("text.txt", "r") as f:
            assert f.read() == "text data appended"


def test_large_file_operations(memory_fs, s3_fs):
    """Test operations with larger files"""
    data = b"large data " * 1024  # 10KB
    for fs in [memory_fs, s3_fs]:
        # Write large file
        with fs.open("large.bin", "wb") as f:
            f.write(data)

        # Read in chunks
        with fs.open("large.bin", "rb") as f:
            chunks = []
            while True:
                chunk = f.read(1024)
                if not chunk:
                    break
                chunks.append(chunk)
            assert b"".join(chunks) == data


def test_error_handling(memory_fs, s3_fs):
    """Test error conditions"""
    for fs in [memory_fs, s3_fs]:
        # Try to read non-existent file
        with pytest.raises(FileNotFoundError):
            with fs.open("nonexistent.txt", "rb") as f:
                f.read()

        # Try to write to a path where parent exists as a file
        logger.info("Creating parent file...")
        with fs.open("parent.txt", "wb") as f:
            f.write(b"I am a file")

        logger.info("Verifying parent is a file...")
        info = fs.info("parent.txt")
        assert info["type"] == "file"

        logger.info("Attempting to write to child path...")
        with pytest.raises(OSError, match="Parent path 'parent.txt' is a file"):
            with fs.open("parent.txt/file.txt", "wb") as f:
                f.write(b"data")


def test_simple_write(memory_fs):
    """Test very basic write operation"""
    logger.info("Starting simple write test")
    try:
        with memory_fs.open("simple.txt", "wb") as f:
            logger.info("Writing test data")
            f.write(b"test")
            logger.info("Write operation completed")
    except Exception as e:
        logger.error(f"Write failed: {e}", exc_info=True)
        raise

    logger.info("Reading back data")
    with memory_fs.open("simple.txt", "rb") as f:
        data = f.read()
        logger.info(f"Read back: {data}")
        assert data == b"test"
