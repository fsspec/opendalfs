"""S3-specific functionality tests."""

import boto3


def test_s3_virtual_directories(s3_fs):
    """Test S3's virtual directory behavior.

    This test verifies that:
    1. We can write to nested paths without explicitly creating directories
    2. The file exists both in S3 and through our interface
    3. Virtual directories work as expected
    """
    path = "a/b/c/test.txt"
    content = b"test"

    # Write file using our interface
    with s3_fs.open(path, "wb") as f:
        f.write(content)

    # Verify with boto3
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        verify=False,
    )

    # Check if file exists in S3
    response = s3.list_objects_v2(Bucket="test-bucket")
    assert "Contents" in response
    assert any(obj["Key"] == path for obj in response["Contents"])

    # Verify through our interface
    assert s3_fs.exists(path)

    # Verify virtual directories
    assert s3_fs.exists("a/b/c/")
    assert "a/b/c/test.txt" in s3_fs.ls("a/b/c/")


def test_s3_bucket_operations(s3_fs):
    """Test S3 bucket-level operations.

    This test verifies that:
    1. We can write to bucket root
    2. Root listing works correctly
    3. Files are properly accessible
    """
    # Write file to bucket root
    content = b"test"
    s3_fs.fs._write("root.txt", content)

    # Verify file exists
    assert "root.txt" in s3_fs.ls("/")

    # Verify content
    with s3_fs.open("root.txt", "rb") as f:
        assert f.read() == content


def test_s3_special_characters(s3_fs):
    """Test S3 paths with special characters.

    This test verifies that:
    1. We can handle paths with spaces and special characters
    2. Path normalization works correctly
    """
    paths = [
        "file with spaces.txt",
        "path/with/special/chars/!@#$.txt",
        "unicode/path/🚀.txt",
    ]
    content = b"test"

    for path in paths:
        # Write and verify
        s3_fs.fs._write(path, content)
        assert s3_fs.exists(path)
        with s3_fs.open(path, "rb") as f:
            assert f.read() == content
