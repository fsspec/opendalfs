import fsspec

from opendalfs.registry import (
    OpendalAzBlobFileSystem,
    OpendalGCSFileSystem,
    OpendalS3FileSystem,
    S3FileSystem,
    _OpendalServiceFileSystem,
)


class _ScopedMemoryFileSystem(_OpendalServiceFileSystem):
    """Memory backend with a bucket-shaped external namespace for path tests."""

    protocol = "opendal+memory"
    _authority_option = "bucket"

    def __init__(self, *args, **kwargs):
        kwargs.pop("bucket")
        super().__init__(*args, **kwargs)


def test_installed_protocols_resolve():
    assert fsspec.get_filesystem_class("opendal+s3") is OpendalS3FileSystem
    assert fsspec.get_filesystem_class("opendal+gcs") is OpendalGCSFileSystem
    assert fsspec.get_filesystem_class("opendal+azblob") is OpendalAzBlobFileSystem


def test_s3_adapter_can_be_registered():
    fsspec.register_implementation("s3", S3FileSystem, clobber=True)

    assert fsspec.get_filesystem_class("s3") is S3FileSystem


def test_backend_key_can_start_with_the_authority(tmp_path):
    fs = _ScopedMemoryFileSystem(bucket="bucket", skip_instance_cache=True)
    directory = "bucket/bucket"
    source = f"{directory}/source.txt"
    copied = f"{directory}/copied.txt"
    moved = f"{directory}/moved.txt"

    fs.pipe_file(source, b"content")
    with fs.open(source, "ab") as source_file:
        assert source_file.path == source
        source_file.write(b" appended")

    download = tmp_path / "source.txt"
    fs.get_file(source, download)
    fs.cp_file(source, copied)
    fs.mv(copied, moved)

    assert download.read_bytes() == b"content appended"
    assert fs.cat_file(moved) == b"content appended"
    assert fs.info(moved)["name"] == moved
    assert set(fs.ls(directory, detail=False)) == {source, moved}
