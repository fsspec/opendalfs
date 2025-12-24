from opendalfs.registry import (
    OpendalAzBlobFileSystem,
    OpendalGCSFileSystem,
    OpendalS3FileSystem,
    register_opendal_protocols,
    register_opendal_service,
)


def test_register_default_protocols():
    from fsspec.registry import get_filesystem_class

    registered = register_opendal_protocols()
    assert registered == ["opendal+azblob", "opendal+gcs", "opendal+s3"]

    assert get_filesystem_class("opendal+s3") is OpendalS3FileSystem
    assert get_filesystem_class("opendal+gcs") is OpendalGCSFileSystem
    assert get_filesystem_class("opendal+azblob") is OpendalAzBlobFileSystem


def test_strip_protocol_and_kwargs():
    assert (
        OpendalS3FileSystem._strip_protocol("opendal+s3://bucket/dir/file.txt")
        == "dir/file.txt"
    )
    assert OpendalS3FileSystem._get_kwargs_from_urls("opendal+s3://bucket/dir/file.txt")[
        "bucket"
    ] == "bucket"

    assert (
        OpendalAzBlobFileSystem._strip_protocol("opendal+azblob://container/dir/file.txt")
        == "dir/file.txt"
    )
    assert OpendalAzBlobFileSystem._get_kwargs_from_urls(
        "opendal+azblob://container/dir/file.txt"
    )["container"] == "container"


def test_dynamic_service_registration():
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("oss")
    assert protocol == "opendal+oss"

    cls = get_filesystem_class("opendal+oss")
    assert cls.protocol == "opendal+oss"
    assert cls.service == "oss"
