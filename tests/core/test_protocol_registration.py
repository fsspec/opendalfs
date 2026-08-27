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
        == "bucket/dir/file.txt"
    )
    assert OpendalS3FileSystem._get_kwargs_from_urls(
        "opendal+s3://bucket/dir/file.txt"
    ) == {"bucket": "bucket"}
    assert OpendalS3FileSystem._get_kwargs_from_urls(
        "opendal+s3://bucket/dir/file.txt?bucket=other&region=elsewhere"
    ) == {"bucket": "bucket"}

    assert (
        OpendalAzBlobFileSystem._strip_protocol(
            "opendal+azblob://container/dir/file.txt"
        )
        == "container/dir/file.txt"
    )
    assert OpendalAzBlobFileSystem._get_kwargs_from_urls(
        "opendal+azblob://container/dir/file.txt"
    ) == {"container": "container"}


def test_dynamic_service_registration_uses_opendal_authority_option():
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("oss")
    assert protocol == "opendal+oss"

    cls = get_filesystem_class(protocol)
    assert cls.protocol == protocol
    assert cls._strip_protocol("opendal+oss://bucket/dir/file.txt") == (
        "bucket/dir/file.txt"
    )
    assert cls._get_kwargs_from_urls("opendal+oss://bucket/dir/file.txt") == {
        "bucket": "bucket"
    }


def test_dynamic_service_registration_does_not_guess_authority_option():
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("webdav")
    cls = get_filesystem_class(protocol)

    assert cls._strip_protocol("opendal+webdav://host/dir/file.txt") == (
        "/host/dir/file.txt"
    )
    assert cls._get_kwargs_from_urls("opendal+webdav://host/dir/file.txt") == {}


def test_dynamic_service_paths_without_authority_match_fsspec_memory():
    from fsspec.implementations.memory import MemoryFileSystem
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("memory")
    cls = get_filesystem_class(protocol)
    assert cls._strip_protocol(["opendal+memory:///one", "opendal+memory://two"]) == [
        "/one",
        "/two",
    ]

    opendal_fs = cls(skip_instance_cache=True)
    memory_fs = MemoryFileSystem(skip_instance_cache=True)
    root = "integration/path-contract"

    def path_behavior(fs):
        path = fs._strip_protocol(fs.unstrip_protocol(root))
        file_path = f"{path}/one.txt"
        nested_path = f"{path}/nested/two.txt"
        fs.pipe_file(file_path, b"one")
        fs.pipe_file(nested_path, b"two")
        behavior = {
            "path": path,
            "name": fs.info(file_path)["name"],
            "find": fs.find(path),
            "walk": list(fs.walk(path)),
        }
        fs.rm_file(file_path)
        behavior["find_after_rm"] = fs.find(path)
        return behavior

    assert path_behavior(opendal_fs) == path_behavior(memory_fs)
