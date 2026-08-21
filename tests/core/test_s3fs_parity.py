def _file_behavior(fs, *, url: str, path: str, name_prefix: str) -> dict:
    fs.pipe_file(url, b"hello from minio")
    info = fs.info(url)

    return {
        "exists": fs.exists(url),
        "isfile": fs.isfile(url),
        "isdir": fs.isdir(url),
        "name": info["name"].removeprefix(name_prefix),
        "size": info["size"],
        "type": info["type"],
        "read_url": fs.cat_file(url),
        "read_path": fs.cat_file(path),
        "read_roundtrip_url": fs.cat_file(fs.unstrip_protocol(path)),
    }


def test_file_url_behavior_matches_s3fs(s3_fs, s3fs_fs):
    expected = _file_behavior(
        s3fs_fs,
        url="s3://test-bucket/parity/file.txt",
        path="test-bucket/parity/file.txt",
        name_prefix="test-bucket/",
    )
    actual = _file_behavior(
        s3_fs,
        url="opendal+s3://test-bucket/parity/file.txt",
        path="parity/file.txt",
        name_prefix="",
    )

    assert actual == expected
