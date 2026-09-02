import pytest

from opendalfs import S3FileSystem


def _file_behavior(fs, *, url: str, path: str, name_prefix: str) -> dict:
    fs.pipe_file(url, b"hello from minio")
    info = fs.info(url)

    return {
        "exists": fs.exists(url),
        "isfile": fs.isfile(url),
        "isdir": fs.isdir(url),
        "name": info["name"].removeprefix(name_prefix),
        "list": fs.ls(path.rsplit("/", 1)[0], detail=False),
        "size": info["size"],
        "type": info["type"],
        "read_url": fs.cat_file(url),
        "read_path": fs.cat_file(path),
        "read_roundtrip_url": fs.cat_file(fs.unstrip_protocol(path)),
    }


def test_file_url_behavior_matches_s3fs(s3_fs, s3fs_fs, s3_config):
    bucket = s3_config.bucket
    expected = _file_behavior(
        s3fs_fs,
        url=f"s3://{bucket}/parity/file.txt",
        path=f"{bucket}/parity/file.txt",
        name_prefix=f"{bucket}/",
    )
    actual = _file_behavior(
        s3_fs,
        url=f"opendal+s3://{bucket}/parity/file.txt",
        path=f"{bucket}/parity/file.txt",
        name_prefix=f"{bucket}/",
    )

    assert actual == expected


def test_standard_s3_url_and_options_match_s3fs(standard_s3_fs, s3fs_fs, s3_config):
    bucket = s3_config.bucket
    expected = _file_behavior(
        s3fs_fs,
        url=f"s3://{bucket}/standard-parity/file.txt",
        path=f"{bucket}/standard-parity/file.txt",
        name_prefix=f"{bucket}/",
    )
    actual = _file_behavior(
        standard_s3_fs,
        url=f"s3://{bucket}/standard-parity/file.txt",
        path=f"{bucket}/standard-parity/file.txt",
        name_prefix=f"{bucket}/",
    )

    assert actual == expected
    assert standard_s3_fs.unstrip_protocol("") == s3fs_fs.unstrip_protocol("")


def test_standard_s3_rejects_a_path_from_another_bucket(
    standard_s3_fs, s3fs_fs, s3_config
):
    outside_path = f"{s3_config.bucket}-other/wrong.txt"

    with pytest.raises(ValueError, match="does not belong to bucket"):
        standard_s3_fs.pipe_file(outside_path, b"must not be misrouted")

    assert not s3fs_fs.exists(f"{s3_config.bucket}/{outside_path}")


def test_standard_s3_rejects_non_mapping_client_options():
    with pytest.raises(TypeError, match=r"client_kwargs.*mapping"):
        S3FileSystem(client_kwargs=[], skip_instance_cache=True)
