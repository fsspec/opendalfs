from datetime import datetime


def _transfer_behavior(fs, *, url: str, tmp_path) -> dict:
    source = tmp_path / f"{fs.protocol}-source.txt"
    downloaded = tmp_path / f"{fs.protocol}-downloaded.txt"
    source.write_bytes(b"transferred through fsspec")

    fs.put_file(source, url)
    modified = fs.modified(url)
    fs.get_file(url, downloaded)

    return {
        "content": downloaded.read_bytes(),
        "modified_is_datetime": isinstance(modified, datetime),
        "modified_has_timezone": modified.tzinfo is not None,
    }


def test_file_transfer_and_modified_behavior_matches_s3fs(
    s3_fs, s3fs_fs, s3_config, tmp_path
):
    bucket = s3_config.bucket
    expected = _transfer_behavior(
        s3fs_fs,
        url=f"s3://{bucket}/transfer/reference.txt",
        tmp_path=tmp_path,
    )
    actual = _transfer_behavior(
        s3_fs,
        url=f"opendal+s3://{bucket}/transfer/actual.txt",
        tmp_path=tmp_path,
    )

    assert actual == expected
