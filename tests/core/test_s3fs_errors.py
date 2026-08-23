def _missing_file_behavior(fs, *, url: str) -> dict[str, str]:
    operations = {
        "info": lambda: fs.info(url),
        "cat": lambda: fs.cat_file(url),
        "open": lambda: fs.open(url, "rb"),
        "copy": lambda: fs.cp_file(url, f"{url}-copy"),
    }
    failures = {}
    for name, operation in operations.items():
        try:
            operation()
        except Exception as error:
            failures[name] = type(error).__name__
    return failures


def test_missing_file_behavior_matches_s3fs(s3_fs, s3fs_fs, s3_config):
    bucket = s3_config.bucket
    expected = _missing_file_behavior(s3fs_fs, url=f"s3://{bucket}/missing-file")
    actual = _missing_file_behavior(s3_fs, url=f"opendal+s3://{bucket}/missing-file")

    assert actual == expected
