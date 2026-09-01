"""universal_pathlib compatibility cases migrated for issue #51.

Source: universal-pathlib 0.3.10,
``docs/usage.md`` (the public ``protocol=`` constructor) and
``upath/tests/cases.py`` (``make_test_files``, ``test_iterdir``,
``test_write_text`` and ``test_write_bytes``).
"""

import fsspec.config
from upath import UPath

from opendalfs import S3FileSystem


def test_upath_protocol_read_write_and_listing(
    s3_config,
    tmp_path,
    monkeypatch,
):
    fsspec.register_implementation("s3", S3FileSystem, clobber=True)
    monkeypatch.setitem(
        fsspec.config.conf,
        "s3",
        {
            "endpoint_url": s3_config.endpoint,
            "client_kwargs": {"region_name": s3_config.region},
            "key": s3_config.access_key_id,
            "secret": s3_config.secret_access_key,
        },
    )
    root = UPath(f"s3://{s3_config.bucket}/integration/{tmp_path.name}")

    folder = root / "folder1"
    folder.mkdir(parents=True)
    text_path = folder / "file1.txt"
    bytes_path = folder / "file2.txt"
    text_path.write_text("hello world")
    bytes_path.write_bytes(b"hello bytes")

    assert text_path.read_text() == "hello world"
    assert bytes_path.read_bytes() == b"hello bytes"

    children = set(folder.iterdir())
    assert children == {text_path, bytes_path}
    assert all(path.exists() for path in children)
