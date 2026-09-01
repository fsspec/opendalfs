"""Prefect RemoteFileSystem coverage adapted from Prefect 3.8.4."""

import pytest

prefect_filesystems = pytest.importorskip("prefect.filesystems")


@pytest.mark.asyncio
async def test_remote_filesystem_roundtrip_through_opendal_url(s3_fs, opendal_s3_url):
    basepath = f"{opendal_s3_url}/prefect/storage"
    filesystem = prefect_filesystems.RemoteFileSystem(
        basepath=basepath,
        settings=s3_fs.storage_options,
    )

    path = await filesystem.write_path("nested/result.txt", b"hello from Prefect")

    assert path == f"{basepath}/nested/result.txt"
    assert await filesystem.read_path("nested/result.txt") == b"hello from Prefect"
