"""Airflow ObjectStoragePath coverage adapted from Task SDK 1.3.1."""

import pytest

airflow_sdk = pytest.importorskip("airflow.sdk")
airflow_store = pytest.importorskip("airflow.sdk.io.store")


def test_attached_object_storage_path_read_write(opendal_fs, opendal_root):
    protocol = opendal_fs.protocol
    conn_id = "opendal"
    airflow_store.attach(protocol, conn_id=conn_id, fs=opendal_fs)
    path = airflow_sdk.ObjectStoragePath(
        f"{opendal_root}/airflow/file.txt",
        protocol=protocol,
        conn_id=conn_id,
    )

    with path.open("wb") as stream:
        stream.write(b"foo")

    with path.open("rb") as stream:
        assert stream.read() == b"foo"

    path.unlink()
