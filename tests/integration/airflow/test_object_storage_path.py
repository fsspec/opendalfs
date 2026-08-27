"""Airflow ObjectStoragePath coverage adapted from Task SDK 1.3.1."""

import pytest

airflow_sdk = pytest.importorskip("airflow.sdk")
airflow_store = pytest.importorskip("airflow.sdk.io.store")


def test_attached_object_storage_path_read_and_copy(opendal_fs, opendal_root):
    protocol = opendal_fs.protocol
    airflow_store.attach(protocol, fs=opendal_fs)
    source = airflow_sdk.ObjectStoragePath(
        f"{opendal_root}/airflow/source.txt",
        protocol=protocol,
    )
    target = source.parent / "copy.txt"

    with source.open("wb") as stream:
        stream.write(b"hello from Airflow")
    source.copy(target)

    with target.open("rb") as stream:
        assert stream.read() == b"hello from Airflow"
