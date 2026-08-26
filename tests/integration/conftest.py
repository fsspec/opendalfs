from dataclasses import dataclass
from typing import Any

import fsspec
import fsspec.config
import pytest

from opendalfs import OpendalFileSystem, register_opendal_service


@dataclass(frozen=True)
class OpendalStorage:
    backend: str
    fs: OpendalFileSystem
    base_path: str
    base_url: str
    storage_options: dict[str, Any]

    def path(self, name: str) -> str:
        return f"{self.base_path}/{name.lstrip('/')}"

    def url(self, name: str) -> str:
        return f"{self.base_url}/{name.lstrip('/')}"


@pytest.fixture(params=("memory", "fs", "s3"))
def opendal_storage(request, tmp_path, s3_config, monkeypatch):
    """Provide each integration with memory, filesystem, and MinIO storage."""
    backend = request.param
    base_path = f"integration/{tmp_path.name}"

    if backend == "s3":
        fs = request.getfixturevalue("s3_fs")
        protocol = "opendal+s3"
        storage_options = {
            "endpoint": s3_config.endpoint,
            "region": s3_config.region,
            "access_key_id": s3_config.access_key_id,
            "secret_access_key": s3_config.secret_access_key,
        }
        base_url = f"{protocol}://{s3_config.bucket}/{base_path}"
    else:
        protocol = register_opendal_service(backend)
        storage_options = {}
        if backend == "fs":
            storage_root = tmp_path / "storage"
            storage_root.mkdir()
            storage_options["root"] = str(storage_root)
        fs = fsspec.filesystem(protocol, **storage_options)
        base_url = f"{protocol}:///{base_path}"

    monkeypatch.setitem(fsspec.config.conf, protocol, storage_options)
    storage = OpendalStorage(
        backend=backend,
        fs=fs,
        base_path=base_path,
        base_url=base_url,
        storage_options=storage_options,
    )
    yield storage

    if fs.exists(base_path):
        fs.rm(base_path, recursive=True)
