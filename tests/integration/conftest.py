import fsspec
import fsspec.config
import pytest

from opendalfs import OpendalFileSystem


@pytest.fixture(params=("memory", "fs", "s3"))
def opendal_backend(request):
    return request.param


@pytest.fixture
def opendal_storage_options(opendal_backend, tmp_path, s3_config):
    if opendal_backend == "memory":
        return {}
    if opendal_backend == "fs":
        storage_root = tmp_path / "storage"
        storage_root.mkdir()
        return {"root": str(storage_root)}
    return {
        "endpoint": s3_config.endpoint,
        "region": s3_config.region,
        "access_key_id": s3_config.access_key_id,
        "secret_access_key": s3_config.secret_access_key,
    }


@pytest.fixture
def opendal_fs(
    request,
    opendal_backend,
    opendal_storage_options,
):
    if opendal_backend == "s3":
        return request.getfixturevalue("s3_fs")
    return OpendalFileSystem(opendal_backend, **opendal_storage_options)


@pytest.fixture
def opendal_root(tmp_path, opendal_fs, opendal_backend, s3_config):
    base_path = f"integration/{tmp_path.name}"
    root = f"{s3_config.bucket}/{base_path}" if opendal_backend == "s3" else base_path
    yield root

    if opendal_fs.exists(root):
        opendal_fs.rm(root, recursive=True)


@pytest.fixture
def opendal_s3_storage_options(s3_fs):
    return s3_fs.storage_options


@pytest.fixture
def opendal_s3_root(tmp_path, s3_config):
    return f"{s3_config.bucket}/integration/{tmp_path.name}"


@pytest.fixture
def opendal_s3_url(
    s3_fs,
    opendal_s3_root,
    opendal_s3_storage_options,
    monkeypatch,
):
    monkeypatch.setitem(
        fsspec.config.conf,
        "opendal+s3",
        opendal_s3_storage_options,
    )
    return s3_fs.unstrip_protocol(opendal_s3_root)
