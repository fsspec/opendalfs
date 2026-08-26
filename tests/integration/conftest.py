import fsspec
import fsspec.config
import pytest

from opendalfs import register_opendal_service


@pytest.fixture(params=("memory", "fs", "s3"))
def opendal_backend(request):
    return request.param


@pytest.fixture
def opendal_root(tmp_path):
    return f"integration/{tmp_path.name}"


@pytest.fixture
def opendal_protocol(opendal_backend):
    if opendal_backend == "s3":
        return "opendal+s3"
    return register_opendal_service(opendal_backend)


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
    opendal_protocol,
    opendal_root,
    opendal_storage_options,
    monkeypatch,
):
    if opendal_backend == "s3":
        fs = request.getfixturevalue("s3_fs")
    else:
        fs = fsspec.filesystem(opendal_protocol, **opendal_storage_options)

    monkeypatch.setitem(
        fsspec.config.conf,
        opendal_protocol,
        opendal_storage_options,
    )
    yield fs

    if fs.exists(opendal_root):
        fs.rm(opendal_root, recursive=True)


@pytest.fixture
def opendal_url(
    opendal_backend,
    opendal_protocol,
    opendal_root,
    opendal_fs,
    s3_config,
):
    if opendal_backend == "s3":
        return f"{opendal_protocol}://{s3_config.bucket}/{opendal_root}"
    return f"{opendal_protocol}://{opendal_root}"
