from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import parse_qsl, urlsplit

from .fs import OpendalFileSystem


_DEFAULT_CONTAINER_KEY_BY_SERVICE: dict[str, str] = {
    "azblob": "container",
}

_DYNAMIC_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {}


def _parse_opendal_url(url: str) -> tuple[str | None, str | None, str, dict[str, str]]:
    if "://" not in url:
        return None, None, url.lstrip("/"), {}

    parsed = urlsplit(url)
    scheme = parsed.scheme or None
    host = parsed.hostname or parsed.netloc or None
    path = (parsed.path or "").lstrip("/")
    query = {k: v for k, v in parse_qsl(parsed.query, keep_blank_values=True)}
    return scheme, host, path, query


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    service: ClassVar[str]
    container_key: ClassVar[str] = "bucket"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop("scheme", None)
        super().__init__(type(self).service, *args, **kwargs)

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, (list, tuple)):
            return type(path)(cls._strip_protocol(p) for p in path)
        if not isinstance(path, str):
            return path

        scheme, _host, stripped, _query = _parse_opendal_url(path)
        if scheme is None:
            return stripped
        if scheme != cls.protocol:
            return path
        return stripped

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        scheme, host, _stripped, query = _parse_opendal_url(path)
        if scheme is not None and scheme != cls.protocol:
            return {}

        kwargs: dict[str, Any] = dict(query)
        if host:
            kwargs.setdefault(cls.container_key, host)
        return kwargs


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    service = "s3"
    container_key = "bucket"


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    service = "gcs"
    container_key = "bucket"


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    service = "azblob"
    container_key = "container"


def register_opendal_service(service: str, *, container_key: str | None = None) -> str:
    from fsspec.registry import register_implementation

    protocol = f"opendal+{service}"
    if protocol not in _DYNAMIC_FILESYSTEMS:
        key = container_key or _DEFAULT_CONTAINER_KEY_BY_SERVICE.get(service, "bucket")
        safe = "".join([c if c.isalnum() else "_" for c in service])
        name = f"Opendal_{safe}_FileSystem"
        cls = type(
            name,
            (_OpendalServiceFileSystem,),
            {
                "protocol": protocol,
                "service": service,
                "container_key": key,
            },
        )
        _DYNAMIC_FILESYSTEMS[protocol] = cls

    register_implementation(protocol, _DYNAMIC_FILESYSTEMS[protocol])
    return protocol


def register_opendal_protocols(services: list[str] | None = None) -> list[str]:
    from fsspec.registry import register_implementation

    builtins: dict[str, type[OpendalFileSystem]] = {
        "opendal+s3": OpendalS3FileSystem,
        "opendal+gcs": OpendalGCSFileSystem,
        "opendal+azblob": OpendalAzBlobFileSystem,
    }

    if services is None:
        for protocol, cls in builtins.items():
            register_implementation(protocol, cls)
        return sorted(builtins.keys())

    registered: list[str] = []
    for service in services:
        protocol = f"opendal+{service}"
        if protocol in builtins:
            register_implementation(protocol, builtins[protocol])
            registered.append(protocol)
        else:
            registered.append(register_opendal_service(service))

    return sorted(set(registered))
