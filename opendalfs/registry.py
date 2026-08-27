from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import urlsplit

from .fs import OpendalFileSystem

# OpenDAL services whose native URI maps its authority directly to one option.
_AUTHORITY_OPTION_BY_SERVICE = {
    "aliyun-drive": "drive_type",
    "azblob": "container",
    "b2": "bucket",
    "cos": "bucket",
    "gcs": "bucket",
    "obs": "bucket",
    "oss": "bucket",
    "s3": "bucket",
    "tos": "bucket",
    "upyun": "bucket",
}


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    _authority_option: ClassVar[str | None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop("scheme", None)
        service = type(self).protocol.removeprefix("opendal+")
        super().__init__(service, *args, **kwargs)

    @property
    def _authority(self) -> str:
        if self._authority_option is None:
            return ""
        return self.storage_options.get(self._authority_option, "")

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return super()._strip_protocol(path)

        path = super()._strip_protocol(path)
        if cls._authority_option is None and path:
            return f"/{path.lstrip('/')}"
        return path

    def _normalize_path(self, path: str) -> str:
        path = super()._normalize_path(path)
        # Service adapters expose authority/path, while the OpenDAL operator
        # is already scoped by the corresponding service option.
        authority = self._authority
        if path == authority:
            return ""
        if authority and path.startswith(f"{authority}/"):
            return path[len(authority) + 1 :]
        return path

    def _add_authority(self, path: str) -> str:
        authority = self._authority
        return f"{authority}/{path}" if path else authority

    async def _ls(self, path: str, detail=True, **kwargs):
        entries = await super()._ls(path, detail=detail, **kwargs)
        if not detail:
            return [self._add_authority(path) for path in entries]
        return [
            {**entry, "name": self._add_authority(entry["name"])} for entry in entries
        ]

    async def _info(self, path: str, **kwargs):
        info = await super()._info(path, **kwargs)
        return {**info, "name": self._add_authority(info["name"])}

    def unstrip_protocol(self, name: str) -> str:
        path = self._add_authority(self._normalize_path(name))
        return super().unstrip_protocol(path)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        if "://" not in path:
            return {}

        parsed = urlsplit(path)
        if parsed.scheme != cls.protocol:
            return {}

        if not parsed.netloc or cls._authority_option is None:
            return {}
        return {cls._authority_option: parsed.netloc}


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["s3"]


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["gcs"]


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["azblob"]


_BUILTIN_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {
    "s3": OpendalS3FileSystem,
    "gcs": OpendalGCSFileSystem,
    "azblob": OpendalAzBlobFileSystem,
}
_DYNAMIC_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {}


def register_opendal_service(service: str) -> str:
    from fsspec.registry import register_implementation

    protocol = f"opendal+{service}"
    cls = _BUILTIN_FILESYSTEMS.get(service)
    if cls is None:
        cls = _DYNAMIC_FILESYSTEMS.get(service)
    if cls is None:
        safe = "".join([c if c.isalnum() else "_" for c in service])
        name = f"Opendal_{safe}_FileSystem"
        cls = type(
            name,
            (_OpendalServiceFileSystem,),
            {
                "protocol": protocol,
                "_authority_option": _AUTHORITY_OPTION_BY_SERVICE.get(service),
            },
        )
        _DYNAMIC_FILESYSTEMS[service] = cls

    register_implementation(protocol, cls)
    return protocol


def register_opendal_protocols(services: list[str] | None = None) -> list[str]:
    if services is None:
        services = list(_BUILTIN_FILESYSTEMS)

    return sorted({register_opendal_service(service) for service in services})
