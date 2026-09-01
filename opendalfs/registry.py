from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar
from urllib.parse import urlsplit

from .fs import OpendalFileSystem


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    _service: ClassVar[str]
    _authority_option: ClassVar[str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(self._service, *args, **kwargs)

    @property
    def _authority(self) -> str:
        return self.storage_options.get(self._authority_option, "")

    def _to_operator_path(self, path: str) -> str:
        path = super()._to_operator_path(path)
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
        path = self._add_authority(self._to_operator_path(name))
        return super().unstrip_protocol(path)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        if "://" not in path:
            return {}

        parsed = urlsplit(path)
        if parsed.scheme != cls.protocol or not parsed.netloc:
            return {}
        return {cls._authority_option: parsed.netloc}


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    _service = "s3"
    _authority_option = "bucket"


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    _service = "gcs"
    _authority_option = "bucket"


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    _service = "azblob"
    _authority_option = "container"


_S3FS_OPTION_ALIASES = {
    "key": "access_key_id",
    "secret": "secret_access_key",
    "token": "session_token",
    "anon": "skip_signature",
    "endpoint_url": "endpoint",
    "requester_pays": "enable_request_payer",
}
_S3FS_CLIENT_OPTION_ALIASES = {
    "aws_access_key_id": "access_key_id",
    "aws_secret_access_key": "secret_access_key",
    "aws_session_token": "session_token",
    "endpoint_url": "endpoint",
    "region_name": "region",
}


def _translate_s3fs_options(options: dict[str, Any]) -> dict[str, Any]:
    translated = options.copy()
    for s3fs_name, opendal_name in _S3FS_OPTION_ALIASES.items():
        if (value := translated.pop(s3fs_name, None)) is not None:
            translated.setdefault(opendal_name, value)

    client_options = translated.pop("client_kwargs", {}) or {}
    if not isinstance(client_options, Mapping):
        raise TypeError("S3 option 'client_kwargs' must be a mapping")
    client_options = dict(client_options)
    for s3fs_name, opendal_name in _S3FS_CLIENT_OPTION_ALIASES.items():
        if (value := client_options.pop(s3fs_name, None)) is not None:
            translated.setdefault(opendal_name, value)
    if client_options:
        unsupported = ", ".join(sorted(client_options))
        raise TypeError(f"Unsupported S3 client_kwargs: {unsupported}")

    return translated


class S3FileSystem(OpendalS3FileSystem):
    """Route standard ``s3://`` URLs through OpenDAL."""

    protocol = "s3"

    def __init__(
        self,
        *args: Any,
        default_block_size: int | None = None,
        _bucket_from_url: str | None = None,
        **kwargs: Any,
    ) -> None:
        options = _translate_s3fs_options(kwargs)
        if _bucket_from_url is not None:
            options["bucket"] = _bucket_from_url
        self._bucket = options.get("bucket", "")
        super().__init__(*args, **options)
        if default_block_size is not None:
            self.blocksize = default_block_size

    @property
    def _authority(self) -> str:
        return self._bucket

    def unstrip_protocol(self, name: str) -> str:
        return OpendalFileSystem.unstrip_protocol(self, name)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        options = super()._get_kwargs_from_urls(path)
        if bucket := options.pop("bucket", None):
            options["_bucket_from_url"] = bucket
        return options
