from __future__ import annotations

from glob import has_magic
import os
from typing import Any

from fsspec.asyn import AsyncFileSystem, sync_wrapper
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.implementations.local import trailing_sep
from fsspec.utils import stringify_path
import logging
from opendal import AsyncOperator, Operator
from .file import OpendalAsyncBufferedFile, OpendalBufferedFile
from opendal.exceptions import NotFound, Unsupported
from opendal.layers import RetryLayer

logger = logging.getLogger("opendalfs")


class OpendalFileSystem(AsyncFileSystem):
    """OpenDAL implementation of fsspec AsyncFileSystem.

    This implementation provides both synchronous and asynchronous access to
    various storage backends supported by OpenDAL.
    """

    async_impl = True

    def __init__(
        self,
        scheme: str,
        *args: Any,
        asynchronous: bool = False,
        loop=None,
        batch_size: int | None = None,
        use_listings_cache: bool = True,
        listings_expiry_time: float | None = None,
        max_paths: int | None = None,
        retries: int = 5,
        **kwargs: Any,
    ) -> None:
        """Initialize OpendalFileSystem.

        Parameters
        ----------
        scheme : str
            The storage backend scheme (e.g., 's3', 'memory')
        asynchronous : bool
            Whether to return async versions of methods (default: False)
        loop : event loop (optional)
            Specific event loop to use
        batch_size : int, optional
            Maximum number of concurrent fsspec batch operations
        use_listings_cache : bool
            Whether fsspec should cache directory listings
        listings_expiry_time : float, optional
            Number of seconds before cached listings expire
        max_paths : int, optional
            Maximum number of cached directory listings
        retries : int
            Number of retries for temporary OpenDAL failures
        **kwargs : dict
            Passed only to the OpenDAL backend implementation
        """
        super().__init__(
            *args,
            asynchronous=asynchronous,
            loop=loop,
            batch_size=batch_size,
            use_listings_cache=use_listings_cache,
            listings_expiry_time=listings_expiry_time,
            max_paths=max_paths,
        )
        self.scheme = scheme
        self.retries = retries
        self.async_fs = AsyncOperator(scheme, *args, **kwargs)
        if retries > 0:
            self.async_fs = self.async_fs.layer(RetryLayer(max_times=retries))
        self.operator: Operator = self.async_fs.to_operator()

    @staticmethod
    def _fsspec_type_from_mode(mode: Any) -> str:
        if hasattr(mode, "is_dir") and mode.is_dir():
            return "directory"
        if hasattr(mode, "is_file") and mode.is_file():
            return "file"
        return "other"

    @staticmethod
    def _directory_path(path: str) -> str:
        return path if not path or path.endswith("/") else path + "/"

    def _normalize_path(self, path: str) -> str:
        path = stringify_path(path)
        normalized = self._strip_protocol(path).lstrip("/")
        if path.endswith("/"):
            return self._directory_path(normalized)
        return normalized

    # Async implementations using Rust's async methods directly
    #
    async def _ls(self, path: str, detail=True, **kwargs):
        """List contents of path"""
        path = self._normalize_path(path)
        cache_path = path.rstrip("/")
        refresh = bool(kwargs.pop("refresh", False))

        if not refresh:
            try:
                cached = self._ls_from_cache(cache_path)
            except FileNotFoundError:
                cached = None
            if cached is not None:
                return cached if detail else [info["name"] for info in cached]

        lister = await self.async_fs.list(self._directory_path(path))

        out: list[dict[str, Any]] = []
        async for entry in lister:
            out.append(self._info_from_metadata(entry.path, entry.metadata))

        self.dircache[cache_path] = out
        return out if detail else [info["name"] for info in out]

    def _info_from_metadata(self, path: str, metadata: Any) -> dict[str, Any]:
        entry_type = self._fsspec_type_from_mode(metadata.mode)
        return {
            "name": path.rstrip("/") if entry_type == "directory" else path,
            "size": metadata.content_length,
            "type": entry_type,
        }

    async def _info(self, path: str, **kwargs):
        """Get path info"""
        path = self._normalize_path(path)
        logger.debug(f"Getting info for: {path}")

        canonical_path = path.rstrip("/")
        refresh = bool(kwargs.pop("refresh", False))
        if not refresh:
            cached = self._ls_from_cache(canonical_path)
            if cached is not None:
                exact = [info for info in cached if info["name"] == canonical_path]
                if exact and (
                    not path.endswith("/") or exact[0]["type"] == "directory"
                ):
                    return exact[0]
                if not exact:
                    return {"name": canonical_path, "size": 0, "type": "directory"}

        try:
            info = await self.async_fs.stat(path)
        except NotFound:
            if path and not path.endswith("/"):
                try:
                    info = await self.async_fs.stat(self._directory_path(path))
                except NotFound:
                    info = None
            else:
                info = None

        if info is not None:
            return self._info_from_metadata(path, info)

        directory_path = self._directory_path(canonical_path)
        try:
            lister = await self.async_fs.list(directory_path, limit=1)
            async for _ in lister:
                return {
                    "name": canonical_path,
                    "size": 0,
                    "type": "directory",
                }
        except NotFound:
            pass

        raise FileNotFoundError(path)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory"""
        path = self._directory_path(self._normalize_path(path))
        await self.async_fs.create_dir(path)
        self.invalidate_cache(self._parent(path.rstrip("/")))

    async def _rmdir(self, path: str, recursive: bool = False) -> None:
        """Remove directory"""
        path = self._directory_path(self._normalize_path(path))
        if recursive:
            await self.async_fs.remove_all(path)
        else:
            await self.async_fs.delete(path)
        base = path.rstrip("/")
        self.invalidate_cache(path)
        self.invalidate_cache(self._parent(base))

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file"""
        path = self._normalize_path(path)
        await self.async_fs.delete(path)
        self.invalidate_cache(self._parent(path))

    async def _cp_file(self, path1: str, path2: str, **kwargs) -> None:
        """Copy file from path1 to path2."""
        path1 = self._normalize_path(path1)
        path2 = self._normalize_path(path2)
        try:
            try:
                await self.async_fs.copy(path1, path2)
            except Unsupported:
                data = await self.async_fs.read(path1)
                await self.async_fs.write(path2, data)
        except NotFound as err:
            raise FileNotFoundError(path1) from err
        self.invalidate_cache(self._parent(path2.rstrip("/")))

    async def _read(self, path: str, **kwargs):
        try:
            return await self.async_fs.read(path, **kwargs)
        except NotFound as err:
            raise FileNotFoundError(path) from err

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ):
        """Get file content as bytes (async implementation)."""
        path = self._normalize_path(path)
        if start is None and end is None:
            return await self._read(path)

        size = None
        if (start is not None and start < 0) or (end is not None and end < 0):
            try:
                info = await self.async_fs.stat(path)
            except NotFound as err:
                raise FileNotFoundError(path) from err
            size = info.content_length

        if start is None:
            start = 0
        elif start < 0:
            start = max(0, size + start)

        if end is None:
            if size is not None:
                end = size
        elif end < 0:
            end = size + end

        if end is None:
            if start == 0:
                return await self._read(path)
            return await self._read(path, offset=start)

        length = end - start
        if length <= 0:
            return b""
        return await self._read(path, offset=start, size=length)

    async def _get_file(
        self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs
    ) -> None:
        """Download a remote file to a local path."""
        rpath = self._normalize_path(rpath)
        lpath = os.fspath(lpath)
        if os.path.isdir(lpath):
            return

        info = await self._info(rpath)
        callback.set_size(info["size"])
        reader = await self.async_fs.open(rpath, "rb")
        try:
            with open(lpath, "wb") as target:
                while chunk := await reader.read(2**20):
                    callback.relative_update(target.write(chunk))
        finally:
            await reader.close()

    async def _put_file(
        self,
        lpath,
        rpath,
        callback=DEFAULT_CALLBACK,
        mode="overwrite",
        **kwargs,
    ) -> None:
        """Upload a local file to a remote path."""
        lpath = os.fspath(lpath)
        if os.path.isdir(lpath):
            return

        rpath = self._normalize_path(rpath)
        if mode == "create" and await self._exists(rpath):
            raise FileExistsError(rpath)

        callback.set_size(os.path.getsize(lpath))
        writer = await self.async_fs.open(rpath, "wb")
        try:
            with open(lpath, "rb") as source:
                while chunk := source.read(2**20):
                    await writer.write(chunk)
                    callback.relative_update(len(chunk))
        finally:
            await writer.close()
        self.invalidate_cache(self._parent(rpath))

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs
    ) -> None:
        """Write bytes into file (async implementation)."""
        path = self._normalize_path(path)
        if mode == "create" and await self._exists(path):
            raise FileExistsError(path)
        await self.async_fs.write(path, value)
        self.invalidate_cache(self._parent(path.rstrip("/")))

    async def _opendal_rename(self, source: str, target: str) -> None:
        source = self._normalize_path(source)
        target = self._normalize_path(target)
        try:
            await self.async_fs.rename(source, target)
        except NotFound as err:
            raise FileNotFoundError(source) from err

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs: Any,
    ) -> OpendalBufferedFile:
        """Open a file for reading or writing"""
        path = self._normalize_path(path)
        return OpendalBufferedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    async def open_async(self, path, mode="rb", **kwargs):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError

        path = self._normalize_path(path)

        size = None
        if mode == "rb":
            try:
                info = await self.async_fs.stat(path)
            except NotFound as err:
                raise FileNotFoundError(path) from err
            else:
                size = info.content_length

        file = OpendalAsyncBufferedFile(self, path, mode, size=size, **kwargs)

        if mode == "ab":
            try:
                info = await self.async_fs.stat(path)
                file.loc = info.content_length
            except NotFound:
                file.loc = 0

        return file

    async def _modified(self, path: str):
        """Get modified time (async version)"""
        path = self._normalize_path(path)
        try:
            info = await self.async_fs.stat(path)
        except NotFound as err:
            raise FileNotFoundError(path) from err
        else:
            return info.last_modified

    modified = sync_wrapper(_modified)

    def mv(
        self,
        path1,
        path2,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs,
    ):
        if (
            isinstance(path1, str)
            and isinstance(path2, str)
            and not recursive
            and maxdepth is None
            and not has_magic(path1)
        ):
            src = self._normalize_path(path1)
            dst = self._normalize_path(path2)
            if trailing_sep(dst) or self.isdir(dst):
                base = src.rstrip("/").split("/")[-1]
                dst = dst.rstrip("/") + "/" + base
            try:
                self.operator.rename(src, dst)
                self.invalidate_cache(self._parent(src.rstrip("/")))
                self.invalidate_cache(self._parent(dst.rstrip("/")))
                return None
            except NotFound as err:
                raise FileNotFoundError(src) from err
            except Unsupported:
                pass
        return super().mv(
            path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs
        )

    def invalidate_cache(self, path: str | None = None):
        if path is None:
            self.dircache.clear()
            super().invalidate_cache(path)
            return

        stripped = self._normalize_path(path).rstrip("/")
        self.dircache.pop(stripped, None)
        prefix = stripped + "/"
        for key in list(self.dircache):
            if key.startswith(prefix):
                self.dircache.pop(key, None)
        super().invalidate_cache(stripped)
