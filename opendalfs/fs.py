from __future__ import annotations

from collections.abc import Mapping
from glob import has_magic
from typing import Any

import logging
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import trailing_sep
from opendal import AsyncOperator, Operator
from opendal.exceptions import NotFound, Unsupported

from .file import OpendalAsyncFileHandle, OpendalFileHandle
from .options import WriteOptions, ensure_write_options, pop_write_options

logger = logging.getLogger("opendalfs")


class OpendalFileSystem(AsyncFileSystem):
    """OpenDAL implementation of fsspec AsyncFileSystem.

    This implementation provides both synchronous and asynchronous access to
    various storage backends supported by OpenDAL.
    """

    async_impl = True
    retries = 5  # Like s3fs

    def __init__(
        self,
        scheme: str,
        *args: Any,
        asynchronous: bool = False,
        loop=None,
        write_options: WriteOptions | None = None,
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
        **kwargs : dict
            Passed to backend implementation
        """
        if "write_options" in kwargs:
            if write_options is not None:
                raise TypeError("write_options specified multiple times")
            write_options = kwargs.pop("write_options")
        write_options = ensure_write_options(write_options)
        self._write_options = write_options

        super().__init__(asynchronous=asynchronous, loop=loop, *args, **kwargs)
        self.scheme = scheme
        self.async_fs = AsyncOperator(scheme, *args, **kwargs)
        self.operator: Operator = self.async_fs.to_operator()

    @staticmethod
    def _fsspec_type_from_mode(mode: Any) -> str:
        if hasattr(mode, "is_dir") and mode.is_dir():
            return "directory"
        if hasattr(mode, "is_file") and mode.is_file():
            return "file"
        return "other"

    def _clean_path(self, path: str) -> str:
        stripped = self._strip_protocol(path)
        if (
            isinstance(stripped, str)
            and path.endswith("/")
            and stripped
            and not stripped.endswith("/")
        ):
            return stripped + "/"
        return stripped

    def _as_dir_path(self, path: str) -> str:
        cleaned = self._clean_path(path)
        if cleaned and not cleaned.endswith("/"):
            return cleaned + "/"
        return cleaned

    def _stat_sync(self, path: str):
        try:
            return self.operator.stat(path)
        except NotFound as err:
            raise FileNotFoundError(path) from err

    async def _stat_async(self, path: str):
        try:
            return await self.async_fs.stat(path)
        except NotFound as err:
            raise FileNotFoundError(path) from err

    def _stat_size_sync(self, path: str) -> int:
        return int(self._stat_sync(path).content_length)

    async def _stat_size_async(self, path: str) -> int:
        info = await self._stat_async(path)
        return int(info.content_length)

    def _ensure_not_exists_sync(self, path: str) -> None:
        if self.operator.exists(path):
            raise FileExistsError(path)

    async def _ensure_not_exists_async(self, path: str) -> None:
        if await self.async_fs.exists(path):
            raise FileExistsError(path)

    # Async implementations using Rust's async methods directly
    #
    async def _ls(self, path: str, detail=True, **kwargs):
        """List contents of path"""
        path = self._clean_path(path)
        cache_path = path.rstrip("/")
        refresh = bool(kwargs.pop("refresh", False))

        if detail and not refresh:
            try:
                cached = self._ls_from_cache(cache_path)
            except FileNotFoundError:
                cached = None
            if cached is not None:
                return cached

        list_path = path
        if path and not path.endswith("/"):
            list_path = path + "/"

        lister = await self.async_fs.list(list_path)

        paths: list[str] = []
        async for entry in lister:
            paths.append(entry.path)

        if not detail:
            return paths

        out: list[dict[str, Any]] = []
        for p in paths:
            out.append(await self._info(p))
        self.dircache[cache_path] = out
        return out

    async def _info(self, path: str, **kwargs):
        """Get path info"""
        path = self._clean_path(path)
        logger.debug("Getting info for: %s", path)
        info = await self._stat_async(path)
        return {
            "name": path,
            "size": info.content_length,
            "type": self._fsspec_type_from_mode(info.mode),
        }

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory"""
        path = self._as_dir_path(path)
        base = path.rstrip("/")
        if not create_parents:
            parent = self._parent(base)
            if parent:
                parent_dir = parent if parent.endswith("/") else parent + "/"
                parent_exists = await self.async_fs.exists(parent) or await self.async_fs.exists(
                    parent_dir
                )
                if not parent_exists:
                    raise FileNotFoundError(parent)
        await self.async_fs.create_dir(path)
        self.invalidate_cache(self._parent(base))

    async def _rmdir(self, path: str, recursive: bool = False) -> None:
        """Remove directory"""
        path = self._as_dir_path(path)
        if recursive:
            await self.async_fs.remove_all(path)
        else:
            await self.async_fs.delete(path)
        base = path.rstrip("/")
        self.invalidate_cache(path)
        self.invalidate_cache(self._parent(base))

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file"""
        path = self._clean_path(path)
        await self.async_fs.delete(path)
        self.invalidate_cache(self._parent(path))

    async def _cp_file(self, path1: str, path2: str, **kwargs) -> None:
        """Copy file from path1 to path2."""
        path1 = self._clean_path(path1)
        path2 = self._clean_path(path2)
        try:
            await self.async_fs.copy(path1, path2)
        except Unsupported:
            data = await self.async_fs.read(path1)
            await self.async_fs.write(path2, data)
        self.invalidate_cache(self._parent(path2.rstrip("/")))

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ):
        """Get file content as bytes (async implementation)."""
        path = self._clean_path(path)
        if start is None and end is None:
            return await self.async_fs.read(path)

        size = None
        if (start is not None and start < 0) or (end is not None and end < 0):
            size = await self._stat_size_async(path)

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
                return await self.async_fs.read(path)
            return await self.async_fs.read(path, offset=start)

        length = end - start
        if length <= 0:
            return b""
        return await self.async_fs.read(path, offset=start, size=length)

    async def _pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs
    ) -> None:
        """Write bytes into file (async implementation)."""
        path = self._clean_path(path)
        if mode == "create" and await self._exists(path):
            raise FileExistsError(path)
        write_opts = pop_write_options(kwargs, defaults=self._write_options)
        file = await self.async_fs.open(path, "wb", **write_opts)
        try:
            await file.write(value)
        finally:
            await file.close()
        self.invalidate_cache(self._parent(path.rstrip("/")))

    def pipe_file(
        self, path: str, value: bytes, mode: str = "overwrite", **kwargs
    ) -> None:
        """Write bytes into file (sync implementation)."""
        path = self._clean_path(path)
        if mode == "create" and self.exists(path):
            raise FileExistsError(path)
        write_opts = pop_write_options(kwargs, defaults=self._write_options)
        file = self.operator.open(path, "wb", **write_opts)
        try:
            file.write(value)
        finally:
            file.close()
        self.invalidate_cache(self._parent(path.rstrip("/")))

    async def _opendal_rename(self, source: str, target: str) -> None:
        await self.async_fs.rename(source, target)

    # Higher-level async operations built on core methods
    async def _exists(self, path: str, **kwargs):
        """Check path existence"""
        path = self._clean_path(path)
        return await self.async_fs.exists(path)

    @staticmethod
    def _apply_block_size_to_chunk(
        block_size: Any, write_opts: dict[str, Any]
    ) -> dict[str, Any]:
        if block_size in (None, "default") or "chunk" in write_opts:
            return write_opts
        try:
            block_size_int = int(block_size)
        except (TypeError, ValueError):
            return write_opts
        if block_size_int > 0:
            write_opts["chunk"] = block_size_int
        return write_opts

    def _collect_write_opts(
        self, mode: str, block_size: Any, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        if mode not in {"wb", "ab", "xb"}:
            return {}
        write_opts = pop_write_options(kwargs, defaults=self._write_options)
        return self._apply_block_size_to_chunk(block_size, write_opts)

    def _open_append_sync(
        self, path: str, mode: str, write_opts: dict[str, Any]
    ) -> OpendalFileHandle:
        path = self._clean_path(path)
        cap = self.operator.capability()
        if getattr(cap, "write_can_append", False):
            try:
                size = self.operator.stat(path).content_length
            except NotFound:
                size = 0
            file = self.operator.open(path, "ab", **write_opts)
            return OpendalFileHandle(self, path, mode, file, size)

        try:
            existing = self.operator.read(path)
        except NotFound:
            existing = b""
        file = self.operator.open(path, "wb", **write_opts)
        if existing:
            file.write(existing)
        size = len(existing)
        return OpendalFileHandle(self, path, mode, file, size)

    async def _open_append_async(
        self, path: str, mode: str, write_opts: dict[str, Any]
    ) -> OpendalAsyncFileHandle:
        path = self._clean_path(path)
        cap = self.async_fs.capability()
        if getattr(cap, "write_can_append", False):
            try:
                info = await self.async_fs.stat(path)
                size = info.content_length
                loc = size
            except NotFound:
                size = 0
                loc = 0
            file = await self.async_fs.open(path, "ab", **write_opts)
            return OpendalAsyncFileHandle(self, path, mode, file, size, loc=loc)

        try:
            existing = await self.async_fs.read(path)
        except NotFound:
            existing = b""
        file = await self.async_fs.open(path, "wb", **write_opts)
        if existing:
            await file.write(existing)
        size = len(existing)
        loc = size
        return OpendalAsyncFileHandle(self, path, mode, file, size, loc=loc)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs: Any,
    ) -> OpendalFileHandle:
        """Open a file for reading or writing via native OpenDAL file."""
        _ = autocommit, cache_options
        path = self._clean_path(path)
        write_opts = self._collect_write_opts(mode, block_size, kwargs)

        if mode == "xb":
            self._ensure_not_exists_sync(path)
            mode = "wb"

        size = None
        if mode == "rb":
            size = self._stat_size_sync(path)

        if mode == "ab":
            return self._open_append_sync(path, mode, write_opts)

        file = self.operator.open(path, mode, **write_opts)
        return OpendalFileHandle(self, path, mode, file, size)

    async def open_async(self, path, mode="rb", **kwargs):
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError

        path = self._clean_path(path)
        block_size = kwargs.pop("block_size", None)
        write_opts = self._collect_write_opts(mode, block_size, kwargs)

        if mode == "xb":
            await self._ensure_not_exists_async(path)
            mode = "wb"

        size = None
        loc = 0
        if mode == "rb":
            size = await self._stat_size_async(path)

        if mode == "ab":
            return await self._open_append_async(path, mode, write_opts)

        file = await self.async_fs.open(path, mode, **write_opts)
        return OpendalAsyncFileHandle(self, path, mode, file, size, loc=loc)

    async def _modified(self, path: str):
        """Get modified time (async version)"""
        path = self._clean_path(path)
        info = await self._stat_async(path)
        return info.last_modified

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
            src = self._clean_path(path1)
            dst = self._clean_path(path2)
            if trailing_sep(dst) or self.isdir(dst):
                base = src.rstrip("/").split("/")[-1]
                dst = dst.rstrip("/") + "/" + base
            try:
                self.operator.rename(src, dst)
                self.invalidate_cache(self._parent(src.rstrip("/")))
                self.invalidate_cache(self._parent(dst.rstrip("/")))
                return None
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

        stripped = self._clean_path(path).rstrip("/")
        self.dircache.pop(stripped, None)
        prefix = stripped + "/"
        for key in list(self.dircache):
            if key.startswith(prefix):
                self.dircache.pop(key, None)
        super().invalidate_cache(stripped)
