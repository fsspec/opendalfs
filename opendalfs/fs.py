from __future__ import annotations

from glob import has_magic
from typing import Any

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import trailing_sep
import logging
from opendal import AsyncOperator, Operator
from .file import OpendalAsyncBufferedFile, OpendalBufferedFile
from opendal.exceptions import NotFound, Unsupported

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

    # Async implementations using Rust's async methods directly
    #
    async def _ls(self, path: str, detail=True, **kwargs):
        """List contents of path"""
        path = self._strip_protocol(path)
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
        logger.debug(f"Getting info for: {path}")
        try:
            info = await self.async_fs.stat(path)
        except NotFound as err:
            raise FileNotFoundError(path) from err
        return {
            "name": path,
            "size": info.content_length,
            "type": self._fsspec_type_from_mode(info.mode),
        }

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory"""
        await self.async_fs.create_dir(path)
        self.invalidate_cache(self._parent(path.rstrip("/")))

    async def _rmdir(self, path: str, recursive: bool = False) -> None:
        """Remove directory"""
        if recursive:
            await self.async_fs.remove_all(path)
        else:
            await self.async_fs.delete(path)
        base = path.rstrip("/")
        self.invalidate_cache(path)
        self.invalidate_cache(self._parent(base))

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file"""
        await self.async_fs.delete(path)
        self.invalidate_cache(self._parent(path))

    async def _cp_file(self, path1: str, path2: str, **kwargs) -> None:
        """Copy file from path1 to path2."""
        try:
            await self.async_fs.copy(path1, path2)
        except Unsupported:
            data = await self.async_fs.read(path1)
            await self.async_fs.write(path2, data)
        self.invalidate_cache(self._parent(path2.rstrip("/")))

    async def _cat_file(self, path: str, start: int | None = None, end: int | None = None, **kwargs):
        """Get file content as bytes (async implementation)."""
        data = await self.async_fs.read(path)
        if start is None and end is None:
            return data

        size = len(data)
        if start is None:
            start = 0
        elif start < 0:
            start = max(0, size + start)

        if end is None:
            end = size
        elif end < 0:
            end = size + end

        return data[start:end]

    async def _pipe_file(self, path: str, value: bytes, mode: str = "overwrite", **kwargs) -> None:
        """Write bytes into file (async implementation)."""
        if mode == "create" and await self._exists(path):
            raise FileExistsError(path)
        await self.async_fs.write(path, value)
        self.invalidate_cache(self._parent(path.rstrip("/")))

    async def _opendal_rename(self, source: str, target: str) -> None:
        await self.async_fs.rename(source, target)

    # Higher-level async operations built on core methods
    async def _exists(self, path: str, **kwargs):
        """Check path existence"""
        return await self.async_fs.exists(path)

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
        try:
            info = await self.async_fs.stat(path)
        except NotFound as err:
            raise FileNotFoundError(path) from err
        else:
            return info.last_modified

    def mv(self, path1, path2, recursive: bool = False, maxdepth: int | None = None, **kwargs):
        if (
            isinstance(path1, str)
            and isinstance(path2, str)
            and not recursive
            and maxdepth is None
            and not has_magic(path1)
        ):
            src = self._strip_protocol(path1)
            dst = self._strip_protocol(path2)
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
        return super().mv(path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs)

    def invalidate_cache(self, path: str | None = None):
        if path is None:
            self.dircache.clear()
            super().invalidate_cache(path)
            return

        stripped = self._strip_protocol(path).rstrip("/")
        self.dircache.pop(stripped, None)
        prefix = stripped + "/"
        for key in list(self.dircache):
            if key.startswith(prefix):
                self.dircache.pop(key, None)
        super().invalidate_cache(stripped)
