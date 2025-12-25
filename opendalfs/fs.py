from typing import Any
from fsspec.asyn import AsyncFileSystem
import logging
from opendal import AsyncOperator, Operator
from .file import OpendalAsyncBufferedFile, OpendalBufferedFile

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
        return out

    async def _info(self, path: str, **kwargs):
        """Get path info"""
        logger.debug(f"Getting info for: {path}")
        info = await self.async_fs.stat(path)
        return {
            "name": path,
            "size": info.content_length,
            "type": self._fsspec_type_from_mode(info.mode),
        }

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create directory"""
        await self.async_fs.create_dir(path)

    async def _rmdir(self, path: str, recursive: bool = False) -> None:
        """Remove directory"""
        if recursive:
            await self.async_fs.remove_all(path)
        else:
            await self.async_fs.delete(path)

    async def _rm_file(self, path: str, **kwargs) -> None:
        """Remove file"""
        await self.async_fs.delete(path)

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
            info = await self.async_fs.stat(path)
            size = info.content_length

        file = OpendalAsyncBufferedFile(self, path, mode, size=size, **kwargs)

        if mode == "ab":
            try:
                info = await self.async_fs.stat(path)
                file.loc = info.content_length
            except FileNotFoundError:
                file.loc = 0

        return file

    async def _modified(self, path: str):
        """Get modified time (async version)"""
        info = await self.async_fs.stat(path)
        return info.last_modified
