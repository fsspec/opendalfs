from typing import Any
from fsspec.asyn import AsyncFileSystem, sync
import logging
from opendal import Operator, AsyncOperator
from .file import OpendalBufferedFile
from .decorator import generate_blocking_methods

logger = logging.getLogger("opendalfs")

@generate_blocking_methods
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
        self.fs = Operator(scheme, *args, **kwargs)
        self.async_fs = AsyncOperator(scheme, *args, **kwargs)

    # Async implementations using Rust's async methods directly
    #
    # TODO: support detail
    async def _ls(self, path: str, detail=True, **kwargs):
        """List contents of path"""
        return await self.async_fs.list(path)

    async def _info(self, path: str, **kwargs):
        """Get path info"""
        logger.debug(f"Getting info for: {path}")
        info = await self.async_fs.stat(path)
        return {
            "size": info.content_length,
            "path": path,
            "type": info.mode,
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

    async def _read(self, path: str):
        """Read file contents"""
        return await self.async_fs.read(path)

    async def _write(self, path: str, data: bytes):
        """Write file contents"""
        await self.async_fs.write(path, data)

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

    async def _modified(self, path: str):
        """Get modified time (async version)"""
        info = await self.async_fs.stat(path)
        return info.last_modified
