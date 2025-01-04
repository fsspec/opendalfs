import importlib
from typing import Any, List, Dict
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.spec import AbstractBufferedFile
import io
import logging
import time

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
        try:
            module = importlib.import_module(f"opendalfs_service_{scheme}")
            fs_class = getattr(module, f"{scheme.capitalize()}FileSystem")
            # Filter out fsspec-specific kwargs
            rust_kwargs = {
                k: v for k, v in kwargs.items() if k not in ("asynchronous", "loop")
            }
            self.fs = fs_class(**rust_kwargs)
        except ImportError:
            raise ImportError(f"Cannot import opendal_service_{scheme}")
        except AttributeError:
            raise AttributeError(f"Cannot find {scheme.capitalize()}FileSystem")

    async def _call_rust(self, future):
        """Helper to properly await Rust futures with consistent error handling"""
        try:
            result = await future
            return result
        except Exception as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "notfound" in error_msg:
                raise FileNotFoundError(str(e))
            if "permission" in error_msg:
                raise PermissionError(str(e))
            if "exists" in error_msg:
                raise FileExistsError(str(e))
            raise OSError(str(e))

    # Async implementations using Rust's async methods directly
    async def _ls(self, path: str) -> List[Dict[str, Any]]:
        """List contents of path"""
        try:
            entries = await self._call_rust(self.fs.ls(path))
            # Filter out the directory itself from the entries
            if isinstance(entries, list):
                entries = [e for e in entries if e != path and e != path.rstrip("/")]
            return entries
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(str(e))
            raise OSError(str(e))

    async def _info(self, path: str) -> Dict[str, Any]:
        """Get path info"""
        logger.debug(f"Getting info for: {path}")
        future = self.fs.info(path)
        return await self._call_rust(future)

    async def _mkdir(self, path: str, create_parents: bool = True) -> None:
        """Create directory"""
        try:
            await self.fs.mkdir(path, create_parents=create_parents)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(str(e))
            raise OSError(str(e))

    async def _rmdir(self, path: str, recursive: bool = False) -> None:
        """Remove directory"""
        try:
            # Check if directory exists and is empty
            entries = await self._ls(path)
            if entries and not recursive:
                raise OSError(f"Directory not empty: {path}")

            # If we get here, either directory is empty or recursive=True
            future = self.fs.rmdir(path, recursive=recursive)
            if future is None:
                raise OSError(f"Rust rmdir returned None for {path}")

            await self._call_rust(future)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(str(e))
            raise OSError(str(e))

    def rmdir(self, path: str, recursive: bool = False) -> None:
        """Sync version of rmdir"""
        return sync(self.loop, self._rmdir, path, recursive=recursive)

    async def _rm_file(self, path: str) -> None:
        """Remove file"""
        try:
            future = self.fs.rm_file(path)
            if future is None:
                raise OSError(f"Rust rm_file returned None for {path}")

            await self._call_rust(future)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(str(e))
            raise OSError(str(e))

    def rm_file(self, path: str) -> None:
        """Sync version of rm_file"""
        return sync(self.loop, self._rm_file, path)

    async def _read(self, path: str) -> bytes:
        """Read file contents"""
        try:
            logger.debug(f"Reading file: {path}")
            future = self.fs._read(path)
            if future is None:
                raise OSError(f"Rust _read returned None for {path}")

            result = await self._call_rust(future)
            if not isinstance(result, bytes):
                result = bytes(result)
            return result
        except Exception as e:
            logger.error(f"Read failed: {e}", exc_info=True)
            raise OSError(f"Failed to read {path}: {e}")

    async def _write(self, path: str, data: bytes) -> None:
        """Write file contents"""
        try:
            logger.debug(f"Writing {len(data)} bytes to {path}")

            if isinstance(data, io.BytesIO):
                data = data.getvalue()
            elif not isinstance(data, bytes):
                data = str(data).encode()

            future = self.fs._write(path, data)
            if future is None:
                raise OSError(f"Rust _write returned None for {path}")

            await self._call_rust(future)
            logger.debug(f"Write completed successfully to {path}")

        except Exception as e:
            logger.error(f"Write failed to {path}: {e}", exc_info=True)
            if "not found" in str(e).lower():
                raise FileNotFoundError(f"Failed to write to {path}: {e}")
            raise OSError(f"Failed to write to {path}: {e}")

    def _write_sync(self, path: str, data: bytes) -> None:
        """Synchronous version of write"""
        try:
            logger.debug(f"_write_sync: Writing {len(data)} bytes to {path}")
            result = sync(self.loop, self._write, path, data)
            logger.debug("_write_sync: Write completed successfully")
            return result
        except Exception as e:
            logger.error(f"_write_sync: Write failed to {path}: {e}", exc_info=True)
            raise OSError(f"Failed to write to {path}: {e}")

    # Higher-level async operations built on core methods
    async def _exists(self, path: str) -> bool:
        """Check path existence"""
        logger.debug(f"Checking existence of: {path}")
        try:
            # Try both with and without trailing slash for directories
            paths_to_check = [path]
            if not path.endswith("/"):
                paths_to_check.append(path + "/")

            for p in paths_to_check:
                future = self.fs.exists(p)
                if await self._call_rust(future):
                    return True

            return False
        except Exception as e:
            logger.debug(f"Existence check failed: {e}")
            return False

    async def _isfile(self, path: str) -> bool:
        """Check if path is file"""
        try:
            info = await self._info(path)
            return info.get("type", None) == "file"
        except FileNotFoundError:
            return False

    async def _isdir(self, path: str) -> bool:
        """Check if path is directory"""
        try:
            info = await self._info(path)
            return info.get("type", None) == "directory"
        except FileNotFoundError:
            return False

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create directory and parents"""
        try:
            await self._mkdir(path, create_parents=True)
        except FileExistsError:
            if not exist_ok:
                raise

    # Sync wrappers for methods not handled by mirror_sync_methods
    makedirs = sync_wrapper(_makedirs)
    exists = sync_wrapper(_exists)
    isfile = sync_wrapper(_isfile)
    isdir = sync_wrapper(_isdir)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = 50 * 2**20,
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict = None,
        **kwargs: Any,
    ) -> "OpendalBufferedFile":
        """Open a file for reading or writing"""
        try:
            if "r" in mode:
                # Only check existence for read modes
                info = self.info(path)
                if info["type"] != "file":
                    raise IsADirectoryError(f"{path} is not a file")
            elif "w" in mode or "a" in mode:
                # For write modes, only check if we're trying to write through a file
                if "/" in path:
                    parts = path.split("/")
                    for i in range(len(parts) - 1):
                        parent = "/".join(parts[: i + 1])
                        if parent and self.exists(parent):
                            info = self.info(parent)
                            if info["type"] == "file":
                                raise NotADirectoryError(
                                    f"Parent path '{parent}' is a file, cannot write '{path}' through it"
                                )

            return OpendalBufferedFile(
                self,
                path,
                mode=mode,
                block_size=block_size,
                autocommit=autocommit,
                cache_type=cache_type,
                cache_options=cache_options,
                **kwargs,
            )

        except FileNotFoundError:
            if "r" in mode:
                raise  # Re-raise for read mode
            # For write mode, we allow missing parents (virtual directories)
            return OpendalBufferedFile(
                self,
                path,
                mode=mode,
                block_size=block_size,
                autocommit=autocommit,
                cache_type=cache_type,
                cache_options=cache_options,
                **kwargs,
            )
        except Exception as e:
            raise OSError(f"Error opening file: {e}")

    def created(self, path: str) -> None:
        """Get creation time (not supported)"""
        raise NotImplementedError("Creation time is not supported by OpenDAL")

    async def _modified(self, path: str):
        """Get modified time (async version)"""
        try:
            # Get the metadata future (don't await it yet)
            metadata_future = self.fs.modified(path)

            # Use _call_rust to properly handle the future
            return await self._call_rust(metadata_future)

        except Exception as e:
            if "Last modified time not available" in str(e):
                raise NotImplementedError("Modified time not supported by this backend")
            raise

    def modified(self, path: str):
        """Get modified time (sync version)"""
        try:
            return sync(self.loop, self._modified, path)
        except Exception as e:
            if "Last modified time not available" in str(e):
                raise NotImplementedError("Modified time not supported by this backend")
            raise

    # Explicit sync wrappers
    def read(self, path: str) -> bytes:
        """Sync version of read"""
        try:
            return sync(self.loop, self._read, path)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(f"File {path} not found")
            raise OSError(f"Failed to read {path}: {e}")

    def write(self, path: str, data: bytes) -> None:
        """Sync version of write"""
        try:
            if isinstance(data, io.BytesIO):
                data = data.getvalue()
            elif not isinstance(data, bytes):
                data = str(data).encode()

            sync(self.loop, self._write, path, data)
        except Exception as e:
            logger.error(f"Write failed: {e}", exc_info=True)
            raise OSError(f"Failed to write to {path}: {e}")

    def info(self, path: str) -> Dict[str, Any]:
        """Sync version of info"""
        return sync(self.loop, self._info, path)


class OpendalBufferedFile(AbstractBufferedFile):
    """Buffered file implementation for OpenDAL"""

    def __init__(
        self,
        fs: OpendalFileSystem,
        path: str,
        mode: str = "rb",
        block_size: int = 50 * 2**20,
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict = None,
        size: int = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

    async def _call_rust(self, future):
        """Helper to properly await Rust futures"""
        try:
            result = await future
            return result
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(str(e))
            raise OSError(str(e))

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Download data between start and end"""
        try:
            logger.debug(f"Fetching range {start}-{end} from {self.path}")
            data = self.fs.read(self.path)
            if not isinstance(data, bytes):
                data = bytes(data)
            return data[start:end]
        except Exception as e:
            logger.debug(f"Fetch range failed: {e}")
            if "not found" in str(e).lower():
                raise FileNotFoundError(f"File {self.path} not found")
            raise OSError(str(e))

    def _upload_chunk(self, final: bool = False) -> bool:
        """Upload partial chunk of data"""
        try:
            data = self.buffer.getvalue()
            if not data:  # Empty buffer
                return True

            if isinstance(data, io.BytesIO):
                data = data.getvalue()
            elif not isinstance(data, bytes):
                data = str(data).encode()

            # For append mode, read existing content first
            if "a" in self.mode:
                try:
                    existing = self.fs.read(self.path)
                    data = existing + data
                except FileNotFoundError:
                    pass  # File doesn't exist yet

            # Clear the buffer first
            self.buffer.seek(0)
            self.buffer.truncate()

            # Write the data
            try:
                self.fs.write(self.path, data)
                logger.debug(f"Successfully wrote {len(data)} bytes to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Write failed in _upload_chunk: {e}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Upload failed in buffer handling: {e}", exc_info=True)
            return False

    def _initiate_upload(self) -> None:
        """Prepare for uploading"""
        pass

    def _commit_upload(self) -> None:
        """Ensure upload is complete"""
        if self._upload_chunk(final=True) is False:
            raise OSError("Upload failed")

    def close(self):
        """Ensure data is written before closing"""
        if self.mode == "wb":
            try:
                if not self._upload_chunk(final=True):
                    logger.error(f"Failed to write file on close: {self.path}")
                    raise OSError(f"Failed to write file {self.path} on close")
            except Exception as e:
                logger.error(f"Error during close of {self.path}: {e}", exc_info=True)
                raise OSError(f"Failed to write file {self.path} on close: {e}")
        super().close()


def test_exists(memory_fs, s3_fs):
    """Test path existence checks."""
    for fs in [memory_fs, s3_fs]:
        # File existence
        with fs.open("test.txt", "wb") as f:
            f.write(b"test")

        # Add a small delay to ensure write is complete
        time.sleep(0.1)

        assert fs.exists("test.txt")
        assert not fs.exists("nonexistent.txt")
