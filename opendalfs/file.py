import inspect
import io
from errno import ESPIPE
from typing import Any

from opendal import AsyncFile as OpendalAsyncFile
from opendal import File as OpendalFile

_WRITE_MODES = {"wb", "ab", "xb"}


def _is_read_mode(mode: str) -> bool:
    return "r" in mode


def _is_write_mode(mode: str) -> bool:
    return mode in _WRITE_MODES


def _compute_seek_target(loc: int, whence: int, current: int, size: int | None) -> int:
    if whence == 0:
        nloc = int(loc)
    elif whence == 1:
        nloc = current + int(loc)
    elif whence == 2:
        if size is None:
            raise ValueError("Cannot seek from end without known size")
        nloc = size + int(loc)
    else:
        raise ValueError(f"invalid whence ({whence}, should be 0, 1 or 2)")
    if nloc < 0:
        raise ValueError("Seek before start of file")
    return nloc


class OpendalFileHandle(io.IOBase):
    """Thin wrapper over OpenDAL File with an explicit fsspec contract.

    Contract surface kept intentionally small and stable:
    - Attributes: fs, path, size
    - Methods: read, write, seek, tell, close, flush, commit, discard
    - Behaviors: tell() must work in write mode; seek follows fsspec semantics
    """

    def __init__(
        self,
        fs,
        path: str,
        mode: str,
        file: OpendalFile,
        size: int | None,
    ) -> None:
        self.fs = fs
        self.path = path
        self.mode = mode
        self._file = file
        self._size = size
        # Track position locally because OpenDAL's write-only files may not
        # support tell(), but fsspec may still call it during writes.
        if mode == "ab" and size is not None:
            self._loc = size
        else:
            try:
                self._loc = file.tell()
            except OSError:
                self._loc = 0
        if self._size is None and _is_write_mode(mode):
            self._size = self._loc
        self._closed = file.closed

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def closed(self) -> bool:  # type: ignore[override]
        return self._closed

    def readable(self) -> bool:
        return _is_read_mode(self.mode) and not self.closed

    def writable(self) -> bool:
        return _is_write_mode(self.mode) and not self.closed

    def seekable(self) -> bool:
        return self.readable()

    def tell(self) -> int:
        try:
            return self._file.tell()
        except OSError:
            return self._loc

    def seek(self, loc: int, whence: int = 0) -> int:
        if not self.readable():
            raise OSError(ESPIPE, "Seek only available in read mode")
        nloc = _compute_seek_target(loc, whence, self._loc, self._size)
        self._file.seek(nloc, 0)
        self._loc = nloc
        return self._loc

    def read(self, size: int | None = -1) -> bytes:
        if not self.readable():
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size is None or size < 0:
            data = self._file.read()
        else:
            data = self._file.read(size)
        self._loc += len(data)
        return data

    def readinto(self, b) -> int:
        if not self.readable():
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        n_raw = self._file.readinto(b)
        n = int(n_raw if n_raw is not None else 0)
        self._loc += n
        return n

    def readline(self, size: int | None = -1) -> bytes:
        if not self.readable():
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size is None or size < 0:
            line = self._file.readline()
        else:
            line = self._file.readline(size)
        self._loc += len(line)
        return line

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            data = memoryview(data).tobytes()
        if not data:
            return 0
        written = self._file.write(data)
        if written is None:
            written = len(data)
        self._loc += written
        if self._size is None:
            self._size = self._loc
        else:
            self._size = max(self._size, self._loc)
        return written

    def flush(self) -> None:
        if self.closed:
            raise ValueError("Flush on closed file")
        self._file.flush()

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._file.close()
        finally:
            self._closed = True
            if _is_write_mode(self.mode):
                self.fs.invalidate_cache(self.path)
                self.fs.invalidate_cache(self.fs._parent(self.path))

    def commit(self) -> None:
        self.close()

    def discard(self) -> None:
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class OpendalAsyncFileHandle:
    """Async wrapper over OpenDAL AsyncFile with the same minimal contract."""

    def __init__(
        self,
        fs,
        path: str,
        mode: str,
        file: OpendalAsyncFile,
        size: int | None,
        loc: int = 0,
    ) -> None:
        self.fs = fs
        self.path = path
        self.mode = mode
        self._file = file
        self._size = size
        self._loc = loc
        self._closed = False
        self._pending_seek: Any | None = None

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return _is_read_mode(self.mode) and not self.closed

    def writable(self) -> bool:
        return _is_write_mode(self.mode) and not self.closed

    def seekable(self) -> bool:
        return self.readable()

    def tell(self) -> int:
        return self._loc

    def seek(self, loc: int, whence: int = 0) -> int:
        if not self.readable():
            raise OSError(ESPIPE, "Seek only available in read mode")
        nloc = _compute_seek_target(loc, whence, self._loc, self._size)
        self._loc = nloc
        self._pending_seek = self._file.seek(nloc, 0)
        return self._loc

    async def _maybe_await(self, value):
        if inspect.isawaitable(value):
            return await value
        return value

    async def _drain_pending_seek(self) -> None:
        if self._pending_seek is None:
            return
        await self._maybe_await(self._pending_seek)
        self._pending_seek = None

    async def read(self, size: int | None = -1) -> bytes:
        if not self.readable():
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size is None or size < 0:
            await self._drain_pending_seek()
            data = await self._maybe_await(self._file.read())
        else:
            await self._drain_pending_seek()
            data = await self._maybe_await(self._file.read(size))
        self._loc += len(data)
        return data

    async def write(self, data: bytes | bytearray | memoryview) -> int:
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            data = memoryview(data).tobytes()
        if not data:
            return 0
        await self._drain_pending_seek()
        written = await self._maybe_await(self._file.write(data))
        if written is None:
            written = len(data)
        self._loc += written
        if self._size is None:
            self._size = 0
        self._size = max(self._size, self._loc)
        return written

    async def flush(self) -> None:
        if self.closed:
            raise ValueError("Flush on closed file")
        if hasattr(self._file, "flush"):
            await self._maybe_await(self._file.flush())

    async def close(self) -> None:
        if self.closed:
            return
        try:
            await self._drain_pending_seek()
            await self._maybe_await(self._file.close())
        finally:
            self._closed = True
            if _is_write_mode(self.mode):
                self.fs.invalidate_cache(self.path)
                self.fs.invalidate_cache(self.fs._parent(self.path))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
