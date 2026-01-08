import logging

from fsspec.asyn import AbstractAsyncStreamedFile
from fsspec.spec import AbstractBufferedFile
from opendal import AsyncFile as OpendalAsyncFile
from opendal import File as OpendalFile
from opendal.exceptions import NotFound
from .options import pop_write_options

logger = logging.getLogger("opendalfs")


class OpendalBufferedFile(AbstractBufferedFile):
    """Buffered file implementation for OpenDAL"""

    _opendal_writer: OpendalFile | None
    _append_via_write: bool
    _initiated: bool

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        write_mode = kwargs.pop("opendal_write_mode", None)
        write_options = pop_write_options(
            kwargs, defaults=getattr(fs, "_write_options", None)
        )

        if write_mode is not None and write_mode != "buffered":
            raise ValueError("opendal_write_mode must be 'buffered'")

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

        self._opendal_writer = None
        self._append_via_write = False
        self._initiated = False
        self._write_options = write_options

        if mode == "ab":
            # Match python semantics: append writes start from end-of-file.
            try:
                self.loc = self.details["size"]
            except FileNotFoundError:
                self.loc = 0

    def _fetch_range(self, start: int, end: int):
        """Download data between start and end"""
        if start >= end:
            return b""

        length = end - start
        return self.fs.operator.read(self.path, offset=start, size=length)

    def _should_bypass_buffer(self, data) -> bool:
        if self.mode not in {"wb", "xb"}:
            return False
        if self._append_via_write:
            return False
        blocksize = getattr(self, "blocksize", None)
        if not isinstance(blocksize, int) or blocksize <= 0:
            return False
        try:
            size = len(data)
        except TypeError:
            return False
        if size < blocksize:
            return False
        return self.buffer.tell() == 0

    def write(self, data):
        if not self._should_bypass_buffer(data):
            return super().write(data)
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")

        if not data:
            return 0

        if self.offset is None:
            self.offset = 0
            try:
                self._initiate_upload()
            except Exception:
                self.closed = True
                raise

        if self._opendal_writer is None:
            self._opendal_writer = self.fs.operator.open(
                self.path, "wb", **self._write_options
            )

        if not isinstance(data, (bytes, bytearray)):
            data = memoryview(data).tobytes()

        self._opendal_writer.write(data)
        size = len(data)
        self.loc += size
        self.offset += size
        return size

    def _upload_chunk(self, final: bool = False):
        """Upload partial chunk of data"""
        if not self._initiated:
            raise RuntimeError("Upload has not been initiated")

        self.buffer.seek(0)
        chunk = self.buffer.read()

        if not chunk:
            if not final:
                return False
            if self.mode == "ab" and self._append_via_write:
                if not self.fs.operator.exists(self.path):
                    self.fs.operator.write(self.path, b"")
                return None
            self._commit_upload()
            return None

        if self.mode == "ab" and self._append_via_write:
            # Let OpenDAL handle append semantics if the backend supports it.
            self.fs.operator.write(self.path, chunk, append=True, **self._write_options)
            return None

        if self._opendal_writer is None:
            self._opendal_writer = self.fs.operator.open(
                self.path, "wb", **self._write_options
            )

        if chunk:
            self._opendal_writer.write(chunk)

        if final:
            self._commit_upload()
        return None

    def _initiate_upload(self) -> None:
        """Prepare for uploading"""
        if self._initiated:
            return

        if self.mode == "xb" and self.fs.operator.exists(self.path):
            raise FileExistsError(self.path)

        if self.mode == "ab":
            cap = self.fs.operator.capability()
            if getattr(cap, "write_can_append", False):
                self._append_via_write = True
                # Align offset with existing size for correct accounting.
                self.offset = self.loc
            else:
                # Fallback: emulate append by rewriting the full object.
                try:
                    existing = self.fs.operator.read(self.path)
                except (FileNotFoundError, NotFound):
                    existing = b""
                if existing:
                    self._opendal_writer = self.fs.operator.open(
                        self.path, "wb", **self._write_options
                    )
                    self._opendal_writer.write(existing)
                    self.offset = len(existing)

        self._initiated = True

    def _commit_upload(self) -> None:
        """Ensure upload is complete"""
        if self.mode == "ab" and self._append_via_write:
            return

        if self._opendal_writer is None:
            # Ensure empty files are created on close.
            self.fs.operator.write(self.path, b"", **self._write_options)
            return

        self._opendal_writer.flush()
        self._opendal_writer.close()
        self._opendal_writer = None

    def close(self):
        """Ensure data is written before closing"""
        if self.closed:
            return

        try:
            super().close()
        finally:
            if self._opendal_writer is not None:
                try:
                    self._opendal_writer.close()
                finally:
                    self._opendal_writer = None


class OpendalAsyncBufferedFile(AbstractAsyncStreamedFile):
    """Async buffered file implementation for OpenDAL."""

    _opendal_writer: OpendalAsyncFile | None
    _append_via_write: bool
    _initiated: bool
    _exclusive_create: bool

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        self._exclusive_create = mode == "xb"
        normalized_mode = "wb" if self._exclusive_create else mode

        write_mode = kwargs.pop("opendal_write_mode", None)
        write_options = pop_write_options(
            kwargs, defaults=getattr(fs, "_write_options", None)
        )
        if write_mode is not None and write_mode != "buffered":
            raise ValueError("opendal_write_mode must be 'buffered'")

        super().__init__(
            fs,
            path,
            mode=normalized_mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

        self._opendal_writer = None
        self._append_via_write = False
        self._initiated = False
        self._write_options = write_options

    async def _fetch_range(self, start: int, end: int):
        if start >= end:
            return b""

        length = end - start
        return await self.fs.async_fs.read(self.path, offset=start, size=length)

    async def _upload_chunk(self, final: bool = False):
        if not self._initiated:
            raise RuntimeError("Upload has not been initiated")

        self.buffer.seek(0)
        chunk = self.buffer.read()

        if not chunk:
            if not final:
                return False
            if self.mode == "ab" and self._append_via_write:
                if not await self.fs.async_fs.exists(self.path):
                    await self.fs.async_fs.write(self.path, b"")
                return None
            await self._commit_upload()
            return None

        if self.mode == "ab" and self._append_via_write:
            await self.fs.async_fs.write(
                self.path, chunk, append=True, **self._write_options
            )
            return None

        if self._opendal_writer is None:
            self._opendal_writer = await self.fs.async_fs.open(
                self.path, "wb", **self._write_options
            )

        await self._opendal_writer.write(chunk)

        if final:
            await self._commit_upload()
        return None

    async def _initiate_upload(self) -> None:
        if self._initiated:
            return

        if self._exclusive_create and await self.fs.async_fs.exists(self.path):
            raise FileExistsError(self.path)

        if self.mode == "ab":
            cap = self.fs.async_fs.capability()
            if getattr(cap, "write_can_append", False):
                self._append_via_write = True
                self.offset = self.loc
            else:
                try:
                    existing = await self.fs.async_fs.read(self.path)
                except (FileNotFoundError, NotFound):
                    existing = b""
                if existing:
                    self._opendal_writer = await self.fs.async_fs.open(
                        self.path, "wb", **self._write_options
                    )
                    await self._opendal_writer.write(existing)
                    self.offset = len(existing)

        self._initiated = True

    async def _commit_upload(self) -> None:
        if self.mode == "ab" and self._append_via_write:
            return

        if self._opendal_writer is None:
            await self.fs.async_fs.write(self.path, b"", **self._write_options)
            return

        try:
            await self._opendal_writer.close()
        finally:
            self._opendal_writer = None

    async def close(self):
        if self.closed:
            return

        try:
            await super().close()
        finally:
            if self._opendal_writer is not None:
                try:
                    await self._opendal_writer.close()
                finally:
                    self._opendal_writer = None
