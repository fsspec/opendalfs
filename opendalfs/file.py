import logging

from fsspec.asyn import AbstractAsyncStreamedFile
from fsspec.spec import AbstractBufferedFile
from opendal import AsyncFile as OpendalAsyncFile
from opendal import File as OpendalFile

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

        # Follow s3fs style: make an independent range read request (no shared cursor).
        reader = self.fs.operator.open(self.path, "rb")
        try:
            reader.seek(start)
            return reader.read(end - start)
        finally:
            reader.close()

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
            self.fs.operator.write(self.path, chunk, append=True)
            return None

        if self._opendal_writer is None:
            self._opendal_writer = self.fs.operator.open(self.path, "wb")

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
                except FileNotFoundError:
                    existing = b""
                if existing:
                    self._opendal_writer = self.fs.operator.open(self.path, "wb")
                    self._opendal_writer.write(existing)
                    self.offset = len(existing)

        self._initiated = True

    def _commit_upload(self) -> None:
        """Ensure upload is complete"""
        if self.mode == "ab" and self._append_via_write:
            return

        if self._opendal_writer is None:
            # Ensure empty files are created on close.
            self.fs.operator.write(self.path, b"")
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

    async def _fetch_range(self, start: int, end: int):
        if start >= end:
            return b""

        reader = await self.fs.async_fs.open(self.path, "rb")
        try:
            await reader.seek(start)
            return await reader.read(end - start)
        finally:
            await reader.close()

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
            await self.fs.async_fs.write(self.path, chunk, append=True)
            return None

        if self._opendal_writer is None:
            self._opendal_writer = await self.fs.async_fs.open(self.path, "wb")

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
                except FileNotFoundError:
                    existing = b""
                if existing:
                    self._opendal_writer = await self.fs.async_fs.open(self.path, "wb")
                    await self._opendal_writer.write(existing)
                    self.offset = len(existing)

        self._initiated = True

    async def _commit_upload(self) -> None:
        if self.mode == "ab" and self._append_via_write:
            return

        if self._opendal_writer is None:
            await self.fs.async_fs.write(self.path, b"")
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
