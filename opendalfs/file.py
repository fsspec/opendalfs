from fsspec.spec import AbstractBufferedFile
import logging

logger = logging.getLogger("opendalfs")


class OpendalBufferedFile(AbstractBufferedFile):
    """Buffered file implementation for OpenDAL"""

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

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Download data between start and end."""
        logger.debug(f"Fetching bytes from {start} to {end} for {self.path}")
        data = self.fs.fs.read(self.path)  # sync operator
        return data[start:end]

    def _upload_chunk(self, final: bool = False):
        """No-op: we buffer until close and upload once."""
        pass

    def _initiate_upload(self) -> None:
        """Prepare for uploading"""
        logger.debug(f"Initiated upload for {self.path}")

    def _commit_upload(self) -> None:
        """Write the full buffer to the backend once"""
        logger.debug(f"Committing full upload for {self.path}")
        self.buffer.seek(0)
        data = self.buffer.read()
        self.fs.write(self.path, data)

    def close(self):
        """Ensure data is written before closing"""
        if not self.closed:
            if self.mode in ("wb", "ab"):
                self._commit_upload()
            super().close()
            logger.debug(f"Closed file {self.path}")
