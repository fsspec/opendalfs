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

    def _fetch_range(self, start: int, end: int):
        """Download data between start and end"""
        pass

    def _upload_chunk(self, final: bool = False):
        """Upload partial chunk of data"""
        pass

    def _initiate_upload(self) -> None:
        """Prepare for uploading"""
        pass

    def _commit_upload(self) -> None:
        """Ensure upload is complete"""
        pass

    def close(self):
        """Ensure data is written before closing"""
        pass
