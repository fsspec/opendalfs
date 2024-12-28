import importlib
from typing import Any, List, Dict
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
import io


class OpendalFileSystem(AbstractFileSystem):
    """OpenDAL implementation of fsspec AbstractFileSystem"""

    def __init__(self, scheme: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        try:
            # Load the module dynamically based on scheme
            module = importlib.import_module(f"opendalfs_service_{scheme}")
            # Get the file system class based on scheme
            fs_class = getattr(module, f"{scheme.capitalize()}FileSystem")
            # initialize the file system with the kwargs
            self.fs = fs_class(**kwargs)
        except ImportError:
            raise ImportError(
                f"Cannot import opendal_service_{scheme}, please check if the module exists"
            )
        except AttributeError:
            raise AttributeError(
                f"Cannot find {scheme.capitalize()}FileSystem in opendal_service_{scheme}"
            )

    def fsid(self):
        raise NotImplementedError

    def mkdir(self, path, create_parents=True, **kwargs):
        return self.fs.mkdir(path, create_parents=create_parents, **kwargs)

    def mkdirs(self, path, exist_ok=False):
        return self.fs.mkdirs(path, exist_ok=exist_ok)

    def rmdir(self, path, recursive=False):
        return self.fs.rmdir(path, recursive=recursive)

    def ls(self, path: str, **kwargs: Any) -> List[str]:
        """List contents of path"""
        return self.fs.ls(path, **kwargs)

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        """Get info of path"""
        return self.fs.info(path, **kwargs)

    def rm_file(self, path):
        """Remove a file."""
        return self.fs.rm_file(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Return raw bytes-mode file-like from the filesystem"""
        return OpendalBufferedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        raise NotImplementedError("Creation time is not supported by OpenDAL")

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        try:
            return self.fs.modified(path)
        except Exception as e:
            if "Last modified time not available" in str(e):
                raise NotImplementedError("Modified time not supported by this backend")
            raise

    def _write(self, path, data):
        """Write data to path."""
        if isinstance(data, io.BytesIO):
            data = data.getvalue()
        elif not isinstance(data, bytes):
            data = str(data).encode()
        return self.fs._write(path, data)


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

    def _upload_chunk(self, final=False):
        """Upload partial chunk of data"""
        data = self.buffer.getvalue()
        self.fs.fs._write(self.path, data if isinstance(data, bytes) else data.encode())
        return True

    def _initiate_upload(self):
        """Prepare for uploading"""
        pass

    def _fetch_range(self, start, end):
        """Download data between start and end"""
        data = self.fs.fs._read(self.path)
        if not isinstance(data, bytes):
            data = bytes(data)
        return data[start:end]
