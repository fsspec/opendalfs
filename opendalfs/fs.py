import importlib

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile


class OpendalFileSystem(AbstractFileSystem):
    def __init__(self, scheme, *args, **kwargs):
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
        raise NotImplementedError

    def mkdirs(self, path, exist_ok=False):
        raise NotImplementedError

    def rmdir(self, path):
        raise NotImplementedError

    def ls(self, path, **kwargs):
        return self.fs.ls(path, **kwargs)

    def info(self, path, **kwargs):
        raise NotImplementedError

    def rm_file(self, path):
        raise NotImplementedError

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        raise NotImplementedError

    def created(self, path):
        raise NotImplementedError

    def modified(self, path):
        raise NotImplementedError


class OpendalBufferedFile(AbstractBufferedFile):
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
        raise NotImplementedError

    def _upload_chunk(self, final=False):
        raise NotImplementedError

    def _initiate_upload(self):
        raise NotImplementedError

    def _fetch_range(self, start, end):
        raise NotImplementedError
