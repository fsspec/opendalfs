import importlib

from fsspec import AbstractFileSystem


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

    def ls(self, path, **kwargs):
        return self.fs.ls(path)