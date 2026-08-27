# opendalfs

`opendalfs` makes storage services supported by Apache OpenDAL available through
the fsspec API. You can use the same filesystem interface with pandas, Dask,
Xarray, Zarr, PyArrow, and other libraries that accept fsspec URLs or filesystem
objects.

## Install and try it

Install `opendalfs` from PyPI:

```console
pip install opendalfs
```

Then create an in-memory filesystem and use the familiar fsspec interface. This
example does not need credentials or leave files on disk:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
fs.pipe_file("hello.txt", b"hello from opendalfs\n")

assert fs.cat_file("hello.txt") == b"hello from opendalfs\n"
```

Continue with the {doc}`user-guide/quickstart` to use file-like objects and
fsspec URLs.

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Get started
:link: user-guide/quickstart
:link-type: doc

Follow the first user guide from installation through fsspec URL access.
:::

:::{grid-item-card} Connect storage
:link: user-guide/connecting-to-storage
:link-type: doc

Choose an OpenDAL service and pass its configuration through fsspec.
:::

:::{grid-item-card} Use an integration
:link: integrations/index
:link-type: doc

Find examples backed by the repository's integration tests.
:::

:::{grid-item-card} Read the API reference
:link: reference/api
:link-type: doc

Inspect the public classes and registration helpers provided by `opendalfs`.
:::
::::

## Where opendalfs fits

OpenDAL connects to storage services. fsspec defines the Python filesystem
interface used by the wider data ecosystem. `opendalfs` adapts the OpenDAL
Python binding to that interface.

Configuration travels down through the stack, while file operations travel up:

```text
pandas, Dask, Xarray, Zarr, PyArrow, ...
                    |
               fsspec API
                    |
                opendalfs
                    |
         OpenDAL Python binding
                    |
          S3, GCS, Azure, ...
```

OpenDAL remains the source of truth for service configuration. This site
documents the fsspec adapter, its URL rules, and the integrations tested in this
repository.

```{toctree}
:hidden:
:maxdepth: 2

user-guide/index
concepts/index
integrations/index
reference/index
contributing/index
```
