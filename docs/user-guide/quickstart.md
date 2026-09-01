# Quickstart

This tutorial installs `opendalfs` and uses OpenDAL's in-memory service for a
small read and write workflow. It leaves no files behind and does not need
credentials.

## Install opendalfs

`opendalfs` requires Python 3.12 or newer. Install it from PyPI:

```console
pip install opendalfs
```

The package installs OpenDAL and fsspec as dependencies.

## Create a filesystem

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
```

`OpendalFileSystem` implements the fsspec filesystem interface. Standard
filesystem methods work without calling the OpenDAL operator directly.

## Write and read a file

```python
fs.makedirs("demo", exist_ok=True)
fs.pipe_file("demo/hello.txt", b"hello from opendalfs\n")

assert fs.cat_file("demo/hello.txt") == b"hello from opendalfs\n"
assert fs.ls("demo", detail=False) == ["demo/hello.txt"]
```

The same filesystem also returns file-like objects:

```python
with fs.open("demo/notes.txt", "wt") as stream:
    stream.write("written through fsspec\n")

with fs.open("demo/notes.txt", "rt") as stream:
    assert stream.read() == "written through fsspec\n"
```

## Connect real storage

The filesystem operations do not change when you switch services. S3, GCS, and
Azure Blob also provide fsspec URLs for libraries that require one. The next
guide explains the available entry points and where service options go:
{doc}`connecting-to-storage`.
