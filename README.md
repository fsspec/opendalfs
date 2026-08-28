# opendalfs

[![PyPI](https://img.shields.io/pypi/v/opendalfs)](https://pypi.org/project/opendalfs/)
[![Tests](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml)
[![License](https://img.shields.io/github/license/fsspec/opendalfs)](https://github.com/fsspec/opendalfs/blob/main/LICENSE)

`opendalfs` is a [fsspec](https://filesystem-spec.readthedocs.io/) filesystem
backed by [Apache OpenDAL](https://opendal.apache.org/). It lets Python libraries
that work with fsspec use storage services supported by OpenDAL.

**Documentation:** [opendalfs.readthedocs.io](https://opendalfs.readthedocs.io/)

## Installation

`opendalfs` requires Python 3.12 or newer.

```console
pip install opendalfs
```

## Quick start

The OpenDAL memory service provides a small example that needs no credentials
and writes nothing to disk:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
fs.pipe_file("hello.txt", b"hello from opendalfs\n")

assert fs.cat_file("hello.txt") == b"hello from opendalfs\n"
```

`OpendalFileSystem` implements the fsspec filesystem interface, including
methods such as `open`, `ls`, `glob`, `info`, and `rm`.

## Connect to storage

The package registers fsspec protocols for S3, Google Cloud Storage, and Azure
Blob Storage:

```python
import fsspec

fs = fsspec.filesystem(
    "opendal+s3",
    bucket="my-bucket",
    region="us-east-1",
)
```

These protocols also work in URLs accepted by fsspec-compatible libraries:

```text
opendal+s3://my-bucket/path/to/file
opendal+gcs://my-bucket/path/to/file
opendal+azblob://my-container/path/to/file
```

Register other OpenDAL services at runtime:

```python
import fsspec

from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
```

Service options are passed to OpenDAL without being renamed. See the
[documentation](https://opendalfs.readthedocs.io/) for storage configuration,
URL rules, supported operations, tested integrations, and the API reference.

## Community

- Read the [contributing guide](https://github.com/fsspec/opendalfs/blob/main/CONTRIBUTING.md)
  to set up a development environment and submit changes.
- Open an [issue](https://github.com/fsspec/opendalfs/issues/new/choose) for bugs
  and feature requests.
- Use [GitHub Discussions](https://github.com/fsspec/opendalfs/discussions) for
  questions and general discussion.

## License

`opendalfs` is licensed under the
[Apache License 2.0](https://github.com/fsspec/opendalfs/blob/main/LICENSE).
