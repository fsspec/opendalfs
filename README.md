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

To keep an existing `s3://` URL, explicitly register the OpenDAL adapter with
fsspec:

```python
import fsspec
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)
```

Registration is process-wide and intentionally opt-in. Installing `opendalfs`
does not change the implementation of `s3://`.

Each `S3FileSystem` instance is scoped to one bucket. Separate filesystem
instances can access different buckets, but one multi-path operation cannot
span buckets.

OpenDAL service options are passed without being renamed; only the opt-in
`S3FileSystem` adapter translates common `s3fs` names. See the
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
