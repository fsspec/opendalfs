# Architecture

`opendalfs` connects two existing abstractions. OpenDAL supplies storage
services and operations. fsspec supplies the filesystem interface used by
Python data libraries.

## The request path

When pandas opens an `s3` or `opendal+s3` URL, the request passes through these layers:

```text
pandas
  -> fsspec URL resolution
  -> opendalfs filesystem method
  -> OpenDAL Python operator
  -> storage service
```

The reverse path returns bytes, metadata, directory entries, or a file-like
object in the shape expected by fsspec.

## One filesystem, one service configuration

An {class}`opendalfs.OpendalFileSystem` owns one OpenDAL operator. The operator
is configured for one service and one root. Create another filesystem for a
second bucket, container, or root.

```python
from opendalfs import OpendalFileSystem

incoming = OpendalFileSystem("s3", bucket="incoming", region="us-east-1")
archive = OpendalFileSystem("s3", bucket="archive", region="us-east-1")
```

fsspec may cache instances created with identical arguments. This is useful for
connection reuse, but it does not merge different service configurations.

## What the adapter owns

`opendalfs` is responsible for:

- fsspec method shapes and metadata dictionaries
- sync wrappers around async storage operations
- buffered file behavior
- URL protocol registration
- translating URL authorities and paths into operator-relative paths
- adapter settings such as retries and concurrent writes

OpenDAL remains responsible for service clients, credentials, backend-specific
configuration, and the native capability of each service.

## One adapter model

The installed protocol adapters share one filesystem implementation.
Each adapter declares its fixed OpenDAL service and how the URL authority maps to that service's scope.
Other services use `OpendalFileSystem` directly.

`S3FileSystem` accepts common `s3fs` constructor names and preserves the bucket-in-path convention expected by standard `s3://` consumers.
Applications can register that class through fsspec when they intentionally want OpenDAL to handle `s3://`.

## Why integrations work

Most integrations do not know about OpenDAL. They call fsspec with a URL,
filesystem, mapping, or file-like object. Compatibility depends on how closely
the adapter follows those fsspec contracts. The repository therefore tests
real downstream entry points in addition to its own filesystem methods.
