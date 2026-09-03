# Connecting to storage

An `opendalfs` filesystem needs an OpenDAL service name and that service's
configuration. Choose the construction style that matches the library you are
using.

## Construct the filesystem directly

Use {class}`opendalfs.OpendalFileSystem` when your code controls the filesystem
object:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    region="us-east-1",
    endpoint="https://s3.amazonaws.com",
)
```

The first argument selects the OpenDAL service. Remaining service-specific
keyword arguments are passed to the OpenDAL Python binding.

## Ask fsspec for a registered filesystem

The package installs fsspec entry points for S3, Google Cloud Storage, and Azure
Blob:

```python
import fsspec

fs = fsspec.filesystem(
    "opendal+s3",
    bucket="my-bucket",
    region="us-east-1",
)
```

## Keep an existing S3 URL

Applications that cannot rewrite existing `s3://` URLs can explicitly replace fsspec's S3 implementation:

```python
import fsspec
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)

fs, path = fsspec.core.url_to_fs(
    "s3://my-bucket/reports/2026.csv",
    key="access-key",
    secret="secret-key",
    client_kwargs={"region_name": "us-east-1"},
)
```

The registration call is process-wide and should run during application startup, before constructing an S3 filesystem.
`S3FileSystem` translates common s3fs names such as `key`, `secret`, `token`, `anon`, and supported `client_kwargs`.
Installing `opendalfs` alone never changes `s3://`.

An OpenDAL S3 operator is scoped to one bucket, so each `S3FileSystem` instance is also scoped to one bucket.
Independent fsspec calls can use different buckets.
A single multi-path operation spanning buckets raises `ValueError` instead of sending a path to the wrong bucket.

## Understand OpenDAL URLs

The installed URL protocols use this form:

```text
opendal+<service>://<authority>/<path>
```

The authority supplies the bucket or container:

```text
opendal+s3://my-bucket/reports/2026.csv
opendal+gcs://my-bucket/reports/2026.csv
opendal+azblob://my-container/reports/2026.csv
```

Other OpenDAL services intentionally have no URL adapter.
Construct `OpendalFileSystem` directly and pass the filesystem, mapping, or opened file to the consuming library.

## Find service options

OpenDAL maintains the configuration reference for every service. Consult the
[OpenDAL service directory](https://opendal.apache.org/services/) for option
names, required fields, credential behavior, and backend-specific notes.

The installed `opendal+...` protocols pass OpenDAL option names through unchanged.
The opt-in `S3FileSystem` adapter accepts the common s3fs aliases listed in {doc}`../reference/configuration`.

Keep credentials outside source code. Read them from the provider's standard
environment, a secret manager, or environment variables that your application
passes into `storage_options`.
