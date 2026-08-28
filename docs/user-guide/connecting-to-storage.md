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

## Register another OpenDAL service

Other OpenDAL services can be registered for the current Python process. The
memory service keeps this example credential-free:

```python
from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
```

Register several services together when an application needs them during
startup:

```python
from opendalfs import register_opendal_protocols

protocols = register_opendal_protocols(["memory", "fs", "oss"])
```

Repeating a registration is safe. Libraries that create their filesystem while
loading a dataset, catalog, or path object need registration to happen first.

## Pass a URL to another library

Libraries such as pandas and Dask usually accept an fsspec URL. This complete
example uses the memory service so it can run without credentials:

```python
import fsspec
import pandas as pd

from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
fs.pipe_file("data/table.csv", b"name,value\nalice,1\nbob,2\n")

frame = pd.read_csv("opendal+memory:///data/table.csv")
assert frame["value"].tolist() == [1, 2]
```

For a configured S3 service, use a URL such as
`opendal+s3://my-bucket/data/table.parquet` and pass options such as `region`
through `storage_options`. Do not repeat `bucket` in `storage_options` when it
is already present in the URL.

## Understand the URL

An `opendalfs` URL uses this form:

```text
opendal+<service>://<authority>/<path>
```

Object stores use the authority as their bucket or container:

```text
opendal+s3://my-bucket/reports/2026.csv
opendal+gcs://my-bucket/reports/2026.csv
opendal+azblob://my-container/reports/2026.csv
```

Services without a bucket-like scope use a hostless URL:

```text
opendal+memory:///cache/item.bin
opendal+fs:///reports/2026.csv
```

The three slashes preserve the root-relative path while leaving the authority
empty. Service options belong in keyword arguments or `storage_options`, not in
the URL query string.

## Find service options

OpenDAL maintains the configuration reference for every service. Consult the
[OpenDAL service directory](https://opendal.apache.org/services/) for option
names, required fields, credential behavior, and backend-specific notes.

`opendalfs` does not rename those options. For example, OpenDAL's
`access_key_id`, `secret_access_key`, and `endpoint` options use the same names
when passed through fsspec.

Keep credentials outside source code. Read them from the provider's standard
environment, a secret manager, or environment variables that your application
passes into `storage_options`.

See {doc}`../reference/protocols` for the full list of authority mappings known
to the runtime registration helper.
