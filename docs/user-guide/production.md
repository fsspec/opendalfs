# Production use

Production settings depend on both fsspec behavior and the selected OpenDAL
service. Start with the defaults, measure the actual workload, then adjust the
settings that address a specific limit.

## Retries

`opendalfs` applies OpenDAL's retry layer by default:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    region="us-east-1",
    retries=5,
)
```

Set `retries=0` to disable adapter-level retries. Coordinate this value with
retries performed by the application or an orchestration framework so a single
failure does not cause an unexpected number of attempts.

## Concurrent writes

Opened writers can upload parts concurrently:

```python
fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    region="us-east-1",
    write_concurrent=8,
    write_chunk=16 * 1024 * 1024,
)
```

`write_concurrent` controls the number of concurrent part uploads.
`write_chunk` sets the part size in bytes. Leaving `write_chunk=None` uses the
backend default. Both options can be overridden for one file through `fs.open`.

Reduce concurrency when an endpoint, network, or worker has a lower practical
limit.

## Capability-dependent operations

Copy, rename, multipart upload, and other optional operations vary by service.
An unsupported operation raises an OpenDAL error instead of silently changing
its meaning. See {doc}`../concepts/capabilities` before relying on an optional
operation across several services.

## Credentials

Do not put access keys in URLs, documentation, notebooks committed to source
control, or fsspec configuration files shared between users. Prefer the
credential mechanism documented for the service. If the application must pass
credentials explicitly, load them from its secret store at runtime.

## Validate the target service

The integration suite runs against memory, local filesystem, and an
S3-compatible MinIO service. Test production-specific features against the
actual provider, especially authentication, multipart upload, copy, rename,
and directory listing behavior.
