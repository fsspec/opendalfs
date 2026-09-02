# Configuration reference

`OpendalFileSystem` accepts adapter settings, fsspec settings, and OpenDAL
service settings in one constructor.

## Adapter settings

| Option | Default | Meaning |
| --- | --- | --- |
| `scheme` | required | OpenDAL service name, such as `s3` or `memory` |
| `retries` | `5` | Maximum attempts configured on the OpenDAL retry layer |
| `write_concurrent` | `8` | Concurrent part uploads for opened writers |
| `write_chunk` | backend default | Part size in bytes for opened writers |

`write_concurrent` and `write_chunk` can also be passed to `fs.open` for one
writer.

## fsspec settings

| Option | Default | Meaning |
| --- | --- | --- |
| `asynchronous` | `False` | Use the async implementation directly |
| `loop` | fsspec default | Event loop used by sync wrappers |
| `batch_size` | fsspec default | Maximum concurrent batch operations |
| `use_listings_cache` | `True` | Cache directory listings |
| `listings_expiry_time` | no expiry | Expire cached listings after this many seconds |
| `max_paths` | unlimited | Maximum cached directory listings |
| `skip_instance_cache` | `False` | Bypass fsspec filesystem instance reuse |

Additional arguments accepted by {class}`fsspec.asyn.AsyncFileSystem` pass to
its constructor.

## Service settings

All remaining keyword arguments configure the OpenDAL service:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    endpoint="https://s3.amazonaws.com",
    region="us-east-1",
)
```

Use the [OpenDAL service directory](https://opendal.apache.org/services/) as the
configuration reference. Option names pass through unchanged.

The `S3FileSystem` adapter accepts these common `s3fs` aliases:

| s3fs option | OpenDAL S3 option |
| --- | --- |
| `key` | `access_key_id` |
| `secret` | `secret_access_key` |
| `token` | `session_token` |
| `anon` | `skip_signature` |
| `endpoint_url` | `endpoint` |
| `requester_pays` | `enable_request_payer` |
| `client_kwargs.aws_access_key_id` | `access_key_id` |
| `client_kwargs.aws_secret_access_key` | `secret_access_key` |
| `client_kwargs.aws_session_token` | `session_token` |
| `client_kwargs.endpoint_url` | `endpoint` |
| `client_kwargs.region_name` | `region` |

OpenDAL option names take precedence when both forms are provided. Unsupported
nested `client_kwargs` raise `TypeError`.

## URL-derived settings

Registered service adapters can derive one setting from the URL authority. For
example, `opendal+s3://my-bucket/path` supplies `bucket="my-bucket"`.
Explicit filesystem construction requires the bucket keyword instead.

Do not provide conflicting values through the URL and keyword arguments.
