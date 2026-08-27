# opendalfs

[![Discussions](https://img.shields.io/github/discussions/fsspec/opendalfs)](https://github.com/fsspec/opendalfs/discussions)
[![Tests](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml)


`opendalfs` is a Python-based interface for file systems that enables interaction with different storage services by [Apache OpenDAL](https://github.com/apache/opendal). Through `opendalfs`, users can utilize fsspec's standard API to operate on all [storage services supported by OpenDAL](https://docs.rs/opendal/latest/opendal/services/index.html).

Read the [opendalfs documentation](https://opendalfs.readthedocs.io/en/latest/)
for installation, storage configuration, API details, and tested integrations.

## URL Protocols

`opendalfs` registers multiple fsspec protocols in the form of `opendal+<service>`, for example:

```python
import fsspec

f = fsspec.open(
    "opendal+s3://my-bucket/path/to/file",
    mode="rb",
    endpoint="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
)
```

The URL host is mapped to the service container (e.g. `bucket` for `s3`/`gcs`, `container` for `azblob`), and the URL path is used as the object key.

For other OpenDAL services, register protocols at runtime:

```python
import opendalfs

opendalfs.register_opendal_service("oss")
```

Services such as OSS that use the URL authority as an OpenDAL configuration
option are detected automatically. Other services use root-relative URLs and
receive their OpenDAL configuration through fsspec storage options.

## Installation

```bash
pip install opendalfs
```

## Status

See [Tracking issues of 0.1.0 version for opendalfs](https://github.com/fsspec/opendalfs/issues/6)

## Contributing

opendalfs is an exciting project currently under active development. Whether you're looking to use it in your projects or contribute to its growth, there are several ways you can get involved:

- Follow the [Contributing Guide](https://github.com/fsspec/opendalfs/blob/main/CONTRIBUTING.md) to contribute
- Create new [Issue](https://github.com/fsspec/opendalfs/issues/new) for bug reports or feature requests
- Join discussions in [Discussions](https://github.com/fsspec/opendalfs/discussions)

## Getting Help

- Submit [issues](https://github.com/fsspec/opendalfs/issues/new/choose) for bug reports
- Ask questions in [discussions](https://github.com/fsspec/opendalfs/discussions/new?category=q-a)

## License

Licensed under [Apache License, Version 2.0](https://github.com/fsspec/opendalfs/blob/main/LICENSE).
