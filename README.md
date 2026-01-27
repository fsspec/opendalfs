# opendalfs

[![Discussions](https://img.shields.io/github/discussions/fsspec/opendalfs)](https://github.com/fsspec/opendalfs/discussions)
[![Tests](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml)
[![Package](https://github.com/fsspec/opendalfs/actions/workflows/package.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/package.yml)


`opendalfs` is a Python-based interface for file systems that enables interaction with different storage services by [Apache OpenDAL](https://github.com/apache/opendal). Through `opendalfs`, users can utilize fsspec's standard API to operate on all [storage services supported by OpenDAL](https://docs.rs/opendal/latest/opendal/services/index.html).

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

## Installation

### Basic Installation

```bash
pip install opendalfs
```

### Development Installation

```bash
# Install all development dependencies
pip install "opendalfs[all]"

# Or install specific groups
pip install "opendalfs[dev,test]"  # for development and testing
```

## Development Setup

This project uses:
- Python 3.11+ for the Python interface
- ruff for code formatting and linting
- pytest for testing

For development setup and guidelines, see our [Contributing Guide](CONTRIBUTING.md).

## Benchmarks

The benchmark scripts compare Arrow direct, opendalfs (fsspec), and s3fs (fsspec) on MinIO.
Write and read are separate commands; the read phase reuses the manifest produced by the write phase.

### MinIO Setup

Configure MinIO access via environment variables:
`OPENDAL_S3_ENDPOINT`, `OPENDAL_S3_BUCKET`, `OPENDAL_S3_ACCESS_KEY_ID`, `OPENDAL_S3_SECRET_ACCESS_KEY`.
Ensure the bucket already exists; the benchmark will not create it.

Download the official MinIO binary and start it locally:

```bash
curl -Lo ./minio https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x ./minio
./minio server ./minio-data --console-address ":9001"
```

### Quick Start

```bash
uv sync --extra bench
uv run python bench/bench_read_write.py write \
  --sizes 16,32,64 --files 4 --workers 4 \
  --stream-buffer-size 0 --cache-type none \
  --io-chunk 8388608 --io-concurrency 4 \
  --manifest /tmp/opendalfs_bench_manifest.json
uv run python bench/bench_read_write.py read \
  --sizes 16,32,64 --files 4 --workers 4 \
  --stream-buffer-size 0 --cache-type none \
  --io-chunk 8388608 --io-concurrency 4 \
  --manifest /tmp/opendalfs_bench_manifest.json
```

For cold-read comparisons, prefer `--rounds 1 --warmup-rounds 0`.
When using multiple rounds, the read phase will likely include warmed caches.

### Profiling

```bash
uv tool install py-spy
uv run python bench/bench_read_write.py write \
  --sizes 16,32,64 --files 4 --workers 4 \
  --stream-buffer-size 0 --cache-type none \
  --io-chunk 8388608 --io-concurrency 4 \
  --manifest /tmp/opendalfs_bench_manifest.json
uv tool run py-spy record -o bench.svg -- \
  .venv/bin/python bench/bench_read_write.py read --sizes 16,32,64 --files 4 --workers 4 \
  --stream-buffer-size 0 --cache-type none --io-chunk 8388608 --io-concurrency 4 \
  --manifest /tmp/opendalfs_bench_manifest.json
```

If high write concurrency causes timeouts, reduce `--fsspec-workers`.

## Status

See [Tracking issues of 0.1.0 version for opendalfs](https://github.com/fsspec/opendalfs/issues/6)

## Contributing

opendalfs is an exciting project currently under active development. Whether you're looking to use it in your projects or contribute to its growth, there are several ways you can get involved:

- Follow the [Contributing Guide](CONTRIBUTING.md) to contribute
- Create new [Issue](https://github.com/fsspec/opendalfs/issues/new) for bug reports or feature requests
- Join discussions in [Discussions](https://github.com/fsspec/opendalfs/discussions)

## Getting Help

- Submit [issues](https://github.com/fsspec/opendalfs/issues/new/choose) for bug reports
- Ask questions in [discussions](https://github.com/fsspec/opendalfs/discussions/new?category=q-a)

## License

Licensed under [Apache License, Version 2.0](./LICENSE).
