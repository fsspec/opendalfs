# Contributing

opendalfs is a Python package built on the Apache OpenDAL Python bindings.

## Development Setup

1. Clone the repository:

```shell
git clone https://github.com/fsspec/opendalfs.git
cd opendalfs
```

2. Install the locked development environment:

```shell
make install
```

### Dependency Groups

The project uses several dependency groups:

- `dev`: Development tools (Ruff, ty, and prek)
- `test`: Testing tools (pytest, pytest-asyncio, pytest-cov, s3fs, boto3)
- `bench`: Benchmark tools (pyarrow, s3fs, boto3)
- `all`: All dependencies combined

Run `make help` to list the supported development commands.

## Testing

### Unit Tests

Run tests that do not require S3:

```shell
make unit
```

Pass additional pytest options through `PYTEST_ARGS`, for example:

```shell
make unit PYTEST_ARGS="-x"
```

### Integration Tests

Run the complete test suite with the repository's root `docker-compose.yml`:

```shell
make integration
```

This command starts MinIO, waits for it to become healthy, runs the tests, and
stops MinIO afterward. The S3 tests use these default settings:

- Endpoint: `http://localhost:9000`
- Region: `us-east-1`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Bucket: `test-bucket`

To run the complete suite against services you already manage, use:

```shell
make test
```

## Benchmarks

Run the benchmark against the same root Compose service:

```shell
make bench
```

Pass benchmark options through `BENCH_ARGS`. The service remains available for
repeated runs; stop it with `make bench-down` when finished.

## Code Quality

Run the same formatting, linting, type, and pre-commit checks used by CI:

```shell
make check
```

## CI/CD

Our GitHub Actions workflow runs tests with coverage against MinIO.

See `.github/workflows/` for detailed configurations.
