# Contributing

opendalfs is a Python package built on the Apache OpenDAL Python bindings.

## Development Setup

1. Clone the repository:

```shell
git clone https://github.com/fsspec/opendalfs.git
cd opendalfs
```

2. Install [uv](https://docs.astral.sh/uv/) and
   [just](https://just.systems/man/en/packages.html), then install the locked
   development environment:

```shell
just install
```

### Dependency Groups

The project uses several dependency groups:

- `dev`: Development tools (Ruff and ty)
- `test`: Testing tools (pytest, pytest-asyncio, pytest-cov, s3fs, boto3)
- `bench`: Benchmark tools (pyarrow, s3fs, boto3)
- `all`: All dependencies combined

Run `just --list` to list the supported development commands.

## Testing

### Unit Tests

Run tests that do not require S3:

```shell
just unit
```

Pass additional pytest options as recipe arguments, for example:

```shell
just unit -x
```

### Integration Tests

Run the complete test suite with the repository's root `docker-compose.yml`:

```shell
just integration
```

This command starts MinIO, waits for it to become healthy, runs the tests, and
stops MinIO afterward. The S3 tests use these default settings:

- Endpoint: `http://localhost:9000`
- Region: `us-east-1`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Bucket: `test-bucket`

Override them with `OPENDAL_S3_ENDPOINT`, `OPENDAL_S3_REGION`,
`OPENDAL_S3_BUCKET`, `OPENDAL_S3_ACCESS_KEY_ID`, and
`OPENDAL_S3_SECRET_ACCESS_KEY`. Compose and pytest read the same values.

To run the complete suite against services you already manage, use:

```shell
just test
```

## Benchmarks

Run the benchmark against the same root Compose service:

```shell
just bench
```

Pass benchmark options as recipe arguments. The service remains available for
repeated runs; stop it with `just bench-down` when finished.

## Code Quality

Run the same formatting, linting, and type checks used by CI:

```shell
just check
```

## CI/CD

Our GitHub Actions workflow runs tests with coverage against MinIO.

See `.github/workflows/` for detailed configurations.
