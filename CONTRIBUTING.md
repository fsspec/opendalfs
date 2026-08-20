# Contributing

opendalfs is a Python package built on the Apache OpenDAL Python bindings.

## Development Setup

1. Clone the repository:
```shell
git clone https://github.com/fsspec/opendalfs.git
cd opendalfs
```

2. Create a virtual environment and install dependencies:
```shell
uv venv
uv sync --locked --all-extras --dev
```

### Dependency Groups

The project uses several dependency groups:
- `dev`: Development tools (ruff)
- `test`: Testing tools (pytest, pytest-asyncio, pytest-cov, s3fs, boto3)
- `bench`: Benchmark tools (pyarrow, s3fs, boto3)
- `all`: All dependencies combined

Install specific groups as needed:
```shell
pip install -e ".[dev,test]"  # For development and testing
pip install -e ".[bench]"     # For benchmarks
```

## Testing

### Prerequisites

1. For S3 tests, you need MinIO running locally:

```shell
docker compose -f tests/docker/docker-compose.yml up -d
```

Note: The S3 tests use these default settings:

- Endpoint: `http://localhost:9000`
- Region: `us-east-1`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Bucket: `test-bucket`

### Running Tests

1. Run the test suite:

```shell
pytest -v
```

2. After testing, stop MinIO:

```shell
docker compose -f tests/docker/docker-compose.yml down
```

## Code Style

- Format and lint: `ruff format .`
- Check: `ruff check .`

## CI/CD

Our GitHub Actions workflow runs tests with coverage against MinIO.

See `.github/workflows/` for detailed configurations.
