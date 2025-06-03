# Contributing

opendalfs is a hybrid of Rust and Python. The underlying implementation is written in Rust, with a Python interface provided by PyO3.

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
- `test`: Testing tools (pytest, pytest-asyncio, pytest-cov, fsspec, s3fs)
- `all`: All dependencies combined

Install specific groups as needed:
```shell
pip install -e ".[dev,test]"  # For development and testing
pip install -e ".[s3]"        # For S3 backend development
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

Our GitHub Actions workflows handle:
- Code formatting and linting
- Building packages
- Running tests with MinIO
- Package distribution checks

See `.github/workflows/` for detailed configurations.
