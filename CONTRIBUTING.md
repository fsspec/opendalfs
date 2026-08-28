# Contributing

opendalfs is a Python package built on the Apache OpenDAL Python bindings.

## Development setup

Clone the repository:

```shell
git clone https://github.com/fsspec/opendalfs.git
cd opendalfs
```

Install [uv](https://docs.astral.sh/uv/) and
[just](https://just.systems/man/en/packages.html), then create the locked
development environment:

```shell
just install
```

Integration tests and benchmarks also require Docker with the Compose plugin.

Run `just --list` to list the supported development commands.

## Testing

### Unit tests

Run tests that do not require S3:

```shell
just unit
```

Pass additional pytest options as recipe arguments, for example:

```shell
just unit -x
```

### Integration tests

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

Optional downstream integrations use separate dependency groups because some
of their requirements conflict. The test workflow shows the group used for
each integration.

## Code quality

Run the same formatting, linting, and type checks used by CI:

```shell
just check
```

## Documentation

The documentation source is Markdown parsed by MyST and built by Sphinx. Run
the strict build and the examples available in the documentation environment:

```shell
just docs
just docs-examples
```

The HTML output is written to `docs/_build/html`. Use the live-reload server
while editing:

```shell
just docs-serve
```

Integration pages with conflicting optional dependencies run in separate CI
jobs. To run one of those pages locally, select its dependency group:

```shell
DOCS_EXAMPLE_GROUP=integration-prefect \
  uv run --group integration-prefect pytest -q docs/check_examples.py
```

Keep each documentation page focused on one reader need:

- A tutorial leads a new user to a working result.
- A how-to guide solves a specific task.
- An explanation describes how or why the adapter behaves as it does.
- A reference page records options, protocols, compatibility, or API details.

Add or update an integration test before claiming compatibility. An integration
page should use the library's public fsspec entry form, include a runnable
example based on the test, state the operations covered by CI, record relevant
limits, and link to the test file.

OpenDAL owns service configuration and backend capability details. fsspec owns
the generic filesystem contract. Link to those references instead of copying
tables that can drift.

## Benchmarks

Run the benchmark against the same root Compose service:

```shell
just bench
```

Pass benchmark options as recipe arguments. The service remains available for
repeated runs; stop it with `just bench-down` when finished.

## CI/CD

GitHub Actions runs quality checks, the test matrix, and documentation builds.

See `.github/workflows/` for detailed configurations.
