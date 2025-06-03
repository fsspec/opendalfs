# opendalfs

[![Discussions](https://img.shields.io/github/discussions/fsspec/opendalfs)](https://github.com/fsspec/opendalfs/discussions)
[![Tests](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml)
[![Package](https://github.com/fsspec/opendalfs/actions/workflows/package.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/package.yml)


`opendalfs` is a Python-based interface for file systems that enables interaction with different storage services by [Apache OpenDAL](https://github.com/apache/opendal). Through `opendalfs`, users can utilize fsspec's standard API to operate on all [storage services supported by OpenDAL](https://docs.rs/opendal/latest/opendal/services/index.html).

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
