set positional-arguments

uv := env("UV", "uv")
compose := env("COMPOSE", "docker compose")
compose_file := env("COMPOSE_FILE", "docker-compose.yml")

# List available development commands.
default:
    @just --list

# Install all locked development dependencies.
install:
    {{ uv }} sync --locked --all-extras --dev

# Check formatting, lint, and types.
check:
    {{ uv }} run ruff format --check .
    {{ uv }} run ruff check .
    {{ uv }} run ty check

# Run the complete test suite against available services.
test *args:
    {{ uv }} run pytest -v "$@"

# Run tests that do not require S3.
unit *args:
    {{ uv }} run pytest -v -k "not s3" "$@"

# Run the complete test suite with a managed MinIO service.
integration *args:
    #!/usr/bin/env bash
    set -euo pipefail
    trap '{{ compose }} -f "{{ compose_file }}" down --volumes' EXIT
    {{ compose }} -f "{{ compose_file }}" up -d --wait
    {{ uv }} run pytest -v "$@"

# Start the MinIO service used by benchmarks.
bench-up:
    {{ compose }} -f "{{ compose_file }}" up -d --wait

# Run the storage benchmark against MinIO.
bench *args: bench-up
    {{ uv }} run python bench/bench_read_write.py "$@"

# Stop the MinIO service used by benchmarks.
bench-down:
    {{ compose }} -f "{{ compose_file }}" down --volumes
