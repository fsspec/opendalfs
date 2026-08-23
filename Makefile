.DEFAULT_GOAL := help

UV ?= uv
COMPOSE ?= docker compose
COMPOSE_FILE ?= docker-compose.yml
PYTEST_ARGS ?=
BENCH_ARGS ?=

.PHONY: help install check test unit integration bench-up bench bench-down

help: ## Show the available development commands.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "%-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install all locked development dependencies.
	$(UV) sync --locked --all-extras --dev

check: ## Run formatting, linting, type, and pre-commit checks.
	$(UV) run ruff format --check .
	$(UV) run ruff check .
	$(UV) run ty check
	$(UV) run prek run --all-files

test: ## Run the complete test suite against available services.
	$(UV) run pytest -v $(PYTEST_ARGS)

unit: ## Run tests that do not require S3.
	$(UV) run pytest -v -k "not s3" $(PYTEST_ARGS)

integration: ## Run the complete test suite with a managed MinIO service.
	@set -eu; \
		trap '$(COMPOSE) -f $(COMPOSE_FILE) down --volumes' EXIT; \
		$(COMPOSE) -f $(COMPOSE_FILE) up -d --wait; \
		$(UV) run pytest -v $(PYTEST_ARGS)

bench-up: ## Start the MinIO service used by benchmarks.
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --wait

bench: bench-up ## Run the storage benchmark against MinIO.
	$(UV) run python bench/bench_read_write.py $(BENCH_ARGS)

bench-down: ## Stop the MinIO service used by benchmarks.
	$(COMPOSE) -f $(COMPOSE_FILE) down --volumes
