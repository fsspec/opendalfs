PYTEST_ARGS ?= -v  # Override with e.g. PYTEST_ARGS="-vv --tb=short"

test: test-setup test-exec

test-setup:
	docker compose -f tests/docker/docker-compose.yml kill
	docker compose -f tests/docker/docker-compose.yml rm -f
	docker compose -f tests/docker/docker-compose.yml up -d
	sleep 5

test-exec:
	uv run pytest $(PYTEST_ARGS)

test-rebuild:
	docker compose -f tests/docker/docker-compose.yml kill
	docker compose -f tests/docker/docker-compose.yml rm -f
	docker compose -f tests/docker/docker-compose.yml build --no-cache
