.PHONY: help install dev test test-unit test-integration test-e2e lint format clean run-dryrun run

PYTHON := python3

help:
	@echo "Available targets:"
	@echo "  install          Install package (production)"
	@echo "  dev              Install with dev dependencies"
	@echo "  test             Run unit tests (default fast set)"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests (needs TinyIoT)"
	@echo "  test-e2e         Run e2e tests (needs full sim)"
	@echo "  lint             Run ruff and mypy"
	@echo "  format           Auto-format code with ruff"
	@echo "  clean            Remove build artifacts"
	@echo "  run-dryrun       Validate config without running"
	@echo "  run              Run IPE with config/px4.yaml"

install:
	$(PYTHON) -m pip install -e .

dev:
	$(PYTHON) -m pip install -e ".[dev]"

test: test-unit

test-unit:
	$(PYTHON) -m pytest tests/unit

test-integration:
	$(PYTHON) -m pytest tests/integration -m integration

test-e2e:
	$(PYTHON) -m pytest tests/e2e -m e2e

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage

run-dryrun:
	$(PYTHON) -m ipe --config config/px4.yaml --dry-run

run:
	$(PYTHON) -m ipe --config config/px4.yaml
