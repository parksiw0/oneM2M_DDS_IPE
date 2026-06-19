.PHONY: help install dev lint format clean run-dryrun run

PYTHON := python3

help:
	@echo "Available targets:"
	@echo "  install          Install package (production)"
	@echo "  dev              Install with dev dependencies"
	@echo "  lint             Run ruff and mypy"
	@echo "  format           Auto-format code with ruff"
	@echo "  clean            Remove build artifacts"
	@echo "  run-dryrun       Validate config without running"
	@echo "  run              Run IPE with config/profiles/px4.yaml"

install:
	$(PYTHON) -m pip install -e .

dev:
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	ruff check src/
	mypy src/

format:
	ruff format src/
	ruff check --fix src/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage

run-dryrun:
	$(PYTHON) -m ipe --config config/profiles/px4.yaml --dry-run

run:
	$(PYTHON) -m ipe --config config/profiles/px4.yaml
