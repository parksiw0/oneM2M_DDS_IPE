.PHONY: help venv install dev lint format clean run-dryrun run

PYTHON := python3
VENV := .venv
VENV_PY := $(VENV)/bin/python

help:
	@echo "Available targets:"
	@echo "  venv             Create .venv (--system-site-packages for rclpy) + install [mqtt,dev]"
	@echo "  install          Install package + MQTT transport"
	@echo "  dev              Install with dev + MQTT dependencies"
	@echo "  lint             Run ruff and mypy"
	@echo "  format           Auto-format code with ruff"
	@echo "  clean            Remove build artifacts"
	@echo "  run-dryrun       Validate config without running"
	@echo "  run              Run IPE with config/profiles/px4.yaml"

# 한 번에 환경 구성. ROS2 브리징까지 쓰려면 먼저 source /opt/ros/<distro>/setup.bash
venv:
	$(PYTHON) -m venv --system-site-packages $(VENV)
	$(VENV_PY) -m pip install -U pip
	$(VENV_PY) -m pip install -e ".[mqtt,dev]"
	@echo "Done. Activate: source $(VENV)/bin/activate"

install:
	$(PYTHON) -m pip install -e ".[mqtt]"

dev:
	$(PYTHON) -m pip install -e ".[dev,mqtt]"

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
