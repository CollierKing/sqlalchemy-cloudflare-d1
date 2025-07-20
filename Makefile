.PHONY: all format lint test tests integration_tests help install clean build check spell_check spell_fix

# Default target executed when no arguments are given to make.
all: help

## Install the package and development dependencies
install:
	uv sync --dev

## Run tests
test tests:
	uv run pytest --disable-socket --allow-unix-socket tests/

## Run tests with watcher
test_watch:
	uv run ptw --snapshot-update --now . -- -vv tests/

## Run integration tests (with network access)
integration_test integration_tests:
	uv run pytest -m integration tests/

## Run all checks (lint + test)
check: lint test

######################
# LINTING AND FORMATTING
######################

## Run linting (ruff check + format check + mypy)
lint:
	uv run ruff check .
	uv run ruff format . --diff
	uv run mypy src/

## Run linting on specific files
lint_diff:
	@if [ -n "$$(git diff --name-only --diff-filter=d HEAD^ | grep -E '\.py$$')" ]; then \
		echo "Linting changed files..."; \
		uv run ruff check $$(git diff --name-only --diff-filter=d HEAD^ | grep -E '\.py$$' | tr '\n' ' '); \
		uv run ruff format $$(git diff --name-only --diff-filter=d HEAD^ | grep -E '\.py$$' | tr '\n' ' ') --diff; \
		uv run mypy $$(git diff --name-only --diff-filter=d HEAD^ | grep -E '\.py$$' | tr '\n' ' '); \
	else \
		echo "No Python files changed."; \
	fi

## Fix formatting and linting issues
format:
	uv run ruff format .
	uv run ruff check . --fix

## Spell check
spell_check:
	uv run codespell

## Fix spelling issues
spell_fix:
	uv run codespell --write-changes

######################
# BUILD AND PUBLISH
######################

## Clean build artifacts
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

## Build the package
build: clean
	uv build

## Install pre-commit hooks
setup_hooks:
	uv run pre-commit install

## Run pre-commit on all files
pre_commit_all:
	uv run pre-commit run --all-files

######################
# HELP
######################

## Show help
help:
	@echo ''
	@echo 'Usage:'
	@echo '  make <target>'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) { \
			helpCommand = $$1; \
			helpMessage = $$2; \
			printf "  %-20s %s\n", helpCommand, helpMessage; \
		} \
	}' $(MAKEFILE_LIST)
