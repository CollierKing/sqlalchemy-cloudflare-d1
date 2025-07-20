# Development Guide

This document explains the development setup, tools, and CI/CD pipeline for the SQLAlchemy Cloudflare D1 dialect.

## Setup

### Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) package manager

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/collierking/sqlalchemy-cloudflare-d1.git
cd sqlalchemy-cloudflare-d1

# Install dependencies
make install

# Setup pre-commit hooks
make setup_hooks
```

## Development Tools

We use the following tools for code quality and consistency:

- **Ruff**: Fast Python linter and formatter (replaces black, isort, flake8)
- **mypy**: Static type checker
- **codespell**: Spell checker for code and documentation
- **pre-commit**: Automated checks before commits
- **pytest**: Testing framework

## Available Commands

### Common Development Tasks

```bash
# Install dependencies
make install

# Format code (auto-fix)
make format

# Run linting checks
make lint

# Run tests
make test

# Run all checks (lint + test)
make check

# Build package
make build

# Install pre-commit hooks
make setup_hooks

# Run spell check
make spell_check
```

### Testing

```bash
# Run unit tests (no network access)
make test

# Run tests with file watcher (auto-reload)
make test_watch

# Run integration tests (with network access)
make integration_test
```

### Code Quality

```bash
# Auto-format code and fix linting issues
make format

# Check formatting and run linters (no auto-fix)
make lint

# Check only changed files
make lint_diff

# Spell check
make spell_check

# Fix spelling issues
make spell_fix
```

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration and deployment:

### Workflows

1. **CI** (`.github/workflows/ci.yml`):
   - Runs on every push and pull request
   - Pre-commit checks
   - Linting across Python 3.9 and 3.12
   - Testing across Python 3.9-3.12
   - Package building

2. **Release** (`.github/workflows/release.yml`):
   - Manual trigger for releases
   - Runs full test suite
   - Builds package
   - Publishes to PyPI using trusted publishing
   - Creates GitHub release

### Pre-commit Hooks

The following checks run automatically before each commit:

- YAML syntax validation
- Trailing whitespace removal
- Large file detection
- Merge conflict detection
- Ruff formatting and linting
- mypy type checking
- Spell checking

## Configuration Files

- `pyproject.toml`: Project metadata, dependencies, and tool configuration
- `.pre-commit-config.yaml`: Pre-commit hook configuration
- `Makefile`: Development commands
- `.github/workflows/`: CI/CD pipeline definitions

## Tool Configuration

### Ruff

Configured in `pyproject.toml` under `[tool.ruff]`:

- Line length: 88 characters
- Includes rules for: pycodestyle, pyflakes, isort, print statements, pyupgrade, bugbear, comprehensions
- Auto-fixes common issues

### mypy

Configured in `pyproject.toml` under `[tool.mypy]`:

- Strict type checking enabled
- Requires type annotations for all functions
- Ignores missing imports in test files

### pytest

Configured in `pyproject.toml` under `[tool.pytest.ini_options]`:

- Strict markers and config
- Shows test durations
- Socket access disabled by default (for unit tests)

## Release Process

1. Update version in `pyproject.toml`
2. Commit and push changes
3. Go to GitHub Actions → Release workflow
4. Click "Run workflow" and enter the version number
5. The workflow will:
   - Run all tests
   - Build the package
   - Publish to PyPI
   - Create a GitHub release

## Trusted Publishing

The project uses PyPI's trusted publishing feature, which eliminates the need for API tokens. The publisher is configured on PyPI to trust releases from the `main` branch of this repository's release workflow.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make check` to ensure all tests pass
5. Commit your changes (pre-commit hooks will run)
6. Push and create a pull request

The CI pipeline will automatically run all checks on your pull request.
