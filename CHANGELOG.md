# Changelog

All notable changes to sqlalchemy-cloudflare-d1 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
### Changed
### Fixed


## [0.2.0]

### Added

- Python Worker binding support via `WorkerConnection` and `WorkerCursor` classes
- Async execution support for Workers with `execute_async()` method
- JsProxy to Python conversion for Pyodide/Workers runtime compatibility
- Example Python Worker demonstrating D1 integration
- Integration test suite for Python Workers


## [0.1.0] - Initial Release

### Added

- SQLAlchemy dialect for Cloudflare D1 (`cloudflare_d1://`)
- REST API connection support via `Connection` class (account_id, api_token, database_id)
- DBAPI 2.0 compliant interface with full exception hierarchy
- SQLite-compatible SQL compiler

---

## Release Template

When creating a new release, copy this template and fill in the details:

## [Version] - [Date]

### Added
- New features

### Changed
- Changes in existing functionality

### Deprecated
- Soon-to-be removed features

### Removed
- Now removed features

### Fixed
- Bug fixes

### Security
- Security vulnerability fixes
