# Changelog

All notable changes to sqlalchemy-cloudflare-d1 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
### Changed
### Fixed


## [0.3.1]

### Added

- `create_engine_from_binding()` function for full SQLAlchemy Core/ORM support inside Cloudflare Python Workers
- `SyncWorkerConnection` and `SyncWorkerCursor` classes for synchronous DBAPI-compatible interface in Workers
- `WorkerDBAPI` class providing DBAPI 2.0 module interface for Worker bindings
- Lazy loading for `CloudflareD1Dialect_async` to avoid requiring greenlet at import time (enables package use in Pyodide/Workers without greenlet)

### Changed

- Moved async dialect import to `__getattr__` for lazy loading, preventing ImportError in environments without greenlet

### Fixed

- D1 binding support now works with SQLAlchemy's `create_engine()` via `pyodide.ffi.run_sync()` for async-to-sync bridging


## [0.3.0]

### Changed

- Refactored compilers to inherit from SQLite base classes (`SQLiteCompiler`, `SQLiteDDLCompiler`, `SQLiteTypeCompiler`) instead of generic SQLAlchemy compilers for better SQLite compatibility

### Added

- Support for `INSERT ... ON CONFLICT DO UPDATE` (upsert operations) via SQLite dialect inheritance
- New unit tests for upsert compilation, DDL generation, and compiler inheritance

### Fixed

- Fixed duplicate PRIMARY KEY constraint in CREATE TABLE statements (was generating both inline and separate constraint)
- Fixed AUTOINCREMENT being incorrectly added to non-INTEGER PRIMARY KEY columns (D1 only supports AUTOINCREMENT on INTEGER PRIMARY KEY)


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
