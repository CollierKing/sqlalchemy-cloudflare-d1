# Changelog

All notable changes to sqlalchemy-cloudflare-d1 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
### Changed
### Fixed


## [0.3.5]

### Fixed

- Fixed single-row query results being lost in `cursor.description` for WorkerConnection
  - When a query returned exactly 1 row via the D1 Worker binding, `fetchall()` returned `[]` and `cursor.description` contained data values instead of column names
  - Root cause: D1's `raw({columnNames: true})` inconsistently omits the column names header for single-row results in remote deployments
  - Switched Worker binding from `raw()` to `all()` API which returns structured objects with named keys, eliminating fragile header-row parsing
  - Empty SELECT results still correctly populate `cursor.description` via a guarded `raw()` fallback


## [0.3.4]

### Added

- LargeBinary column type support ([#8](https://github.com/CollierKing/sqlalchemy-cloudflare-d1/issues/8))
  - Added `Binary()` method to DBAPI classes for SQLAlchemy compatibility
  - Added `D1LargeBinary` type processor for automatic base64 decoding on retrieval
  - BLOB columns now properly compile as BLOB (not TEXT) and reflect as LargeBinary
  - Binary data can now be stored and retrieved correctly in both REST API and Worker modes

### Enhanced

- Additional test coverage for ON CONFLICT clauses ([#9](https://github.com/CollierKing/sqlalchemy-cloudflare-d1/issues/9))
  - Added tests for ON CONFLICT DO NOTHING
  - Added tests for composite unique constraints
  - Added tests for conditional updates with WHERE clause
  - Note: ON CONFLICT support was added in v0.3.0, now with comprehensive test coverage


## [0.3.3]

### Fixed

- Fixed Boolean columns returning strings instead of Python `bool` ([#6](https://github.com/CollierKing/sqlalchemy-cloudflare-d1/issues/6))
  - D1's API was converting Python booleans to strings (`"true"`/`"false"`) which broke filtering
  - Added `D1Boolean` type class with `bind_processor` (converts `True`/`False` to `1`/`0`) and `result_processor` (converts responses back to Python `bool`)
  - Boolean column filtering (`WHERE is_admin = True`) now works correctly
- Fixed NULL parameter handling in Python Workers
  - Python `None` was being converted to JavaScript `undefined` instead of `null`
  - D1 rejected `undefined` values with `D1_TYPE_ERROR`
  - Now uses `JSON.parse("null")` to get proper JavaScript `null` value


## [0.3.2]

### Fixed

- Fixed `NoSuchColumnError` when querying empty result sets ([#4](https://github.com/CollierKing/sqlalchemy-cloudflare-d1/issues/4))
  - Queries returning 0 rows no longer raise `NoSuchColumnError: Could not locate column in row for column`
  - Switched REST API from `/query` to `/raw` endpoint to get column metadata even on empty results
  - Switched Worker binding from `stmt.all()` to `stmt.raw({columnNames: true})` for reliable column metadata

### Changed

- REST API now uses `/raw` endpoint instead of `/query` for query execution
- Worker bindings now use `raw({columnNames: true})` instead of `all()` for result fetching
- `cursor.description` is now reliably populated with column names even when results are empty


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
