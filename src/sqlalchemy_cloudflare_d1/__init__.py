"""
SQLAlchemy dialect for Cloudflare D1 Serverless SQLite Database.

This dialect provides connectivity to Cloudflare D1 databases via:
1. REST API - for external connections (account_id, api_token, database_id)
2. Worker Binding - for use inside Cloudflare Python Workers (d1_binding)
3. Async REST API - for async connections using create_async_engine()

Sync usage:
    from sqlalchemy import create_engine
    engine = create_engine("cloudflare_d1://account_id:api_token@database_id")

Async usage (requires greenlet: pip install sqlalchemy-cloudflare-d1[async]):
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("cloudflare_d1+async://account_id:api_token@database_id")
"""

from .dialect import CloudflareD1Dialect
from .connection import (
    # Sync classes
    Connection,
    Cursor,
    WorkerConnection,
    WorkerCursor,
    CloudflareD1DBAPI,
    connect,
    # Async classes (these don't need greenlet - just async/await)
    AsyncConnection,
    AsyncCursor,
    connect_async,
    # Worker engine support (for SQLAlchemy Core/ORM in Workers)
    WorkerDBAPI,
    SyncWorkerConnection,
    SyncWorkerCursor,
    create_engine_from_binding,
    # Exceptions
    Error,
    Warning,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)


def __getattr__(name: str):
    """Lazy import for async dialect to avoid requiring greenlet at import time.

    The async SQLAlchemy dialect (CloudflareD1Dialect_async) requires greenlet
    for SQLAlchemy's async engine support. By lazy loading it, we allow the
    package to be used in environments without greenlet (like Python Workers)
    where only WorkerConnection is needed.
    """
    if name == "CloudflareD1Dialect_async":
        try:
            from .dialect_async import CloudflareD1Dialect_async

            return CloudflareD1Dialect_async
        except ImportError as e:
            raise ImportError(
                "CloudflareD1Dialect_async requires greenlet. "
                "Install with: pip install sqlalchemy-cloudflare-d1[async]"
            ) from e
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__version__ = "0.3.1"
__all__ = [
    # Dialects
    "CloudflareD1Dialect",
    "CloudflareD1Dialect_async",
    # Sync connection classes
    "Connection",
    "Cursor",
    "WorkerConnection",
    "WorkerCursor",
    "CloudflareD1DBAPI",
    "connect",
    # Async connection classes
    "AsyncConnection",
    "AsyncCursor",
    "connect_async",
    # Worker engine support (for SQLAlchemy Core/ORM in Workers)
    "WorkerDBAPI",
    "SyncWorkerConnection",
    "SyncWorkerCursor",
    "create_engine_from_binding",
    # Exceptions
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
