"""
SQLAlchemy dialect for Cloudflare D1 Serverless SQLite Database.

This dialect provides connectivity to Cloudflare D1 databases via:
1. REST API - for external connections (account_id, api_token, database_id)
2. Worker Binding - for use inside Cloudflare Python Workers (d1_binding)
"""

from .dialect import CloudflareD1Dialect
from .connection import (
    Connection,
    WorkerConnection,
    WorkerCursor,
    CloudflareD1DBAPI,
    connect,
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

__version__ = "0.2.0"
__all__ = [
    "CloudflareD1Dialect",
    "Connection",
    "WorkerConnection",
    "WorkerCursor",
    "CloudflareD1DBAPI",
    "connect",
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
