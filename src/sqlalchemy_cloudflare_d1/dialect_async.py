"""
Async SQLAlchemy dialect for Cloudflare D1.

Provides async support for SQLAlchemy's create_async_engine() using
the AsyncAdapt pattern to bridge async DBAPI to SQLAlchemy's engine layer.

Usage:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "cloudflare_d1+async://account_id:api_token@database_id"
    )

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM users"))
        rows = result.fetchall()
"""

from collections import deque
from typing import Any, Optional, Sequence

from sqlalchemy.engine import AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import await_only

from .connection import (
    AsyncConnection,
    Error,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .dialect import CloudflareD1Dialect


class AsyncAdapt_d1_cursor:
    """Async-adapted cursor for D1.

    This wraps the AsyncCursor and provides sync-looking methods.
    Key insight from aiosqlite: fetch operations read from a local deque,
    not from the async cursor. The data is eagerly fetched during execute().
    """

    __slots__ = (
        "_adapt_connection",
        "_connection",
        "description",
        "await_",
        "_rows",
        "arraysize",
        "rowcount",
        "lastrowid",
    )

    server_side = False

    def __init__(self, adapt_connection: "AsyncAdapt_d1_connection"):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self.arraysize = 1
        self.rowcount = -1
        self.lastrowid = None
        self.description = None
        self._rows = deque()

    def close(self):
        """Close the cursor - just clear local rows."""
        self._rows.clear()

    def execute(self, operation: str, parameters: Optional[Sequence] = None):
        """Execute a database operation.

        Uses await_ to run async operations. Eagerly fetches all results
        into _rows deque so subsequent fetch calls are synchronous.
        """
        try:
            # Get cursor from async connection
            _cursor = self.await_(self._connection.cursor())

            # Execute the query
            self.await_(_cursor.execute(operation, parameters))

            # Determine if this is a row-returning statement
            operation_upper = operation.strip().upper()
            is_select = (
                operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
                or "RETURNING" in operation_upper
            )

            if is_select:
                # For SELECT statements, set description (may be empty list for no-column results)
                # D1 returns [] for empty results since it can't know column names
                self.description = _cursor.description if _cursor.description else []
                self.lastrowid = None
                self.rowcount = -1
                # Eagerly fetch all results into local deque
                rows = self.await_(_cursor.fetchall())
                self._rows = deque(rows if rows else [])
            else:
                # For non-SELECT statements (INSERT, UPDATE, DELETE)
                self.description = None
                self.lastrowid = _cursor.lastrowid
                self.rowcount = _cursor.rowcount
                self._rows = deque()

            # Close the async cursor - we have all the data
            self.await_(_cursor.close())
            return self

        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence]):
        """Execute operation multiple times."""
        try:
            _cursor = self.await_(self._connection.cursor())
            self.await_(_cursor.executemany(operation, seq_of_parameters))
            self.description = None
            self.lastrowid = _cursor.lastrowid
            self.rowcount = _cursor.rowcount
            self.await_(_cursor.close())
            return self
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def setinputsizes(self, *inputsizes):
        """No-op for D1."""
        pass

    def setoutputsize(self, size, column=None):
        """No-op for D1."""
        pass

    def __iter__(self):
        """Iterate over results from local deque."""
        while self._rows:
            yield self._rows.popleft()

    def fetchone(self):
        """Fetch next row from local deque."""
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size=None):
        """Fetch multiple rows from local deque."""
        if size is None:
            size = self.arraysize
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self):
        """Fetch all remaining rows from local deque."""
        retval = list(self._rows)
        self._rows.clear()
        return retval

    async def _async_soft_close(self):
        """Async soft close for SQLAlchemy async result compatibility.

        This is called by SQLAlchemy after execute() completes but BEFORE
        fetchone()/fetchall() are called on buffered results. We MUST NOT
        clear the rows here - they are needed for subsequent fetch calls.

        For buffered cursors (server_side=False), this is essentially a no-op.
        The data stays in _rows until explicitly fetched.
        """
        # Do NOT clear _rows here - fetchone/fetchall need to read from it
        pass


class AsyncAdapt_d1_connection(AdaptedConnection):
    """Async-adapted connection for D1.

    Inherits from AdaptedConnection to provide proper SQLAlchemy interface.
    Uses await_only for async operations within greenlet context.
    """

    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi: "AsyncAdapt_d1_dbapi", connection: AsyncConnection):
        self.dbapi = dbapi
        self._connection = connection

    def cursor(self):
        """Create a cursor."""
        return AsyncAdapt_d1_cursor(self)

    def execute(self, *args, **kw):
        """Execute directly on connection."""
        cursor = self.cursor()
        cursor.execute(*args, **kw)
        return cursor

    def rollback(self):
        """Rollback transaction (no-op for D1)."""
        try:
            self.await_(self._connection.rollback())
        except Exception as error:
            self._handle_exception(error)

    def commit(self):
        """Commit transaction (no-op for D1)."""
        try:
            self.await_(self._connection.commit())
        except Exception as error:
            self._handle_exception(error)

    def close(self):
        """Close the connection."""
        try:
            self.await_(self._connection.close())
        except Exception as error:
            self._handle_exception(error)

    @property
    def closed(self):
        """Check if connection is closed."""
        return self._connection.closed

    def _handle_exception(self, error):
        """Handle and re-raise exceptions appropriately."""
        if isinstance(
            error, (Error, InterfaceError, OperationalError, ProgrammingError)
        ):
            raise error
        raise OperationalError(f"D1 operation failed: {error}") from error


class AsyncAdapt_d1_dbapi:
    """Async-adapted DBAPI module for D1.

    This provides the module-level DBAPI interface that SQLAlchemy expects.
    The connect() method uses await_only to create the connection within
    the greenlet context.
    """

    # PEP 249 DBAPI 2.0 required module attributes
    apilevel = "2.0"
    threadsafety = 1  # Threads may share the module but not connections
    paramstyle = "qmark"

    # Import exception classes at class level for DBAPI compliance
    from .connection import (
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

    # Make them class attributes
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def connect(self, **kwargs) -> AsyncAdapt_d1_connection:
        """Create an async-adapted connection.

        Creates the AsyncConnection and wraps it. The await_only call
        here establishes the greenlet context for subsequent operations.
        """
        # Create the async connection object (not awaited yet)
        async_conn = AsyncConnection(**kwargs)
        # Return the adapted connection - async_conn is ready for use
        # (AsyncConnection doesn't need to be awaited to be created,
        # only its methods need awaiting)
        return AsyncAdapt_d1_connection(self, async_conn)

    @staticmethod
    def Binary(data: bytes) -> bytes:
        """Return binary data for binding to BLOB columns.

        Args:
            data: Raw bytes to bind

        Returns:
            The same bytes (D1 REST API handles base64 encoding internally)
        """
        return data


# Module-level singleton for the DBAPI
_dbapi_singleton = None


def _get_dbapi():
    """Get or create the DBAPI singleton."""
    global _dbapi_singleton
    if _dbapi_singleton is None:
        _dbapi_singleton = AsyncAdapt_d1_dbapi()
    return _dbapi_singleton


class CloudflareD1Dialect_async(CloudflareD1Dialect):
    """Async dialect for Cloudflare D1.

    This dialect is used with SQLAlchemy's create_async_engine() and
    provides full async support for D1 operations.

    Usage:
        engine = create_async_engine(
            "cloudflare_d1+async://account_id:api_token@database_id"
        )
    """

    driver = "async"
    is_async = True
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """Import the async DBAPI module."""
        return _get_dbapi()

    @classmethod
    def get_pool_class(cls, url):
        """Return the async pool class."""
        return AsyncAdaptedQueuePool

    def get_driver_connection(self, connection):
        """Get the underlying driver connection."""
        return connection._connection

    def is_disconnect(self, e, connection, cursor):
        """Check if exception indicates a disconnected state."""
        if isinstance(e, OperationalError):
            msg = str(e).lower()
            if "connection" in msg and ("closed" in msg or "no active" in msg):
                return True
        return super().is_disconnect(e, connection, cursor)


# Convenience alias
dialect = CloudflareD1Dialect_async
