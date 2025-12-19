"""
DBAPI implementation for Cloudflare D1.

Supports two connection modes:
1. REST API - for external connections using httpx (account_id, api_token, database_id)
2. Worker Binding - for use inside Cloudflare Python Workers (d1_binding)
"""

from typing import Any, Dict, List, Optional, Sequence, Union

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# DBAPI Exception hierarchy
class Error(Exception):
    """Base exception for DBAPI errors."""

    pass


class Warning(Exception):
    """Warning exception."""

    pass


class InterfaceError(Error):
    """Error related to the database interface."""

    pass


class DatabaseError(Error):
    """Error related to the database."""

    pass


class DataError(DatabaseError):
    """Error due to problems with the processed data."""

    pass


class OperationalError(DatabaseError):
    """Error related to the database's operation."""

    pass


class IntegrityError(DatabaseError):
    """Error when the relational integrity is affected."""

    pass


class InternalError(DatabaseError):
    """Error when the database encounters an internal error."""

    pass


class ProgrammingError(DatabaseError):
    """Error due to programming error."""

    pass


class NotSupportedError(DatabaseError):
    """Error when a not supported method or API is used."""

    pass


class Row:
    """Row object that behaves like both a tuple and has named access."""

    def __init__(self, data: Dict[str, Any], description: List[tuple]):
        """Initialize row with data and column descriptions."""
        self._data = data
        self._description = description
        self._keys = (
            [desc[0] for desc in description] if description else list(data.keys())
        )
        self._values = [data.get(key) for key in self._keys]

    def __getitem__(self, key: Union[int, str]) -> Any:
        """Get item by index or column name."""
        if isinstance(key, int):
            return self._values[key]
        elif isinstance(key, str):
            return self._data[key]
        else:
            raise TypeError("Key must be int or str")

    def __iter__(self):
        """Iterate over values."""
        return iter(self._values)

    def __len__(self):
        """Get number of columns."""
        return len(self._values)

    def __bool__(self):
        """Check if row has data."""
        return bool(self._values)

    def __repr__(self):
        """String representation of the row."""
        return f"Row({dict(zip(self._keys, self._values))})"

    def keys(self):
        """Get column names."""
        return self._keys

    def values(self):
        """Get values."""
        return self._values

    def items(self):
        """Get (key, value) pairs."""
        return zip(self._keys, self._values)

    # Add attribute access for compatibility
    def __getattr__(self, name: str) -> Any:
        """Allow attribute access to column values."""
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'Row' object has no attribute '{name}'")


class Cursor:
    """DBAPI-compatible cursor for D1 connections."""

    def __init__(self, connection: "Connection"):
        """Initialize cursor with connection reference."""
        self.connection = connection
        self._result_data = None
        self._description = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False
        self._position = 0
        self._last_result_meta = {}

    def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "Cursor":
        """Execute a database operation."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = self.connection._execute_query(operation, parameters)
            self._result_data = result.get("results", [])
            self._last_result_meta = result.get("meta", {})
            self._rowcount = self._last_result_meta.get(
                "changes", len(self._result_data)
            )

            # Always set description for SELECT-like statements
            # Check if this looks like a SELECT statement or any statement that might return rows
            operation_upper = operation.strip().upper()
            if (
                operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
                or "RETURNING" in operation_upper
            ):
                if self._result_data:
                    # Build description from first row if available
                    first_row = self._result_data[0]
                    self._description = [
                        (name, None, None, None, None, None, None)
                        for name in first_row.keys()
                    ]
                else:
                    # Even if no results, we need to indicate this was a SELECT-like query
                    # We can't know the column names without results, so we'll set an empty description
                    # that still indicates this is a row-returning statement
                    self._description = []
            else:
                # For non-SELECT statements (INSERT, UPDATE, DELETE, etc.), description should be None
                self._description = None

            self._position = 0
            return self

        except Exception as e:
            raise OperationalError(f"Execute failed: {e}")

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence]
    ) -> "Cursor":
        """Execute operation multiple times."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        total_rowcount = 0
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self._rowcount >= 0:
                total_rowcount += self._rowcount

        self._rowcount = total_rowcount
        return self

    def fetchone(self) -> Optional[tuple]:
        """Fetch next row as a tuple."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if not self._result_data or self._position >= len(self._result_data):
            return None

        row_data = self._result_data[self._position]
        self._position += 1

        # Return a tuple of values in the order they appear in the description
        if self._description:
            column_names = [desc[0] for desc in self._description]
            return tuple(row_data.get(name) for name in column_names)
        else:
            # If no description, return values in the order they appear in the dict
            return tuple(row_data.values())

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch multiple rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if size is None:
            size = self._arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self) -> List[tuple]:
        """Fetch all remaining rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._result_data = None
        self._description = None

    @property
    def description(self) -> Optional[List[tuple]]:
        """Get column descriptions.

        Returns:
            List of 7-tuples (name, type_code, display_size, internal_size,
            precision, scale, null_ok) for each column, or None for non-SELECT statements.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Get/set array size for fetchmany."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        """Set array size for fetchmany."""
        self._arraysize = size

    @property
    def lastrowid(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        if hasattr(self, "_last_result_meta"):
            return self._last_result_meta.get("last_row_id")
        return None

    def __iter__(self):
        """Make cursor iterable."""
        return self

    def __next__(self):
        """Get next row for iteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Connection:
    """DBAPI-compatible connection for Cloudflare D1 REST API."""

    def __init__(self, account_id: str, database_id: str, api_token: str, **kwargs):
        """Initialize D1 connection via REST API."""
        if not HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for REST API connections. "
                "Install with: pip install httpx"
            )

        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token

        # Build the D1 REST API URL
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}"

        # HTTP client
        self.client = httpx.Client(
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

        # Connection state
        self._closed = False

    def cursor(self) -> Cursor:
        """Create a cursor."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self.client.close()
            self._closed = True

    def commit(self) -> None:
        """Commit transaction (no-op for D1)."""
        # D1 auto-commits each query
        pass

    def rollback(self) -> None:
        """Rollback transaction (not supported by D1)."""
        # D1 doesn't support explicit transactions via REST API
        pass

    def execute(self, operation: str, parameters: Optional[Sequence] = None):
        """Execute operation directly on connection (convenience method)."""
        cursor = self.cursor()
        cursor.execute(operation, parameters)
        return cursor

    def _execute_query(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Internal method to execute SQL query via D1 REST API."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        # Prepare the request payload
        payload = {"sql": query}

        if parameters:
            # Convert parameters to list for D1 API
            if isinstance(parameters, (tuple, list)):
                payload["params"] = list(parameters)
            elif isinstance(parameters, dict):
                # Handle named parameters by converting to positional
                # This is a simple implementation - more complex queries might need better handling
                payload["params"] = list(parameters.values())
            else:
                payload["params"] = [parameters]

        try:
            # Make the request to D1 REST API
            response = self.client.post(f"{self.base_url}/query", json=payload)
            response.raise_for_status()

            # Parse response
            data = response.json()

            if not data.get("success", False):
                errors = data.get("errors", [])
                if errors:
                    error_msg = errors[0].get("message", "Unknown error")
                    raise OperationalError(f"D1 API error: {error_msg}")
                else:
                    raise OperationalError("D1 API request failed")

            # Extract result data
            result_data = data.get("result", [])
            if result_data:
                query_result = result_data[0]
                return {
                    "results": query_result.get("results", []),
                    "meta": query_result.get("meta", {}),
                    "success": query_result.get("success", True),
                }
            else:
                return {"results": [], "meta": {}, "success": True}

        except httpx.RequestError as e:
            raise OperationalError(f"HTTP request failed: {e}")
        except httpx.HTTPStatusError as e:
            raise OperationalError(
                f"HTTP error {e.response.status_code}: {e.response.text}"
            )

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed


class WorkerConnection:
    """DBAPI-compatible connection for D1 Worker bindings.

    Use this when running inside a Cloudflare Python Worker where you have
    direct access to the D1 binding via env.DB.

    Example:
        from sqlalchemy_cloudflare_d1 import WorkerConnection

        class MyWorker(WorkerEntrypoint):
            async def fetch(self, request):
                conn = WorkerConnection(self.env.DB)
                # Use conn with SQLAlchemy or directly
    """

    def __init__(self, d1_binding: Any):
        """Initialize connection with D1 Worker binding.

        Args:
            d1_binding: The D1 database binding from Worker env (e.g., self.env.DB)
        """
        self._d1 = d1_binding
        self._closed = False

    def cursor(self) -> "WorkerCursor":
        """Create a cursor."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return WorkerCursor(self)

    def close(self) -> None:
        """Close the connection."""
        self._closed = True

    def commit(self) -> None:
        """Commit transaction (no-op for D1)."""
        pass

    def rollback(self) -> None:
        """Rollback transaction (not supported by D1)."""
        pass

    def _execute_query_sync(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Synchronous query execution - not supported in Workers.

        Raises:
            NotSupportedError: Always, as Workers require async execution.
        """
        raise NotSupportedError(
            "Synchronous execution not supported in Workers. "
            "Use async methods or WorkerCursor.execute_async()."
        )

    def _execute_query(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Alias for _execute_query_sync for interface compatibility."""
        return self._execute_query_sync(query, parameters)

    async def _execute_query_async(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Execute SQL query via D1 Worker binding asynchronously."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        try:
            # Prepare the statement
            stmt = self._d1.prepare(query)

            # Bind parameters if provided
            if parameters:
                if isinstance(parameters, (tuple, list)):
                    stmt = stmt.bind(*parameters)
                elif isinstance(parameters, dict):
                    stmt = stmt.bind(*parameters.values())
                else:
                    stmt = stmt.bind(parameters)

            # Execute and get results
            result = await stmt.all()

            # Extract results - D1 binding returns object with results and meta
            # In Pyodide/Workers, these are JsProxy objects that need .to_py() conversion
            results = []
            if hasattr(result, "results") and result.results:
                # Convert JsProxy to Python list if needed
                results_data = result.results
                if hasattr(results_data, "to_py"):
                    results_data = results_data.to_py()

                # Convert each result row to a dict
                for row in results_data:
                    if hasattr(row, "to_py"):
                        row = row.to_py()
                    if isinstance(row, dict):
                        results.append(row)
                    elif hasattr(row, "__dict__"):
                        results.append(dict(row.__dict__))
                    else:
                        # Try to convert to dict
                        results.append(dict(row))

            meta = {}
            if hasattr(result, "meta") and result.meta:
                meta_data = result.meta
                if hasattr(meta_data, "to_py"):
                    meta_data = meta_data.to_py()
                if isinstance(meta_data, dict):
                    meta = meta_data
                elif hasattr(meta_data, "__dict__"):
                    meta = dict(meta_data.__dict__)

            return {
                "results": results,
                "meta": meta,
                "success": getattr(result, "success", True),
            }

        except Exception as e:
            raise OperationalError(f"D1 Worker query failed: {e}")

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed

    @property
    def d1(self) -> Any:
        """Get the underlying D1 binding for direct access."""
        return self._d1


class WorkerCursor:
    """DBAPI-compatible cursor for D1 Worker bindings."""

    def __init__(self, connection: WorkerConnection):
        """Initialize cursor with Worker connection reference."""
        self.connection = connection
        self._result_data: Optional[List[Dict[str, Any]]] = None
        self._description: Optional[List[tuple]] = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False
        self._position = 0
        self._last_result_meta: Dict[str, Any] = {}

    def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "WorkerCursor":
        """Synchronous execute - not supported in Workers."""
        raise NotSupportedError(
            "Synchronous execute not supported in Workers. Use execute_async() instead."
        )

    async def execute_async(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "WorkerCursor":
        """Execute a database operation asynchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = await self.connection._execute_query_async(operation, parameters)
            self._result_data = result.get("results", [])
            self._last_result_meta = result.get("meta", {})
            self._rowcount = self._last_result_meta.get(
                "changes", len(self._result_data) if self._result_data else 0
            )

            # Set description for SELECT-like statements
            operation_upper = operation.strip().upper()
            if (
                operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
                or "RETURNING" in operation_upper
            ):
                if self._result_data:
                    first_row = self._result_data[0]
                    self._description = [
                        (name, None, None, None, None, None, None)
                        for name in first_row.keys()
                    ]
                else:
                    self._description = []
            else:
                self._description = None

            self._position = 0
            return self

        except Exception as e:
            raise OperationalError(f"Execute failed: {e}")

    def fetchone(self) -> Optional[tuple]:
        """Fetch next row as a tuple."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if not self._result_data or self._position >= len(self._result_data):
            return None

        row_data = self._result_data[self._position]
        self._position += 1

        if self._description:
            column_names = [desc[0] for desc in self._description]
            return tuple(row_data.get(name) for name in column_names)
        else:
            return tuple(row_data.values())

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch multiple rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if size is None:
            size = self._arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self) -> List[tuple]:
        """Fetch all remaining rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._result_data = None
        self._description = None

    @property
    def description(self) -> Optional[List[tuple]]:
        """Get column descriptions."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Get array size for fetchmany."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        """Set array size for fetchmany."""
        self._arraysize = size

    @property
    def lastrowid(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        return self._last_result_meta.get("last_row_id")

    def __iter__(self):
        """Make cursor iterable."""
        return self

    def __next__(self):
        """Get next row for iteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


# DBAPI module interface
class CloudflareD1DBAPI:
    """DBAPI module for Cloudflare D1."""

    # DBAPI 2.0 required module attributes
    apilevel = "2.0"
    threadsafety = 1
    paramstyle = "qmark"

    # Exception classes
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

    @staticmethod
    def connect(**kwargs) -> Connection:
        """Create a new database connection."""
        return Connection(**kwargs)


# For backwards compatibility, provide module-level access
apilevel = CloudflareD1DBAPI.apilevel
threadsafety = CloudflareD1DBAPI.threadsafety
paramstyle = CloudflareD1DBAPI.paramstyle


def connect(**kwargs) -> Connection:
    """Create a new database connection."""
    return CloudflareD1DBAPI.connect(**kwargs)


# MARK: - Async Connection Classes


class AsyncConnection:
    """Async DBAPI-compatible connection for Cloudflare D1 REST API.

    Uses httpx.AsyncClient for async HTTP requests.

    Example:
        async with AsyncConnection(account_id, database_id, api_token) as conn:
            cursor = await conn.cursor()
            await cursor.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
    """

    def __init__(self, account_id: str, database_id: str, api_token: str, **kwargs):
        """Initialize async D1 connection via REST API."""
        if not HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for REST API connections. "
                "Install with: pip install httpx"
            )

        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token

        # Build the D1 REST API URL
        self.base_url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/d1/database/{database_id}"
        )

        # Async HTTP client
        self.client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

        # Connection state
        self._closed = False

    async def __aenter__(self) -> "AsyncConnection":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def cursor(self) -> "AsyncCursor":
        """Create an async cursor."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return AsyncCursor(self)

    async def close(self) -> None:
        """Close the async connection."""
        if not self._closed:
            await self.client.aclose()
            self._closed = True

    async def commit(self) -> None:
        """Commit transaction (no-op for D1)."""
        # D1 auto-commits each query
        pass

    async def rollback(self) -> None:
        """Rollback transaction (not supported by D1)."""
        # D1 doesn't support explicit transactions via REST API
        pass

    async def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "AsyncCursor":
        """Execute operation directly on connection (convenience method)."""
        cursor = await self.cursor()
        await cursor.execute(operation, parameters)
        return cursor

    async def _execute_query(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Execute SQL query via D1 REST API asynchronously."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        # Prepare the request payload
        payload: Dict[str, Any] = {"sql": query}

        if parameters:
            # Convert parameters to list for D1 API
            if isinstance(parameters, (tuple, list)):
                payload["params"] = list(parameters)
            elif isinstance(parameters, dict):
                payload["params"] = list(parameters.values())
            else:
                payload["params"] = [parameters]

        try:
            # Make the async request to D1 REST API
            response = await self.client.post(f"{self.base_url}/query", json=payload)
            response.raise_for_status()

            # Parse response
            data = response.json()

            if not data.get("success", False):
                errors = data.get("errors", [])
                if errors:
                    error_msg = errors[0].get("message", "Unknown error")
                    raise OperationalError(f"D1 API error: {error_msg}")
                else:
                    raise OperationalError("D1 API request failed")

            # Extract result data
            result_data = data.get("result", [])
            if result_data:
                query_result = result_data[0]
                return {
                    "results": query_result.get("results", []),
                    "meta": query_result.get("meta", {}),
                    "success": query_result.get("success", True),
                }
            else:
                return {"results": [], "meta": {}, "success": True}

        except httpx.RequestError as e:
            raise OperationalError(f"HTTP request failed: {e}")
        except httpx.HTTPStatusError as e:
            raise OperationalError(
                f"HTTP error {e.response.status_code}: {e.response.text}"
            )

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed


class AsyncCursor:
    """Async DBAPI-compatible cursor for D1 connections."""

    def __init__(self, connection: AsyncConnection):
        """Initialize async cursor with connection reference."""
        self.connection = connection
        self._result_data: Optional[List[Dict[str, Any]]] = None
        self._description: Optional[List[tuple]] = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False
        self._position = 0
        self._last_result_meta: Dict[str, Any] = {}

    async def __aenter__(self) -> "AsyncCursor":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "AsyncCursor":
        """Execute a database operation asynchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = await self.connection._execute_query(operation, parameters)
            self._result_data = result.get("results", [])
            self._last_result_meta = result.get("meta", {})
            self._rowcount = self._last_result_meta.get(
                "changes", len(self._result_data) if self._result_data else 0
            )

            # Set description for SELECT-like statements
            operation_upper = operation.strip().upper()
            if (
                operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
                or "RETURNING" in operation_upper
            ):
                if self._result_data:
                    first_row = self._result_data[0]
                    self._description = [
                        (name, None, None, None, None, None, None)
                        for name in first_row.keys()
                    ]
                else:
                    self._description = []
            else:
                self._description = None

            self._position = 0
            return self

        except Exception as e:
            if isinstance(e, (OperationalError, ProgrammingError)):
                raise
            raise OperationalError(f"Execute failed: {e}")

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence]
    ) -> "AsyncCursor":
        """Execute operation multiple times asynchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        total_rowcount = 0
        for parameters in seq_of_parameters:
            await self.execute(operation, parameters)
            if self._rowcount >= 0:
                total_rowcount += self._rowcount

        self._rowcount = total_rowcount
        return self

    async def fetchone(self) -> Optional[tuple]:
        """Fetch next row as a tuple."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if not self._result_data or self._position >= len(self._result_data):
            return None

        row_data = self._result_data[self._position]
        self._position += 1

        if self._description:
            column_names = [desc[0] for desc in self._description]
            return tuple(row_data.get(name) for name in column_names)
        else:
            return tuple(row_data.values())

    async def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch multiple rows asynchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if size is None:
            size = self._arraysize

        rows = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    async def fetchall(self) -> List[tuple]:
        """Fetch all remaining rows asynchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        rows = []
        while True:
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    async def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._result_data = None
        self._description = None

    @property
    def description(self) -> Optional[List[tuple]]:
        """Get column descriptions."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Get array size for fetchmany."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        """Set array size for fetchmany."""
        self._arraysize = size

    @property
    def lastrowid(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        return self._last_result_meta.get("last_row_id")


async def connect_async(**kwargs) -> AsyncConnection:
    """Create a new async database connection."""
    return AsyncConnection(**kwargs)


# MARK: - Worker Engine Support


class WorkerDBAPI:
    """DBAPI module for D1 Worker bindings.

    This provides a DBAPI interface that SQLAlchemy can use with create_engine().
    It wraps the D1 binding and handles the sync/async bridging needed for Workers.
    """

    # DBAPI 2.0 required module attributes
    apilevel = "2.0"
    threadsafety = 1
    paramstyle = "qmark"

    # Exception classes
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

    def __init__(self, d1_binding: Any):
        """Store the D1 binding for later use in connect()."""
        self._d1_binding = d1_binding

    def connect(self, **kwargs) -> "SyncWorkerConnection":
        """Create a new database connection using the stored D1 binding."""
        return SyncWorkerConnection(self._d1_binding)


class SyncWorkerConnection:
    """Synchronous DBAPI-compatible connection for D1 Worker bindings.

    This wraps WorkerConnection to provide synchronous methods that SQLAlchemy
    can use. It uses a run loop cache to run async operations synchronously.

    Note: This works in Pyodide/Workers because there's a single-threaded
    event loop that can run tasks synchronously via run_sync().
    """

    def __init__(self, d1_binding: Any):
        """Initialize connection with D1 Worker binding."""
        self._d1 = d1_binding
        self._closed = False
        self._pending_results: Optional[Dict[str, Any]] = None

    def cursor(self) -> "SyncWorkerCursor":
        """Create a cursor."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return SyncWorkerCursor(self)

    def close(self) -> None:
        """Close the connection."""
        self._closed = True

    def commit(self) -> None:
        """Commit transaction (no-op for D1)."""
        pass

    def rollback(self) -> None:
        """Rollback transaction (not supported by D1)."""
        pass

    def _execute_query(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> Dict[str, Any]:
        """Execute SQL query via D1 Worker binding.

        This runs the async D1 operations synchronously using Pyodide's
        run_sync() which is available in the Workers environment.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")

        try:
            # Import pyodide to use run_sync for async operations
            # This is available in Cloudflare Python Workers
            from pyodide.ffi import run_sync

            async def _run():
                # Prepare the statement
                stmt = self._d1.prepare(query)

                # Bind parameters if provided
                if parameters:
                    if isinstance(parameters, (tuple, list)):
                        stmt = stmt.bind(*parameters)
                    elif isinstance(parameters, dict):
                        stmt = stmt.bind(*parameters.values())
                    else:
                        stmt = stmt.bind(parameters)

                # Execute and get results
                return await stmt.all()

            result = run_sync(_run())

            def convert_js_null(value):
                """Convert JsNull/JsUndefined to Python None."""
                if value is None:
                    return None
                # Check for JsNull/JsUndefined by type name (avoid import issues)
                type_name = type(value).__name__
                if type_name in ("JsNull", "JsUndefined", "JsProxy"):
                    if hasattr(value, "to_py"):
                        return value.to_py()
                    # JsNull.to_py() returns None, but if it's still JsNull, return None
                    return None
                return value

            def convert_row_values(row_dict):
                """Convert all values in a row dict, handling JsNull."""
                return {k: convert_js_null(v) for k, v in row_dict.items()}

            # Extract results - D1 binding returns object with results and meta
            results = []
            if hasattr(result, "results") and result.results:
                results_data = result.results
                if hasattr(results_data, "to_py"):
                    results_data = results_data.to_py()

                for row in results_data:
                    if hasattr(row, "to_py"):
                        row = row.to_py()
                    if isinstance(row, dict):
                        results.append(convert_row_values(row))
                    elif hasattr(row, "__dict__"):
                        results.append(convert_row_values(dict(row.__dict__)))
                    else:
                        results.append(convert_row_values(dict(row)))

            meta = {}
            if hasattr(result, "meta") and result.meta:
                meta_data = result.meta
                if hasattr(meta_data, "to_py"):
                    meta_data = meta_data.to_py()
                if isinstance(meta_data, dict):
                    meta = meta_data
                elif hasattr(meta_data, "__dict__"):
                    meta = dict(meta_data.__dict__)

            return {
                "results": results,
                "meta": meta,
                "success": getattr(result, "success", True),
            }

        except ImportError:
            raise NotSupportedError(
                "Synchronous execution requires Pyodide's run_sync(). "
                "This is only available inside Cloudflare Python Workers."
            )
        except Exception as e:
            raise OperationalError(f"D1 Worker query failed: {e}")

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed


class SyncWorkerCursor:
    """Synchronous DBAPI-compatible cursor for D1 Worker bindings.

    This wraps the async cursor to provide synchronous methods for SQLAlchemy.
    """

    def __init__(self, connection: SyncWorkerConnection):
        """Initialize cursor with Worker connection reference."""
        self.connection = connection
        self._result_data: Optional[List[Dict[str, Any]]] = None
        self._description: Optional[List[tuple]] = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False
        self._position = 0
        self._last_result_meta: Dict[str, Any] = {}

    def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "SyncWorkerCursor":
        """Execute a database operation synchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = self.connection._execute_query(operation, parameters)
            self._result_data = result.get("results", [])
            self._last_result_meta = result.get("meta", {})
            self._rowcount = self._last_result_meta.get(
                "changes", len(self._result_data) if self._result_data else 0
            )

            # Set description for SELECT-like statements
            operation_upper = operation.strip().upper()
            if (
                operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
                or "RETURNING" in operation_upper
            ):
                if self._result_data:
                    first_row = self._result_data[0]
                    self._description = [
                        (name, None, None, None, None, None, None)
                        for name in first_row.keys()
                    ]
                else:
                    self._description = []
            else:
                self._description = None

            self._position = 0
            return self

        except Exception as e:
            if isinstance(e, (OperationalError, ProgrammingError, NotSupportedError)):
                raise
            raise OperationalError(f"Execute failed: {e}")

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence]
    ) -> "SyncWorkerCursor":
        """Execute operation multiple times."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        total_rowcount = 0
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self._rowcount >= 0:
                total_rowcount += self._rowcount

        self._rowcount = total_rowcount
        return self

    def fetchone(self) -> Optional[tuple]:
        """Fetch next row as a tuple."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if not self._result_data or self._position >= len(self._result_data):
            return None

        row_data = self._result_data[self._position]
        self._position += 1

        if self._description:
            column_names = [desc[0] for desc in self._description]
            return tuple(row_data.get(name) for name in column_names)
        else:
            return tuple(row_data.values())

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch multiple rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        if size is None:
            size = self._arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self) -> List[tuple]:
        """Fetch all remaining rows."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._result_data = None
        self._description = None

    @property
    def description(self) -> Optional[List[tuple]]:
        """Get column descriptions."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Get array size for fetchmany."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        """Set array size for fetchmany."""
        self._arraysize = size

    @property
    def lastrowid(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        return self._last_result_meta.get("last_row_id")

    def __iter__(self):
        """Make cursor iterable."""
        return self

    def __next__(self):
        """Get next row for iteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


def create_engine_from_binding(d1_binding: Any, **kwargs) -> Any:
    """Create a SQLAlchemy engine from a D1 Worker binding.

    This allows using SQLAlchemy Core and ORM patterns inside Cloudflare
    Python Workers without raw SQL.

    Example:
        from sqlalchemy import MetaData, Table, select
        from sqlalchemy_cloudflare_d1 import create_engine_from_binding

        class MyWorker(WorkerEntrypoint):
            async def fetch(self, request):
                engine = create_engine_from_binding(self.env.DB)

                # Reflect existing table
                metadata = MetaData()
                users = Table('users', metadata, autoload_with=engine)

                # Query using SQLAlchemy Core
                with engine.connect() as conn:
                    result = conn.execute(select(users).limit(10))
                    rows = result.fetchall()

    Args:
        d1_binding: The D1 database binding from Worker env (e.g., self.env.DB)
        **kwargs: Additional arguments passed to create_engine()
            (echo, pool_size, etc.)

    Returns:
        SQLAlchemy Engine configured to use the D1 binding
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    # Create a custom DBAPI module that wraps the D1 binding
    dbapi = WorkerDBAPI(d1_binding)

    # Create engine with our custom DBAPI
    # Use StaticPool to reuse the same connection (D1 is stateless anyway)
    # Use the cloudflare_d1 dialect for proper SQL compilation
    engine = create_engine(
        "cloudflare_d1://",
        module=dbapi,
        creator=dbapi.connect,
        poolclass=StaticPool,
        **kwargs,
    )

    return engine
