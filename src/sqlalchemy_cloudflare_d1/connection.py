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


# MARK: - Helper Functions


def _prepare_parameters(parameters: Optional[Sequence]) -> Optional[List]:
    """Convert parameters to list format for D1 API.

    Args:
        parameters: Query parameters (tuple, list, dict, or single value)

    Returns:
        List of parameters or None if no parameters
    """
    if not parameters:
        return None

    if isinstance(parameters, (tuple, list)):
        return list(parameters)
    elif isinstance(parameters, dict):
        return list(parameters.values())
    else:
        return [parameters]


def _build_description(
    operation: str, columns: List[str], result_data: Optional[List[Dict[str, Any]]]
) -> Optional[List[tuple]]:
    """Build cursor description from query result.

    Args:
        operation: The SQL operation that was executed
        columns: Column names from the query result
        result_data: The result data rows

    Returns:
        List of 7-tuples for SELECT-like statements, None otherwise
    """
    operation_upper = operation.strip().upper()
    is_select_like = (
        operation_upper.startswith(("SELECT", "PRAGMA", "WITH"))
        or "RETURNING" in operation_upper
    )

    if not is_select_like:
        return None

    # Build description from columns
    if columns:
        return [(name, None, None, None, None, None, None) for name in columns]
    elif result_data:
        # Fallback to first row keys if columns not available
        first_row = result_data[0]
        return [(name, None, None, None, None, None, None) for name in first_row.keys()]
    else:
        # Empty result with no column info
        return []


def _convert_js_null(value: Any) -> Any:
    """Convert JsNull/JsUndefined to Python None."""
    if value is None:
        return None
    type_name = type(value).__name__
    if type_name in ("JsNull", "JsUndefined", "JsProxy"):
        if hasattr(value, "to_py"):
            return value.to_py()
        return None
    return value


def _get_attr_or_key(obj: Any, key: str, default: Any = None) -> Any:
    """Get value from object by attribute or key access."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _parse_all_result(all_result: Any) -> Dict[str, Any]:
    """Parse the result from D1's stmt.all() into a standard dict format.

    all() returns {results: [{col: val, ...}, ...], meta: {...}, success: bool}
    This may be a JsProxy or a Python dict depending on whether to_py() was called.

    Args:
        all_result: The result from stmt.all() (JsProxy or dict after to_py())

    Returns:
        Standardized dict with results, columns, meta, and success keys
    """
    # Convert top-level JsProxy to Python
    if hasattr(all_result, "to_py"):
        all_result = all_result.to_py()

    columns: List[str] = []
    results: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {}

    # Extract results array
    raw_results = _get_attr_or_key(all_result, "results")
    if hasattr(raw_results, "to_py"):
        raw_results = raw_results.to_py()

    if raw_results and len(raw_results) > 0:
        # Extract column names from first result object's keys
        first = raw_results[0]
        if hasattr(first, "to_py"):
            first = first.to_py()
        if isinstance(first, dict):
            columns = list(first.keys())
        elif hasattr(first, "keys"):
            columns = list(first.keys())

        for row_obj in raw_results:
            if hasattr(row_obj, "to_py"):
                row_obj = row_obj.to_py()
            if isinstance(row_obj, dict):
                row_dict = {col: _convert_js_null(val) for col, val in row_obj.items()}
                results.append(row_dict)
            else:
                # JsProxy object with attribute access
                row_dict = {
                    col: _convert_js_null(getattr(row_obj, col, None))
                    for col in columns
                }
                results.append(row_dict)

    # Extract meta
    meta_obj = _get_attr_or_key(all_result, "meta")
    if meta_obj is not None:
        if hasattr(meta_obj, "to_py"):
            meta = meta_obj.to_py()
        elif isinstance(meta_obj, dict):
            meta = meta_obj

    return {
        "results": results,
        "columns": columns,
        "meta": meta if isinstance(meta, dict) else {},
        "success": True,
    }


# MARK: - Row Class


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


# MARK: - Base Cursor Mixin


class BaseCursorMixin:
    """Mixin providing common cursor functionality.

    This mixin provides shared implementations for fetch methods, properties,
    and iteration that are identical across all cursor types.
    """

    # These attributes must be defined by subclasses
    _result_data: Optional[List[Dict[str, Any]]]
    _description: Optional[List[tuple]]
    _rowcount: int
    _arraysize: int
    _closed: bool
    _position: int
    _last_result_meta: Dict[str, Any]

    def _init_cursor_state(self) -> None:
        """Initialize common cursor state. Call from subclass __init__."""
        self._result_data = None
        self._description = None
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False
        self._position = 0
        self._last_result_meta = {}

    def _process_result(self, result: Dict[str, Any], operation: str) -> None:
        """Process query result and update cursor state.

        Args:
            result: The result dict from _execute_query
            operation: The SQL operation that was executed
        """
        self._result_data = result.get("results", [])
        self._last_result_meta = result.get("meta", {})
        self._rowcount = self._last_result_meta.get(
            "changes", len(self._result_data) if self._result_data else 0
        )
        self._description = _build_description(
            operation, result.get("columns", []), self._result_data
        )
        self._position = 0

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


# MARK: - Sync REST API Cursor


class Cursor(BaseCursorMixin):
    """DBAPI-compatible cursor for D1 connections."""

    def __init__(self, connection: "Connection"):
        """Initialize cursor with connection reference."""
        self.connection = connection
        self._init_cursor_state()

    def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "Cursor":
        """Execute a database operation."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = self.connection._execute_query(operation, parameters)
            self._process_result(result, operation)
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


# MARK: - Sync REST API Connection


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
        payload: Dict[str, Any] = {"sql": query}
        params = _prepare_parameters(parameters)
        if params:
            payload["params"] = params

        try:
            # MARK: - Make request to D1 REST API /raw endpoint
            # Use /raw endpoint to get column names even on empty results
            response = self.client.post(f"{self.base_url}/raw", json=payload)
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

            # MARK: - Extract result data from /raw response format
            # /raw returns: {"result": [{"results": {"columns": [...], "rows": [...]}, "meta": {...}}]}
            result_data = data.get("result", [])
            if result_data:
                query_result = result_data[0]
                raw_results = query_result.get("results", {})
                columns = raw_results.get("columns", [])
                rows = raw_results.get("rows", [])

                # Convert rows from arrays to dicts using column names
                results = []
                for row in rows:
                    results.append(dict(zip(columns, row)))

                return {
                    "results": results,
                    "columns": columns,
                    "meta": query_result.get("meta", {}),
                    "success": query_result.get("success", True),
                }
            else:
                return {"results": [], "columns": [], "meta": {}, "success": True}

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

            # MARK: - Execute using all() for reliable structured results
            # all() returns {results: [{col: val, ...}, ...], meta: {...}}
            # This avoids the raw() bug where single-row results lose the
            # column names header row, causing data to end up in description.
            all_result = await stmt.all()

            parsed = _parse_all_result(all_result)

            # MARK: - Fall back to raw() for column names on empty results
            # all() doesn't return column info when results are empty.
            # raw({columnNames: true}) works correctly for 0-row results.
            # Only do this for SELECT queries to avoid re-executing mutations.
            if (
                not parsed["columns"]
                and not parsed["results"]
                and query.strip().upper().startswith("SELECT")
            ):
                try:
                    stmt2 = self._d1.prepare(query)
                    if parameters:
                        if isinstance(parameters, (tuple, list)):
                            stmt2 = stmt2.bind(*parameters)
                        elif isinstance(parameters, dict):
                            stmt2 = stmt2.bind(*parameters.values())
                        else:
                            stmt2 = stmt2.bind(parameters)
                    raw_result = await stmt2.raw({"columnNames": True})
                    if hasattr(raw_result, "to_py"):
                        raw_result = raw_result.to_py()
                    if raw_result and len(raw_result) > 0:
                        first_row = raw_result[0]
                        if hasattr(first_row, "to_py"):
                            first_row = first_row.to_py()
                        parsed["columns"] = list(first_row) if first_row else []
                except Exception:
                    pass  # Column names are best-effort for empty results

            return parsed

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


class WorkerCursor(BaseCursorMixin):
    """DBAPI-compatible cursor for D1 Worker bindings."""

    def __init__(self, connection: WorkerConnection):
        """Initialize cursor with Worker connection reference."""
        self.connection = connection
        self._init_cursor_state()

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
            self._process_result(result, operation)
            return self
        except Exception as e:
            raise OperationalError(f"Execute failed: {e}")


# MARK: - DBAPI Module Interface
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

    @staticmethod
    def Binary(data: bytes) -> bytes:
        """Return binary data for binding to BLOB columns.

        Args:
            data: Raw bytes to bind

        Returns:
            The same bytes (D1 REST API handles base64 encoding internally)
        """
        return data


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
        params = _prepare_parameters(parameters)
        if params:
            payload["params"] = params

        try:
            # MARK: - Make async request to D1 REST API /raw endpoint
            # Use /raw endpoint to get column names even on empty results
            response = await self.client.post(f"{self.base_url}/raw", json=payload)
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

            # MARK: - Extract result data from /raw response format
            # /raw returns: {"result": [{"results": {"columns": [...], "rows": [...]}, "meta": {...}}]}
            result_data = data.get("result", [])
            if result_data:
                query_result = result_data[0]
                raw_results = query_result.get("results", {})
                columns = raw_results.get("columns", [])
                rows = raw_results.get("rows", [])

                # Convert rows from arrays to dicts using column names
                results = []
                for row in rows:
                    results.append(dict(zip(columns, row)))

                return {
                    "results": results,
                    "columns": columns,
                    "meta": query_result.get("meta", {}),
                    "success": query_result.get("success", True),
                }
            else:
                return {"results": [], "columns": [], "meta": {}, "success": True}

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


class AsyncCursor(BaseCursorMixin):
    """Async DBAPI-compatible cursor for D1 connections.

    Note: This extends BaseCursorMixin but overrides fetch methods with async versions.
    The sync fetchone/fetchmany/fetchall from BaseCursorMixin are intentionally hidden
    by the async versions defined here.
    """

    def __init__(self, connection: AsyncConnection):
        """Initialize async cursor with connection reference."""
        self.connection = connection
        self._init_cursor_state()

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
            self._process_result(result, operation)
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

    async def fetchone(self) -> Optional[tuple]:  # type: ignore[override]
        """Fetch next row as a tuple (async version)."""
        # Note: Uses sync implementation from mixin, wrapped as async for API compatibility
        return BaseCursorMixin.fetchone(self)

    async def fetchmany(self, size: Optional[int] = None) -> List[tuple]:  # type: ignore[override]
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

    async def fetchall(self) -> List[tuple]:  # type: ignore[override]
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

    async def close(self) -> None:  # type: ignore[override]
        """Close the cursor (async version)."""
        BaseCursorMixin.close(self)


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

    @staticmethod
    def Binary(data: bytes) -> bytes:
        """Return binary data for binding to BLOB columns.

        Args:
            data: Raw bytes to bind

        Returns:
            The same bytes (Worker binding handles encoding internally)
        """
        return data


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
                from js import JSON

                # Get a proper JS null value (not undefined)
                JS_NULL = JSON.parse("null")

                def convert_param(val):
                    """Convert parameter for D1 binding, handling None -> null."""
                    if val is None:
                        return JS_NULL
                    return val

                # Prepare the statement
                stmt = self._d1.prepare(query)

                # Bind parameters if provided
                # Note: Python None must be converted to JS null via to_js()
                if parameters:
                    if isinstance(parameters, (tuple, list)):
                        converted = [convert_param(p) for p in parameters]
                        stmt = stmt.bind(*converted)
                    elif isinstance(parameters, dict):
                        converted = [convert_param(v) for v in parameters.values()]
                        stmt = stmt.bind(*converted)
                    else:
                        stmt = stmt.bind(convert_param(parameters))

                # MARK: - Execute using all() for reliable structured results
                # all() returns {results: [{col: val, ...}, ...], meta: {...}}
                # This avoids the raw() bug where single-row results lose the
                # column names header row, causing data to end up in description.
                all_result = await stmt.all()

                # For empty results, fall back to raw() for column names
                # all() doesn't return column info when results are empty.
                # raw({columnNames: true}) works correctly for 0-row results.
                # Only do this for SELECT queries to avoid re-executing mutations.
                fallback_columns = None
                parsed = _parse_all_result(all_result)
                if (
                    not parsed["columns"]
                    and not parsed["results"]
                    and query.strip().upper().startswith("SELECT")
                ):
                    try:
                        stmt2 = self._d1.prepare(query)
                        if parameters:
                            if isinstance(parameters, (tuple, list)):
                                converted2 = [convert_param(p) for p in parameters]
                                stmt2 = stmt2.bind(*converted2)
                            elif isinstance(parameters, dict):
                                converted2 = [
                                    convert_param(v) for v in parameters.values()
                                ]
                                stmt2 = stmt2.bind(*converted2)
                            else:
                                stmt2 = stmt2.bind(convert_param(parameters))
                        raw_result = await stmt2.raw({"columnNames": True})
                        if hasattr(raw_result, "to_py"):
                            raw_result = raw_result.to_py()
                        if raw_result and len(raw_result) > 0:
                            first_row = raw_result[0]
                            if hasattr(first_row, "to_py"):
                                first_row = first_row.to_py()
                            fallback_columns = list(first_row) if first_row else []
                    except Exception:
                        pass
                if fallback_columns:
                    parsed["columns"] = fallback_columns
                return parsed

            return run_sync(_run())

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


class SyncWorkerCursor(BaseCursorMixin):
    """Synchronous DBAPI-compatible cursor for D1 Worker bindings.

    This wraps the async cursor to provide synchronous methods for SQLAlchemy.
    """

    def __init__(self, connection: SyncWorkerConnection):
        """Initialize cursor with Worker connection reference."""
        self.connection = connection
        self._init_cursor_state()

    def execute(
        self, operation: str, parameters: Optional[Sequence] = None
    ) -> "SyncWorkerCursor":
        """Execute a database operation synchronously."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        try:
            result = self.connection._execute_query(operation, parameters)
            self._process_result(result, operation)
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


# MARK: - Engine Factory


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
