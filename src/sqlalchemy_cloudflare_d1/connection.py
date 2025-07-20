"""
Connection implementation for Cloudflare D1 REST API.
"""

from typing import Any, Dict, List, Optional, Sequence
import httpx


class CloudflareD1Connection:
    """Connection implementation for Cloudflare D1 using REST API."""

    def __init__(self, account_id: str, database_id: str, api_token: str, **kwargs):
        """Initialize D1 connection.

        Args:
            account_id: Cloudflare account ID
            database_id: D1 database ID (UUID)
            api_token: Cloudflare API token with D1 permissions
        """
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
        self._in_transaction = False

    def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self.client.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed

    def execute(
        self, query: str, parameters: Optional[Sequence] = None
    ) -> "CloudflareD1Result":
        """Execute a SQL query.

        Args:
            query: SQL query string
            parameters: Query parameters for prepared statement

        Returns:
            CloudflareD1Result: Query result wrapper
        """
        if self._closed:
            raise RuntimeError("Connection is closed")

        # Prepare the request payload
        payload = {"sql": query}

        if parameters:
            payload["params"] = list(parameters)

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
                    raise RuntimeError(f"D1 API error: {error_msg}")
                else:
                    raise RuntimeError("D1 API request failed")

            # Extract result data
            result_data = data.get("result", [])
            if result_data:
                query_result = result_data[0]
                return CloudflareD1Result(
                    results=query_result.get("results", []),
                    meta=query_result.get("meta", {}),
                    success=query_result.get("success", True),
                )
            else:
                return CloudflareD1Result(results=[], meta={}, success=True)

        except httpx.RequestError as e:
            raise RuntimeError(f"HTTP request failed: {e}")
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"HTTP error {e.response.status_code}: {e.response.text}"
            )

    def execute_many(
        self, query: str, parameters_list: List[Sequence]
    ) -> List["CloudflareD1Result"]:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            parameters_list: List of parameter sequences

        Returns:
            List of CloudflareD1Result objects
        """
        results = []
        for parameters in parameters_list:
            results.append(self.execute(query, parameters))
        return results

    def begin_transaction(self) -> None:
        """Begin a transaction (D1 doesn't support explicit transactions)."""
        # D1 doesn't support explicit transactions via REST API
        # Each query is automatically wrapped in a transaction
        self._in_transaction = True

    def commit_transaction(self) -> None:
        """Commit a transaction (no-op for D1)."""
        self._in_transaction = False

    def rollback_transaction(self) -> None:
        """Rollback a transaction (not supported by D1)."""
        self._in_transaction = False
        # D1 doesn't support explicit rollback via REST API

    def get_server_version(self) -> str:
        """Get D1 server version."""
        # D1 doesn't expose version info via REST API
        return "D1-REST-API"


class CloudflareD1Result:
    """Result wrapper for D1 query responses."""

    def __init__(
        self, results: List[Dict[str, Any]], meta: Dict[str, Any], success: bool = True
    ):
        """Initialize result object.

        Args:
            results: List of result rows as dictionaries
            meta: Query metadata (duration, rows affected, etc.)
            success: Whether the query was successful
        """
        self.results = results
        self.meta = meta
        self.success = success
        self._index = 0

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch the next row."""
        if self._index < len(self.results):
            row = self.results[self._index]
            self._index += 1
            return row
        return None

    def fetchmany(self, size: int = None) -> List[Dict[str, Any]]:
        """Fetch multiple rows."""
        if size is None:
            size = len(self.results) - self._index

        start = self._index
        end = min(start + size, len(self.results))
        rows = self.results[start:end]
        self._index = end
        return rows

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all remaining rows."""
        rows = self.results[self._index :]
        self._index = len(self.results)
        return rows

    def __iter__(self):
        """Make result iterable."""
        return iter(self.results)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Get row by index."""
        return self.results[index]

    @property
    def rowcount(self) -> int:
        """Get number of affected rows."""
        return self.meta.get("changes", len(self.results))

    @property
    def lastrowid(self) -> Optional[int]:
        """Get last inserted row ID."""
        return self.meta.get("last_row_id")

    @property
    def description(self) -> Optional[List]:
        """Get column descriptions (not available in D1 REST API)."""
        # D1 REST API doesn't return column metadata like DBAPI
        # We could infer from first row if available
        if self.results:
            first_row = self.results[0]
            return [
                (name, None, None, None, None, None, None) for name in first_row.keys()
            ]
        return None
