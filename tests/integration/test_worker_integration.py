"""Integration tests for the SQLAlchemy D1 Python Worker.

These tests start the worker using `pywrangler dev` and make HTTP requests
to verify the WorkerConnection class works correctly with D1 bindings.

The tests mirror those in test_restapi_integration.py to ensure parity
between the REST API and Worker binding approaches.

Requires: pywrangler dev running with --local flag for local D1 database.
"""

import requests


class TestWorkerConnection:
    """Test WorkerConnection via Worker HTTP endpoints.

    These tests mirror the TestD1Connection tests in test_restapi_integration.py.
    """

    def test_connection_can_execute_select(self, dev_server):
        """Test basic SELECT query - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/select")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "select"
        assert data["success"] is True
        assert data["row"][0] == 1

    def test_connection_can_query_sqlite_master(self, dev_server):
        """Test querying sqlite_master - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlite-master")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlite_master"
        assert data["success"] is True
        assert isinstance(data["tables"], list)

    def test_cursor_description_populated(self, dev_server):
        """Test cursor description - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/cursor-description")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "cursor_description"
        assert data["success"] is True
        assert data["description"] == ["num", "txt"]

    def test_create_insert_select_drop(self, dev_server):
        """Test CRUD cycle - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/crud")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "crud"
        assert data["success"] is True
        assert data["insert_rowcount"] == 1
        assert len(data["select_rows"]) == 1
        assert data["select_rows"][0][1] == "test_row"
        assert data["select_rows"][0][2] == 42

    def test_parameterized_query(self, dev_server):
        """Test parameterized queries - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/parameterized")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "parameterized"
        assert data["success"] is True
        assert len(data["query_result"]) == 1
        assert data["query_result"][0][0] == "Alice"


class TestHealthCheck:
    """Test the health check endpoint."""

    def test_health_check_returns_healthy(self, dev_server):
        """GET /health should return healthy status."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/health")

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "healthy"
        assert data["database"] == "connected"
        assert data["value"] == 1


class TestIndex:
    """Test the index/documentation endpoint."""

    def test_index_returns_documentation(self, dev_server):
        """GET / should return API documentation."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/")

        assert response.status_code == 200
        data = response.json()

        assert "endpoints" in data
        assert "package" in data
        assert data["package"] == "sqlalchemy-cloudflare-d1"
        assert data["connection_type"] == "WorkerConnection (D1 binding)"

    def test_unknown_endpoint_returns_index(self, dev_server):
        """GET /unknown should return index documentation."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/unknown")

        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data


class TestSQLAlchemyCore:
    """Test SQLAlchemy Core functionality via create_engine_from_binding().

    These tests verify that SQLAlchemy Core patterns work inside Workers
    without using raw SQL.
    """

    def test_sqlalchemy_select(self, dev_server):
        """Test SQLAlchemy Core SELECT using text()."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-select")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_select"
        assert data["success"] is True
        assert data["row"][0] == 1

    def test_sqlalchemy_crud(self, dev_server):
        """Test SQLAlchemy Core CRUD without raw SQL.

        Uses Table, MetaData, insert(), select() - no raw SQL strings.
        """
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-crud")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_crud"
        assert data["success"] is True
        assert len(data["rows"]) == 1
        assert data["rows"][0][1] == "test_row"
        assert data["rows"][0][2] == 42
        assert "id" in data["columns"]
        assert "name" in data["columns"]
        assert "value" in data["columns"]

    def test_sqlalchemy_reflect(self, dev_server):
        """Test SQLAlchemy table reflection with autoload_with."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-reflect")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_reflect"
        assert data["success"] is True
        assert len(data["rows"]) == 1
        assert data["rows"][0][1] == "alice"

        # Check reflected columns
        column_names = [col["name"] for col in data["reflected_columns"]]
        assert "id" in column_names
        assert "username" in column_names
        assert "email" in column_names
