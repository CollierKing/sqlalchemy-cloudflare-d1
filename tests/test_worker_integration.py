"""Integration tests for the SQLAlchemy D1 Python Worker.

These tests start the worker using `pywrangler dev` and make HTTP requests
to verify the endpoints work correctly with the D1 binding.

The tests use the WorkerConnection class from sqlalchemy-cloudflare-d1 to
interact with D1 inside the Python Worker.
"""

import requests


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


class TestUsers:
    """Test user-related endpoints."""

    def test_list_users_returns_users(self, dev_server):
        """GET /users should return list of users."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/users")

        assert response.status_code == 200
        data = response.json()

        assert "users" in data
        assert "count" in data
        assert isinstance(data["users"], list)
        assert data["count"] == len(data["users"])

    def test_random_user_returns_single_user(self, dev_server):
        """GET /random should return a single random user."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/random")

        assert response.status_code == 200
        data = response.json()

        # Should have user fields
        assert "id" in data
        assert "name" in data
        assert "email" in data

    def test_create_user_inserts_user(self, dev_server):
        """GET /create should create a new user."""
        port = dev_server

        # Get initial count
        response = requests.get(f"http://localhost:{port}/users")
        initial_count = response.json()["count"]

        # Create a user
        response = requests.get(f"http://localhost:{port}/create")
        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert "user_id" in data

        # Verify count increased
        response = requests.get(f"http://localhost:{port}/users")
        new_count = response.json()["count"]
        assert new_count == initial_count + 1


class TestWorkerConnection:
    """Test that WorkerConnection is being used correctly."""

    def test_cursor_description_populated(self, dev_server):
        """Verify that cursor description works (columns are returned)."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/users")

        assert response.status_code == 200
        data = response.json()

        # If we have users, they should have the expected columns
        if data["count"] > 0:
            user = data["users"][0]
            assert "id" in user
            assert "name" in user
            assert "email" in user
            assert "created_at" in user

    def test_parameterized_queries_work(self, dev_server):
        """Verify parameterized queries work (create uses parameters)."""
        port = dev_server

        # Create endpoint uses parameterized query
        response = requests.get(f"http://localhost:{port}/create")
        assert response.status_code == 200

        data = response.json()
        assert data["success"] is True


class TestErrorHandling:
    """Test error handling."""

    def test_unknown_endpoint_returns_index(self, dev_server):
        """GET /unknown should return index documentation."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/unknown")

        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data
