"""Integration tests for the configurable base URL feature (Issue #22).

These tests verify that CF_D1_BASE_URL and the base_url kwarg correctly
redirect HTTP requests away from the hard-coded Cloudflare endpoint.

A minimal local HTTP server mimics the D1 /raw response format so that
no real Cloudflare credentials are required to run these tests.
"""

import json
import os
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from sqlalchemy_cloudflare_d1.connection import Connection


# MARK: - Local D1 Proxy Fixture


@pytest.fixture()
def local_d1_server():
    """Start a local HTTP server that mimics the D1 REST API /raw endpoint.

    Modelled after the local D1 proxy described in Issue #22. Responds with
    the Cloudflare /raw response envelope so the Connection can parse it.
    """

    received_requests: list[str] = []

    class _D1Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received_requests.append(self.path)
            body_bytes = self.rfile.read(int(self.headers["Content-Length"]))
            json.loads(body_bytes)  # parse but ignore — any SQL is fine

            response = {
                "success": True,
                "result": [
                    {
                        "results": {
                            "columns": ["value"],
                            "rows": [[42]],
                        },
                        "meta": {},
                        "success": True,
                    }
                ],
                "errors": [],
                "messages": [],
            }
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):  # noqa: A002
            pass  # suppress server log noise in test output

    server = HTTPServer(("127.0.0.1", 0), _D1Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield f"http://127.0.0.1:{port}", received_requests

    server.shutdown()


# MARK: - Base URL Override Tests


class TestBaseUrlOverride:
    """Verify that base_url kwarg and CF_D1_BASE_URL env var route requests
    to a custom endpoint instead of the default Cloudflare API URL.
    """

    def test_base_url_kwarg_redirects_requests(self, local_d1_server):
        """Connection uses base_url kwarg instead of the Cloudflare endpoint."""
        base_url, received = local_d1_server
        conn = Connection(
            account_id="fake_account",
            database_id="fake_db",
            api_token="fake_token",
            base_url=base_url,
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 AS value")
            row = cur.fetchone()
            assert row == (42,)
            assert any("/raw" in path for path in received)
        finally:
            conn.close()

    def test_cf_d1_base_url_env_var_redirects_requests(self, local_d1_server):
        """Connection reads CF_D1_BASE_URL from the environment and routes there."""
        base_url, received = local_d1_server
        with patch.dict(os.environ, {"CF_D1_BASE_URL": base_url}):
            conn = Connection(
                account_id="fake_account",
                database_id="fake_db",
                api_token="fake_token",
            )
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1 AS value")
                row = cur.fetchone()
                assert row == (42,)
                assert any("/raw" in path for path in received)
            finally:
                conn.close()

    def test_sqlalchemy_engine_with_base_url_query_param(self, local_d1_server):
        """Engine built from a URL with ?base_url= routes requests to the local server."""
        base_url, received = local_d1_server
        encoded = urllib.parse.quote(base_url, safe="")
        engine = create_engine(
            f"cloudflare_d1://fake_account:fake_token@fake_db?base_url={encoded}"
        )
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 AS value"))
                row = result.fetchone()
            assert row == (42,)
            assert any("/raw" in path for path in received)
        finally:
            engine.dispose()

    def test_base_url_kwarg_takes_precedence_over_env_var(self, local_d1_server):
        """When both base_url kwarg and CF_D1_BASE_URL are set, kwarg wins."""
        base_url, received = local_d1_server
        with patch.dict(
            os.environ, {"CF_D1_BASE_URL": "http://should-not-be-used:9999"}
        ):
            conn = Connection(
                account_id="fake_account",
                database_id="fake_db",
                api_token="fake_token",
                base_url=base_url,
            )
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1 AS value")
                row = cur.fetchone()
                assert row == (42,)
            finally:
                conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
