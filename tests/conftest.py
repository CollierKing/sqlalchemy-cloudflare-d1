"""Pytest configuration and fixtures for testing the D1 Python Worker."""

import os
import shutil
import socket
import subprocess
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Optional

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

from tests.test_utils import (
    make_sqlite_method,
    make_sqlite_upsert_method,
    SQLI_PAYLOADS,
    PANDAS_BASIC_DATA,
)


def sync_package_to_python_modules(project_dir: Path) -> None:
    """Copy the latest package source to python_modules for Workers.

    pywrangler has a bug where it doesn't update the bundled packages,
    so we need to manually copy the source files.

    Args:
        project_dir: Path to the examples/workers directory
    """
    src_dir = project_dir.parent.parent / "src" / "sqlalchemy_cloudflare_d1"
    dest_dir = project_dir / "python_modules" / "sqlalchemy_cloudflare_d1"

    if not dest_dir.exists():
        # Need to run pywrangler once to create python_modules
        subprocess.run(
            ["uv", "run", "pywrangler", "--help"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        # The python_modules might not exist yet, will be created on first dev run

    if dest_dir.exists():
        # Copy all .py files from source to destination
        for src_file in src_dir.glob("*.py"):
            shutil.copy2(src_file, dest_dir / src_file.name)


def init_local_database(project_dir: Path) -> None:
    """Initialize the local D1 database with schema and sample data.

    Note: For remote D1 tests, the database should already exist.
    This function is only needed for local testing.

    Args:
        project_dir: Path to the project directory containing db_init.sql
    """
    # Skip local DB init if we're using remote D1
    # The wrangler.jsonc has remote: true configured
    pass


def find_free_port() -> int:
    """Find an available port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def pywrangler_dev_server(
    project_dir: Path, timeout: int = 300, env: Optional[dict] = None
) -> tuple[subprocess.Popen, int]:
    """Start a pywrangler dev server and return the process and port.

    Args:
        project_dir: Path to the project directory containing wrangler.jsonc
        timeout: Maximum time to wait for server startup (default 300s for CI)
        env: Extra environment variables for the server process

    Returns:
        Tuple of (process, port)
    """
    port = find_free_port()

    # Start the dev server with --local flag
    # Note: --remote has issues with Python Workers on Cloudflare edge
    process = subprocess.Popen(
        ["uv", "run", "pywrangler", "dev", "--local", "--port", str(port)],
        cwd=project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, **(env or {})},
    )

    # Wait for server to be ready
    start_time = time.time()
    ready_message = "[wrangler:info] Ready on"

    while time.time() - start_time < timeout:
        if process.poll() is not None:
            # Process exited
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"pywrangler dev exited unexpectedly: {output}")

        line = process.stdout.readline() if process.stdout else ""
        if ready_message in line:
            return process, port

        # Also check for alternative ready messages
        if f"localhost:{port}" in line.lower() or "ready" in line.lower():
            # Give it a moment to fully initialize
            time.sleep(0.5)
            return process, port

    # Timeout reached
    process.terminate()
    raise TimeoutError(f"pywrangler dev did not start within {timeout} seconds")


def get_worker_project_dir() -> Path:
    """Get the path to the examples/workers directory."""
    return Path(__file__).parent.parent / "examples" / "workers"


def get_hyperdrive_project_dir() -> Path:
    """Get the path to the examples/workers-hyperdrive directory."""
    return Path(__file__).parent.parent / "examples" / "workers-hyperdrive"


@pytest.fixture(scope="session")
def initialized_worker():
    """Session-scoped fixture that sets up the Worker environment once.

    This runs once per test session to:
    1. Sync the package source to python_modules (workaround for pywrangler bug)
    2. Initialize the local D1 database with schema and sample data
    """
    project_dir = get_worker_project_dir()
    sync_package_to_python_modules(project_dir)
    init_local_database(project_dir)
    return True


@pytest.fixture(scope="session")
def dev_server(initialized_worker):
    """Session-scoped fixture that starts a single pywrangler dev server.

    The server is reused across all Worker tests and stopped after the session.
    Depends on initialized_worker to ensure Worker environment is set up first.

    Yields:
        int: The port number the server is running on
    """

    project_dir = get_worker_project_dir()

    process = None
    try:
        process, port = pywrangler_dev_server(project_dir)
        yield port
    finally:
        if process is not None:
            # Kill the entire process group to clean up workerd child processes
            try:
                # Try graceful termination first
                process.terminate()
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't terminate
                process.kill()
                process.wait()

            # Also kill any orphaned workerd processes on this port
            try:
                subprocess.run(
                    ["pkill", "-f", f"workerd.*{port}"],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                pass


@pytest.fixture(scope="session")
def hyperdrive_dev_server():
    """Session-scoped fixture that starts the Hyperdrive example Worker.

    Hyperdrive has no local emulation: `wrangler dev` always connects to the
    Postgres named by CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE
    rather than to the real Hyperdrive config. Tests are skipped unless that
    variable is set.

    The example table is created via /setup before the tests and dropped via
    /teardown afterwards, so a run leaves the origin database as it found it.
    A Hyperdrive config often points at a shared database.

    Yields:
        int: The port number the server is running on
    """
    import requests

    conn_var = "CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE"
    if not os.environ.get(conn_var):
        pytest.skip(f"{conn_var} not set (needs a reachable Postgres)")

    project_dir = get_hyperdrive_project_dir()
    sync_package_to_python_modules(project_dir)

    process = None
    port = None
    try:
        process, port = pywrangler_dev_server(
            project_dir, env={conn_var: os.environ[conn_var]}
        )

        setup = requests.get(f"http://localhost:{port}/setup", timeout=120).json()
        assert setup.get("success"), f"Hyperdrive /setup failed: {setup}"

        yield port
    finally:
        if port is not None:
            try:
                requests.get(f"http://localhost:{port}/teardown", timeout=120)
            except Exception:
                pass

        if process is not None:
            try:
                process.terminate()
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

            try:
                subprocess.run(
                    ["pkill", "-f", f"workerd.*{port}"],
                    capture_output=True,
                    timeout=5,
                )
            except Exception:
                pass


@pytest.fixture
def worker_project_dir():
    """Return the examples/workers directory."""
    return get_worker_project_dir()


@pytest.fixture
def project_dir():
    """Return the project root directory."""
    return Path(__file__).parent.parent


# MARK: - Shared Test Fixtures


@pytest.fixture
def test_table_name():
    """Generate a unique test table name."""
    return f"test_sqlalchemy_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sqlite_insert_method():
    """Return the make_sqlite_method helper for pandas to_sql."""
    return make_sqlite_method


@pytest.fixture
def sqlite_upsert_method():
    """Return the make_sqlite_upsert_method helper for pandas to_sql."""
    return make_sqlite_upsert_method


@pytest.fixture
def sqli_payloads():
    """Return SQL injection test payloads."""
    return SQLI_PAYLOADS


@pytest.fixture
def pandas_basic_data():
    """Return basic pandas test data."""
    return PANDAS_BASIC_DATA.copy()


# MARK: - REST API Fixtures


@pytest.fixture
def d1_credentials():
    """Return D1 credentials from environment variables."""
    account_id = os.environ.get("CF_ACCOUNT_ID")
    api_token = os.environ.get("TEST_CF_API_TOKEN")
    database_id = os.environ.get("CF_D1_DATABASE_ID")
    return {
        "account_id": account_id,
        "api_token": api_token,
        "database_id": database_id,
        "available": all([account_id, api_token, database_id]),
    }


@pytest.fixture
def d1_connection(d1_credentials):
    """Create a real D1 connection for REST API tests."""
    if not d1_credentials["available"]:
        pytest.skip("D1 credentials not set")

    from sqlalchemy_cloudflare_d1 import Connection

    conn = Connection(
        account_id=d1_credentials["account_id"],
        database_id=d1_credentials["database_id"],
        api_token=d1_credentials["api_token"],
    )
    yield conn
    conn.close()


@pytest.fixture
def d1_engine(d1_credentials):
    """Create a SQLAlchemy engine connected to D1 for REST API tests."""
    if not d1_credentials["available"]:
        pytest.skip("D1 credentials not set")

    url = (
        f"cloudflare_d1://{d1_credentials['account_id']}:"
        f"{d1_credentials['api_token']}@{d1_credentials['database_id']}"
    )
    engine = create_engine(url)
    yield engine
    engine.dispose()


# MARK: - Shared Table Schema Fixtures


@pytest.fixture
def basic_test_table(test_table_name):
    """Return a basic test table schema (id, name, value)."""
    metadata = MetaData()
    table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100)),
        Column("value", Integer),
    )
    return table, metadata


@pytest.fixture
def user_test_table(test_table_name):
    """Return a user test table schema (id, username, email)."""
    metadata = MetaData()
    table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("username", String(100)),
        Column("email", String(100)),
    )
    return table, metadata


@pytest.fixture
def auth_test_table(test_table_name):
    """Return an auth test table schema (id, username, password)."""
    metadata = MetaData()
    table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("username", String(100)),
        Column("password", String(100)),
    )
    return table, metadata


@pytest.fixture
def json_tags_table(test_table_name):
    """Return a table with JSON tags column (id, name, tags)."""
    metadata = MetaData()
    table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100)),
        Column("tags", String),  # JSON array stored as TEXT
    )
    return table, metadata


@pytest.fixture
def scores_table(test_table_name):
    """Return a scores table schema (id, name, score) with unique name."""
    metadata = MetaData()
    table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100), unique=True),
        Column("score", Integer),
    )
    return table, metadata
