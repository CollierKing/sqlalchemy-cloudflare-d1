"""Pytest configuration and fixtures for testing the D1 Python Worker."""

import shutil
import socket
import subprocess
import time
from contextlib import closing
from pathlib import Path

import pytest


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

    Args:
        project_dir: Path to the project directory containing db_init.sql
    """
    result = subprocess.run(
        [
            "uv",
            "run",
            "pywrangler",
            "d1",
            "execute",
            "example-db",
            "--local",
            "--file",
            "db_init.sql",
        ],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to initialize database: {result.stderr}")


def find_free_port() -> int:
    """Find an available port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def pywrangler_dev_server(
    project_dir: Path, timeout: int = 300
) -> tuple[subprocess.Popen, int]:
    """Start a pywrangler dev server and return the process and port.

    Args:
        project_dir: Path to the project directory containing wrangler.jsonc
        timeout: Maximum time to wait for server startup (default 300s for CI)

    Returns:
        Tuple of (process, port)
    """
    port = find_free_port()

    # Start the dev server with --local flag for local D1 database
    process = subprocess.Popen(
        ["uv", "run", "pywrangler", "dev", "--local", "--port", str(port)],
        cwd=project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
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


@pytest.fixture
def dev_server(request, initialized_worker):
    """Fixture that starts a pywrangler dev server for the test.

    The server is automatically stopped after the test completes.
    Depends on initialized_worker to ensure Worker environment is set up first.

    Yields:
        int: The port number the server is running on
    """
    # Skip server startup for skipped/xfail tests
    markers = list(request.node.iter_markers())
    marker_names = [m.name for m in markers]
    if "skip" in marker_names or "xfail" in marker_names:
        pytest.skip("Skipping server startup for marked test")

    project_dir = get_worker_project_dir()

    process = None
    try:
        process, port = pywrangler_dev_server(project_dir)
        yield port
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.fixture
def worker_project_dir():
    """Return the examples/workers directory."""
    return get_worker_project_dir()


@pytest.fixture
def project_dir():
    """Return the project root directory."""
    return Path(__file__).parent.parent
