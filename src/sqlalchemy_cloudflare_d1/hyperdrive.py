"""SQLAlchemy engine support for Cloudflare Hyperdrive in Python Workers.

Hyperdrive fronts a regular PostgreSQL or MySQL database, so no custom dialect
is required -- SQLAlchemy's own ``postgresql+pg8000`` / ``mysql+pymysql``
dialects speak the wire protocol. What this module provides is the Worker-side
glue:

1. Building an engine URL from the Hyperdrive binding's connection fields.
2. Pool settings appropriate for the Workers runtime (sockets cannot outlive a
   request, and Hyperdrive already pools server-side).
3. Disabling client-side TLS, which Hyperdrive terminates for you.
4. An asyncio lock so synchronous driver I/O is serialized within an isolate.

Only *synchronous* SQLAlchemy is supported. Async SQLAlchemy needs greenlet,
which is unavailable in the Python Workers runtime, which also rules out the
async drivers Cloudflare recommends elsewhere (``asyncpg``, ``aiomysql``).

Example:
    from sqlalchemy import MetaData, Table, select
    from sqlalchemy_cloudflare_d1.hyperdrive import (
        create_engine_from_hyperdrive,
        hyperdrive_connection,
    )

    class Default(WorkerEntrypoint):
        async def fetch(self, request):
            engine = create_engine_from_hyperdrive(self.env.HYPERDRIVE)

            metadata = MetaData()
            users = Table("users", metadata, autoload_with=engine)

            async with hyperdrive_connection(engine) as conn:
                rows = conn.execute(select(users).limit(10)).fetchall()
"""

import asyncio
import weakref
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional

# MARK: - Driver Registry

#: Maps a driver name to its SQLAlchemy ``drivername`` and the connect
#: arguments that disable client-side TLS. Hyperdrive terminates TLS itself,
#: and the Workers socket shim does not support renegotiating it.
_DRIVERS: Dict[str, Dict[str, Any]] = {
    "pg8000": {
        "drivername": "postgresql+pg8000",
        "default_port": 5432,
        # pg8000 treats ssl_context=None as "attempt SSL" -- only False
        # disables it. Attempting TLS here corrupts the stream, because the
        # Workers socket shim cannot complete the handshake.
        "connect_args": {"ssl_context": False},
        "extra": "hyperdrive",
    },
    # MySQL is best effort: verified by hand against MySQL 8, but there is no
    # automated coverage for it. PostgreSQL via pg8000 is the supported path.
    "pymysql": {
        "drivername": "mysql+pymysql",
        "default_port": 3306,
        # Same trap as pg8000: without ssl_disabled, PyMySQL tries STARTTLS and
        # the Workers socket layer rejects it with a secureTransport error.
        "connect_args": {"ssl_disabled": True},
        "extra": "hyperdrive-mysql",
    },
}

# psycopg is deliberately absent. It is listed as supported in Cloudflare's
# docs, but psycopg3 needs libpq and the Workers Pyodide build has none, so
# every connection fails with "no pq wrapper available."
#
# asyncpg and aiomysql are absent for a different reason: they are async-only,
# and SQLAlchemy's async engine needs greenlet, which Workers lack. They work
# fine for raw driver use -- just not behind a SQLAlchemy engine.

#: Driver used when the binding's scheme says PostgreSQL (or is unknown).
DEFAULT_POSTGRES_DRIVER = "pg8000"

#: Driver used when the binding's scheme says MySQL.
DEFAULT_MYSQL_DRIVER = "pymysql"

# MARK: - Serialization Lock

#: One lock per event loop. The Workers socket layer does not tolerate
#: concurrent synchronous driver I/O from a single isolate, so
#: ``hyperdrive_connection()`` holds this lock and overlapping requests take
#: turns instead of interleaving reads.
#:
#: Built lazily rather than at import: on Python 3.9 ``asyncio.Lock()`` binds
#: to whichever loop is current when it is constructed, so a module-level lock
#: raises "attached to a different loop" under any other loop.
_IO_LOCKS: "weakref.WeakKeyDictionary[Any, asyncio.Lock]" = weakref.WeakKeyDictionary()


def _io_lock() -> asyncio.Lock:
    """Return the I/O lock belonging to the running event loop."""
    loop = asyncio.get_running_loop()
    lock = _IO_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _IO_LOCKS[loop] = lock
    return lock


# MARK: - Helper Functions


def _binding_attr(binding: Any, name: str, default: Any = None) -> Any:
    """Read a field from the Hyperdrive binding.

    The binding arrives as a JsProxy, so attribute access is the normal path,
    but fall back to item access for dict-like stand-ins used in tests.
    """
    value = getattr(binding, name, None)
    if value is None:
        try:
            value = binding[name]
        except (TypeError, KeyError, IndexError):
            value = None
    return default if value is None else value


def _detect_driver(binding: Any) -> str:
    """Pick a driver based on the binding's database engine.

    A deployed Hyperdrive binding exposes ``scheme`` directly; prefer it, and
    fall back to the scheme embedded in ``connectionString``. When neither is
    recognized we assume PostgreSQL, which is the common case.
    """
    scheme = str(_binding_attr(binding, "scheme", "")).lower()

    if not scheme:
        connection_string = _binding_attr(binding, "connectionString", "")
        scheme = str(connection_string).split("://", 1)[0].lower()

    if scheme.startswith("mysql"):
        return DEFAULT_MYSQL_DRIVER
    return DEFAULT_POSTGRES_DRIVER


# MARK: - Engine Factory


def create_engine_from_hyperdrive(
    hyperdrive_binding: Any,
    driver: Optional[str] = None,
    connect_args: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Any:
    """Create a SQLAlchemy engine from a Hyperdrive Worker binding.

    Args:
        hyperdrive_binding: The Hyperdrive binding from Worker env
            (e.g., self.env.HYPERDRIVE)
        driver: Driver to connect with -- "pg8000" or "pymysql".
            Defaults to the driver matching the binding's scheme. The driver
            must be declared in the Worker's pyproject dependencies.
        connect_args: Extra DBAPI connect arguments, merged over (and able to
            override) the TLS defaults for the chosen driver.
        **kwargs: Additional arguments passed to create_engine()
            (echo, isolation_level, etc.)

    Returns:
        SQLAlchemy Engine pointed at the Hyperdrive connection.

    Raises:
        ValueError: If ``driver`` is not one of the supported drivers.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    from sqlalchemy.pool import NullPool

    resolved_driver = driver or _detect_driver(hyperdrive_binding)
    if resolved_driver not in _DRIVERS:
        raise ValueError(
            f"Unsupported Hyperdrive driver {resolved_driver!r}. "
            f"Expected one of: {', '.join(sorted(_DRIVERS))}. "
            "Async drivers (asyncpg, aiomysql) cannot be used because "
            "SQLAlchemy's async engine requires greenlet, which is "
            "unavailable in Python Workers."
        )

    spec = _DRIVERS[resolved_driver]

    port = _binding_attr(hyperdrive_binding, "port", spec["default_port"])
    url = URL.create(
        drivername=spec["drivername"],
        host=str(_binding_attr(hyperdrive_binding, "host", "")),
        port=int(port),
        username=str(_binding_attr(hyperdrive_binding, "user", "")),
        password=str(_binding_attr(hyperdrive_binding, "password", "")),
        database=str(_binding_attr(hyperdrive_binding, "database", "")),
    )

    merged_connect_args = dict(spec["connect_args"])
    if connect_args:
        merged_connect_args.update(connect_args)

    # MARK: - NullPool: a Worker cannot reuse sockets across requests, and
    # Hyperdrive maintains the real pool on the server side.
    try:
        return create_engine(
            url,
            poolclass=NullPool,
            connect_args=merged_connect_args,
            **kwargs,
        )
    except ModuleNotFoundError as e:
        # The driver is picked from the binding's scheme, so a MySQL origin can
        # select a driver the caller never knowingly chose. Say which extra
        # provides it rather than leaving a bare import error.
        raise ModuleNotFoundError(
            f"The {resolved_driver!r} driver is required for this Hyperdrive "
            f"binding but is not installed. Install it with: "
            f"pip install sqlalchemy-cloudflare-d1[{spec['extra']}] -- and "
            f"declare {resolved_driver!r} in your Worker's pyproject "
            f"dependencies so pywrangler bundles it."
        ) from e


# MARK: - Serialized Connection Helper


@asynccontextmanager
async def hyperdrive_connection(engine: Any) -> AsyncIterator[Any]:
    """Yield a SQLAlchemy connection while holding the isolate-wide I/O lock.

    Synchronous driver calls block the isolate, and concurrent socket use is
    not supported, so database work is serialized. Prefer this over calling
    ``engine.connect()`` directly inside a Worker.

    Example:
        async with hyperdrive_connection(engine) as conn:
            rows = conn.execute(select(users)).fetchall()
    """
    async with _io_lock():
        with engine.connect() as conn:
            yield conn
