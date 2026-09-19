"""
Unit tests for Hyperdrive engine support.

These do not open sockets -- they only assert on the URL, pool and connect
arguments that create_engine_from_hyperdrive() derives from a binding.
"""

import asyncio

import pytest
from sqlalchemy.pool import NullPool

from sqlalchemy_cloudflare_d1 import (
    create_engine_from_hyperdrive,
    hyperdrive_connection,
)


# MARK: - Fakes


class FakeBinding:
    """Stand-in for the Hyperdrive JsProxy binding."""

    def __init__(self, connection_string="postgresql://u:p@hd.local:5432/db", **kw):
        self.host = kw.get("host", "hd.local")
        self.port = kw.get("port", "5432")
        self.user = kw.get("user", "hd_user")
        self.password = kw.get("password", "hd_pass")
        self.database = kw.get("database", "hd_db")
        self.connectionString = connection_string


class DictBinding(dict):
    """Binding exposing fields via item access only."""


class _StopConnect(Exception):
    """Raised from the do_connect hook to abort before touching the network."""


def capture_connect_params(engine):
    """Return the DBAPI kwargs the engine would connect with, without dialing.

    SQLAlchemy merges connect_args into cparams just before the driver call,
    so the do_connect hook is the only place they are observable.
    """
    from sqlalchemy import event

    captured = {}

    @event.listens_for(engine, "do_connect")
    def _capture(dialect, conn_rec, cargs, cparams):  # noqa: ANN001
        captured.update(cparams)
        raise _StopConnect()

    with pytest.raises(_StopConnect):
        engine.connect()

    return captured


# MARK: - URL Construction


def test_postgres_url_from_binding():
    """Test that a PostgreSQL binding produces a pg8000 URL."""
    engine = create_engine_from_hyperdrive(FakeBinding())

    assert engine.url.drivername == "postgresql+pg8000"
    assert engine.url.host == "hd.local"
    assert engine.url.port == 5432
    assert engine.url.username == "hd_user"
    assert engine.url.password == "hd_pass"
    assert engine.url.database == "hd_db"


def test_mysql_url_from_binding():
    """Test that a MySQL connection string selects the pymysql driver."""
    engine = create_engine_from_hyperdrive(
        FakeBinding(connection_string="mysql://u:p@hd.local:3306/db", port="3306")
    )

    assert engine.url.drivername == "mysql+pymysql"
    assert engine.url.port == 3306


def test_scheme_attribute_takes_precedence():
    """Test that a deployed binding's `scheme` attribute drives detection.

    Deployed bindings expose `scheme` directly, and their connectionString is
    not always a useful indicator.
    """
    binding = FakeBinding(connection_string="")
    binding.scheme = "mysql"

    engine = create_engine_from_hyperdrive(binding)

    assert engine.url.drivername == "mysql+pymysql"


def test_falls_back_to_connection_string_without_scheme():
    """Test that connectionString is still used when scheme is absent."""
    engine = create_engine_from_hyperdrive(
        FakeBinding(connection_string="mysql://u:p@h:3306/d")
    )

    assert engine.url.drivername == "mysql+pymysql"


def test_defaults_to_postgres_without_connection_string():
    """Test that a binding with no connectionString is assumed PostgreSQL."""
    engine = create_engine_from_hyperdrive(FakeBinding(connection_string=""))

    assert engine.url.drivername == "postgresql+pg8000"


def test_special_characters_in_password_are_escaped():
    """Test that credentials needing escaping survive URL construction."""
    engine = create_engine_from_hyperdrive(FakeBinding(password="p@ss/w:rd?"))

    assert engine.url.password == "p@ss/w:rd?"
    assert "p%40ss" in engine.url.render_as_string(hide_password=False)


def test_binding_fields_read_via_item_access():
    """Test that a dict-like binding is supported alongside attribute access."""
    binding = DictBinding(
        host="items.local",
        port=5432,
        user="u",
        password="p",
        database="d",
        connectionString="postgresql://u:p@items.local:5432/d",
    )

    engine = create_engine_from_hyperdrive(binding)

    assert engine.url.host == "items.local"


def test_port_string_is_coerced_to_int():
    """Test that the binding's string port becomes an integer."""
    engine = create_engine_from_hyperdrive(FakeBinding(port="6432"))

    assert engine.url.port == 6432


# MARK: - Driver Selection


def test_explicit_driver_overrides_detection():
    """Test that an explicit driver wins over the connection string scheme."""
    engine = create_engine_from_hyperdrive(FakeBinding(), driver="pymysql")

    assert engine.url.drivername == "mysql+pymysql"


def test_async_driver_is_rejected():
    """Test that async drivers raise, since greenlet is unavailable in Workers."""
    with pytest.raises(ValueError, match="greenlet"):
        create_engine_from_hyperdrive(FakeBinding(), driver="asyncpg")


def test_unknown_driver_is_rejected():
    """Test that an unrecognized driver name raises."""
    with pytest.raises(ValueError, match="Unsupported Hyperdrive driver"):
        create_engine_from_hyperdrive(FakeBinding(), driver="sqlite3")


# MARK: - Pool and Connect Arguments


def test_uses_null_pool():
    """Test that NullPool is used, since Workers cannot reuse sockets."""
    engine = create_engine_from_hyperdrive(FakeBinding())

    assert isinstance(engine.pool, NullPool)


def test_tls_disabled_by_default_for_pg8000():
    """Test that client-side TLS is off, as Hyperdrive terminates it.

    Must be exactly False: pg8000 reads ssl_context=None as "attempt SSL",
    which corrupts the stream inside a Worker.
    """
    engine = create_engine_from_hyperdrive(FakeBinding())

    assert capture_connect_params(engine)["ssl_context"] is False


def test_tls_disabled_by_default_for_pymysql():
    """Test that PyMySQL is told not to attempt STARTTLS.

    Without ssl_disabled the Workers socket layer rejects the upgrade with a
    secureTransport error.
    """
    engine = create_engine_from_hyperdrive(FakeBinding(), driver="pymysql")

    assert capture_connect_params(engine)["ssl_disabled"] is True


def test_missing_driver_names_the_extra_that_provides_it():
    """Test that a missing driver reports how to install it.

    The driver is chosen from the binding's scheme, so a MySQL origin can
    select one the caller never explicitly asked for; a bare ImportError from
    deep inside SQLAlchemy is not an actionable message.
    """
    from unittest.mock import patch

    with patch(
        "sqlalchemy.create_engine", side_effect=ModuleNotFoundError("no pymysql")
    ):
        with pytest.raises(ModuleNotFoundError) as excinfo:
            create_engine_from_hyperdrive(FakeBinding(), driver="pymysql")

    message = str(excinfo.value)
    assert "pymysql" in message
    assert "hyperdrive-mysql" in message


def test_every_driver_declares_an_installable_extra():
    """Test that each driver names an extra that pyproject actually defines."""
    import re
    from pathlib import Path

    from sqlalchemy_cloudflare_d1.hyperdrive import _DRIVERS

    # Read pyproject textually: tomllib is 3.11+ and this package supports 3.9.
    pyproject = (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()
    section = pyproject.split("[project.optional-dependencies]", 1)[1]
    section = section.split("\n[", 1)[0]
    declared = set(re.findall(r"^([A-Za-z0-9_.-]+)\s*=\s*\[", section, re.MULTILINE))

    assert declared, "failed to parse any extras from pyproject.toml"

    for name, spec in _DRIVERS.items():
        assert "extra" in spec, f"{name} declares no extra"
        assert spec["extra"] in declared, (
            f"{name} points at extra {spec['extra']!r}, "
            f"which pyproject does not define (found: {sorted(declared)})"
        )


def test_psycopg_is_not_offered():
    """Test that psycopg is rejected -- Workers have no libpq for it."""
    with pytest.raises(ValueError, match="Unsupported Hyperdrive driver"):
        create_engine_from_hyperdrive(FakeBinding(), driver="psycopg")


def test_connect_args_override_defaults():
    """Test that caller-supplied connect_args merge over the TLS defaults."""
    engine = create_engine_from_hyperdrive(
        FakeBinding(), connect_args={"ssl_context": "custom", "timeout": 5}
    )

    args = capture_connect_params(engine)
    assert args["ssl_context"] == "custom"
    assert args["timeout"] == 5


def test_create_engine_kwargs_are_forwarded():
    """Test that extra kwargs reach create_engine()."""
    engine = create_engine_from_hyperdrive(FakeBinding(), echo=True)

    assert engine.echo is True


# MARK: - Serialized Connection Helper


def test_hyperdrive_connection_serializes_access():
    """Test that overlapping connections are serialized by the I/O lock."""
    events = []

    class FakeConn:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, *exc):
            events.append("exit")
            return False

    class FakeEngine:
        def connect(self):
            return FakeConn()

    async def worker():
        async with hyperdrive_connection(FakeEngine()):
            await asyncio.sleep(0)

    async def main():
        await asyncio.gather(worker(), worker())

    asyncio.run(main())

    # Serialized access never nests: each enter is followed by its own exit.
    assert events == ["enter", "exit", "enter", "exit"]


if __name__ == "__main__":
    pytest.main([__file__])
