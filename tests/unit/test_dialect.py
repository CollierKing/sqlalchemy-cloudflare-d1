"""
Basic tests for the Cloudflare D1 SQLAlchemy dialect.
"""

import pytest
from sqlalchemy_cloudflare_d1 import CloudflareD1Dialect


def test_dialect_import():
    """Test that the dialect can be imported."""
    assert CloudflareD1Dialect is not None


def test_dialect_instantiation():
    """Test that the dialect can be instantiated."""
    dialect = CloudflareD1Dialect()
    assert dialect.name == "cloudflare_d1"
    assert dialect.driver == "httpx"


def test_engine_creation():
    """Test that a dialect can parse connection URLs correctly."""
    from sqlalchemy.engine.url import make_url

    # Test URL parsing without requiring entry point registration
    url = make_url("cloudflare_d1://test_account:test_token@test_database_id")
    dialect = CloudflareD1Dialect()

    args, kwargs = dialect.create_connect_args(url)
    assert kwargs["account_id"] == "test_account"
    assert kwargs["api_token"] == "test_token"
    assert kwargs["database_id"] == "test_database_id"


def test_connection_args_parsing():
    """Test URL parsing for connection arguments."""
    from sqlalchemy.engine.url import make_url

    dialect = CloudflareD1Dialect()
    url = make_url("cloudflare_d1://account123:token456@database789?timeout=30")

    args, kwargs = dialect.create_connect_args(url)

    assert kwargs["account_id"] == "account123"
    assert kwargs["api_token"] == "token456"
    assert kwargs["database_id"] == "database789"
    assert kwargs.get("timeout") == "30"


def test_reserved_words():
    """Test that reserved words are defined."""
    dialect = CloudflareD1Dialect()
    assert len(dialect.reserved_words) > 0
    assert "select" in dialect.reserved_words
    assert "from" in dialect.reserved_words


def test_compiler_classes():
    """Test that compiler classes are properly assigned."""
    from sqlalchemy_cloudflare_d1.compiler import (
        CloudflareD1Compiler,
        CloudflareD1DDLCompiler,
        CloudflareD1TypeCompiler,
    )

    dialect = CloudflareD1Dialect()
    assert dialect.statement_compiler == CloudflareD1Compiler
    assert dialect.ddl_compiler == CloudflareD1DDLCompiler
    # type_compiler gets instantiated automatically by SQLAlchemy
    assert isinstance(dialect.type_compiler, CloudflareD1TypeCompiler)


def test_on_conflict_do_update_compilation():
    """Test that ON CONFLICT DO UPDATE (upsert) compiles correctly.

    This verifies that the dialect inherits SQLite's upsert support.
    """
    from sqlalchemy import Column, Integer, MetaData, String, Table
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    dialect = CloudflareD1Dialect()
    metadata = MetaData()

    # Define a simple table
    test_table = Table(
        "test_table",
        metadata,
        Column("id", String, primary_key=True),
        Column("name", String),
        Column("value", Integer),
    )

    # Create an upsert statement
    stmt = sqlite_insert(test_table).values([{"id": "1", "name": "test", "value": 100}])
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={"name": stmt.excluded.name, "value": stmt.excluded.value},
    )

    # Compile the statement - this should NOT raise an error
    compiled = stmt.compile(dialect=dialect)
    sql = str(compiled)

    # Verify the SQL contains the ON CONFLICT clause
    assert "INSERT" in sql.upper()
    assert "ON CONFLICT" in sql.upper()
    assert "DO UPDATE SET" in sql.upper()


def test_sqlite_compiler_inheritance():
    """Test that compilers inherit from SQLite base classes."""
    from sqlalchemy.dialects.sqlite.base import (
        SQLiteCompiler,
        SQLiteDDLCompiler,
        SQLiteTypeCompiler,
    )
    from sqlalchemy_cloudflare_d1.compiler import (
        CloudflareD1Compiler,
        CloudflareD1DDLCompiler,
        CloudflareD1TypeCompiler,
    )

    assert issubclass(CloudflareD1Compiler, SQLiteCompiler)
    assert issubclass(CloudflareD1DDLCompiler, SQLiteDDLCompiler)
    assert issubclass(CloudflareD1TypeCompiler, SQLiteTypeCompiler)


def test_create_table_no_duplicate_primary_key():
    """Test that CREATE TABLE doesn't have duplicate PRIMARY KEY constraints."""
    from sqlalchemy import Column, MetaData, String, Table
    from sqlalchemy.schema import CreateTable

    dialect = CloudflareD1Dialect()
    metadata = MetaData()

    # Define a table with TEXT primary key
    test_table = Table(
        "test_table",
        metadata,
        Column("id", String, primary_key=True),
        Column("text", String),
        Column("namespace", String),
        Column("metadata", String),
    )

    # Compile the CREATE TABLE statement
    create_stmt = CreateTable(test_table)
    compiled = create_stmt.compile(dialect=dialect)
    sql = str(compiled)

    # Count occurrences of "PRIMARY KEY" - should be exactly 1
    pk_count = sql.upper().count("PRIMARY KEY")
    assert pk_count == 1, f"Expected 1 PRIMARY KEY, found {pk_count} in: {sql}"


def test_create_table_no_autoincrement_on_text():
    """Test that CREATE TABLE doesn't add AUTOINCREMENT on TEXT columns."""
    from sqlalchemy import Column, MetaData, String, Table
    from sqlalchemy.schema import CreateTable

    dialect = CloudflareD1Dialect()
    metadata = MetaData()

    test_table = Table(
        "test_table",
        metadata,
        Column("id", String, primary_key=True),
        Column("name", String),
    )

    create_stmt = CreateTable(test_table)
    compiled = create_stmt.compile(dialect=dialect)
    sql = str(compiled)

    # Should NOT contain AUTOINCREMENT
    assert "AUTOINCREMENT" not in sql.upper(), f"Unexpected AUTOINCREMENT in: {sql}"


def test_async_dialect_import():
    """Test that the async dialect can be imported."""
    from sqlalchemy_cloudflare_d1 import CloudflareD1Dialect_async

    assert CloudflareD1Dialect_async is not None


def test_async_dialect_instantiation():
    """Test that the async dialect can be instantiated."""
    from sqlalchemy_cloudflare_d1 import CloudflareD1Dialect_async

    dialect = CloudflareD1Dialect_async()
    assert dialect.name == "cloudflare_d1"
    assert dialect.driver == "async"
    assert dialect.is_async is True


def test_async_dialect_inherits_from_sync():
    """Test that the async dialect inherits from the sync dialect."""
    from sqlalchemy_cloudflare_d1 import CloudflareD1Dialect, CloudflareD1Dialect_async

    assert issubclass(CloudflareD1Dialect_async, CloudflareD1Dialect)


def test_async_connection_classes():
    """Test that async connection classes can be imported."""
    from sqlalchemy_cloudflare_d1 import AsyncConnection, AsyncCursor, connect_async

    assert AsyncConnection is not None
    assert AsyncCursor is not None
    assert connect_async is not None


def test_async_dialect_url_parsing():
    """Test that the async dialect can parse connection URLs."""
    from sqlalchemy.engine.url import make_url
    from sqlalchemy_cloudflare_d1 import CloudflareD1Dialect_async

    url = make_url("cloudflare_d1+async://test_account:test_token@test_database_id")
    dialect = CloudflareD1Dialect_async()

    args, kwargs = dialect.create_connect_args(url)
    assert kwargs["account_id"] == "test_account"
    assert kwargs["api_token"] == "test_token"
    assert kwargs["database_id"] == "test_database_id"


def test_async_dbapi_module():
    """Test that the async DBAPI module has required attributes."""
    from sqlalchemy_cloudflare_d1.dialect_async import AsyncAdapt_d1_dbapi

    assert AsyncAdapt_d1_dbapi.apilevel == "2.0"
    assert AsyncAdapt_d1_dbapi.threadsafety == 1
    assert AsyncAdapt_d1_dbapi.paramstyle == "qmark"
    assert hasattr(AsyncAdapt_d1_dbapi, "Error")
    assert hasattr(AsyncAdapt_d1_dbapi, "connect")


if __name__ == "__main__":
    pytest.main([__file__])
