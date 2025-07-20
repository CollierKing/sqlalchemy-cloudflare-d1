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


if __name__ == "__main__":
    pytest.main([__file__])
