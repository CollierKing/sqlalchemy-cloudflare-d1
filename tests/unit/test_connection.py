"""
Unit tests for turning D1 /raw replies into rows.

These do not open sockets -- each connection's HTTP client is swapped for
one whose transport answers like D1's /raw endpoint.
"""

import asyncio

import httpx
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import create_async_engine

from sqlalchemy_cloudflare_d1.connection import AsyncConnection, Connection


# MARK: - Fakes


def raw_transport(columns, rows):
    """Build an httpx transport that answers every request like D1's /raw."""

    def handler(request):
        return httpx.Response(
            200,
            json={
                "success": True,
                "result": [
                    {
                        "results": {"columns": columns, "rows": rows},
                        "meta": {},
                        "success": True,
                    }
                ],
            },
        )

    return httpx.MockTransport(handler)


JOIN_SQL = "SELECT a.id, a.name, b.id FROM a JOIN b ON b.a_id = a.id"


# MARK: - Columns With The Same Name


def test_cursor_keeps_values_of_columns_with_same_name():
    """Test that columns sharing a name each keep their own value."""
    conn = Connection(account_id="acct", database_id="db", api_token="token")
    conn.client = httpx.Client(
        transport=raw_transport(["id", "name", "id"], [[1, "x", 7], [2, "y", 8]])
    )

    cursor = conn.cursor()
    cursor.execute(JOIN_SQL)

    assert [d[0] for d in cursor.description] == ["id", "name", "id"]
    assert cursor.fetchone() == (1, "x", 7)
    assert cursor.fetchall() == [(2, "y", 8)]


def test_async_cursor_keeps_values_of_columns_with_same_name():
    """Test that the async cursor keeps each value of columns sharing a name."""

    async def main():
        async with AsyncConnection(
            account_id="acct", database_id="db", api_token="token"
        ) as conn:
            conn.client = httpx.AsyncClient(
                transport=raw_transport(["id", "name", "id"], [[1, "x", 7]])
            )
            cursor = await conn.cursor()
            await cursor.execute(JOIN_SQL)
            return await cursor.fetchall()

    assert asyncio.run(main()) == [(1, "x", 7)]


def test_engine_keeps_values_of_columns_with_same_name():
    """Test that SQLAlchemy gets each value of columns sharing a name."""
    engine = create_engine("cloudflare_d1://acct:token@db")

    @event.listens_for(engine, "connect")
    def use_fake_raw(dbapi_connection, connection_record):
        dbapi_connection.client = httpx.Client(
            transport=raw_transport(["id", "name", "id"], [[1, "x", 7]])
        )

    with engine.connect() as conn:
        rows = conn.execute(text(JOIN_SQL)).fetchall()

    assert rows == [(1, "x", 7)]


def test_async_engine_keeps_values_of_columns_with_same_name():
    """Keep positional values through the async SQLAlchemy adapter, too."""
    engine = create_async_engine("cloudflare_d1+async://acct:token@db")

    @event.listens_for(engine.sync_engine, "connect")
    def use_fake_raw(dbapi_connection, connection_record):
        dbapi_connection._connection.client = httpx.AsyncClient(
            transport=raw_transport(["id", "name", "id"], [[1, "x", 7]])
        )

    async def main():
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("/* note */ " + JOIN_SQL))
                assert list(result.keys()) == ["id", "name", "id"]
                assert result.fetchall() == [(1, "x", 7)]
        finally:
            await engine.dispose()

    asyncio.run(main())


if __name__ == "__main__":
    pytest.main([__file__])
