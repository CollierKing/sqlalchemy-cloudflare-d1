"""Integration tests against a real Cloudflare D1 database.

These tests use the D1 REST API with real credentials to verify
the Connection, Cursor, and SQLAlchemy dialect work correctly.

Environment variables required:
- CF_ACCOUNT_ID: Cloudflare account ID
- TEST_CF_API_TOKEN: Cloudflare API token with D1 permissions
- CF_D1_DATABASE_ID: D1 database ID

Run with: pytest tests/test_d1_integration.py -v -s
"""

import os
import uuid

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from sqlalchemy_cloudflare_d1 import Connection


# Get credentials from environment
ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
API_TOKEN = os.environ.get("TEST_CF_API_TOKEN")
DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")

# Skip all tests if credentials not available
pytestmark = pytest.mark.skipif(
    not all([ACCOUNT_ID, API_TOKEN, DATABASE_ID]),
    reason="D1 credentials not set (CF_ACCOUNT_ID, TEST_CF_API_TOKEN, CF_D1_DATABASE_ID)",
)


@pytest.fixture
def d1_connection():
    """Create a real D1 connection."""
    conn = Connection(
        account_id=ACCOUNT_ID,
        database_id=DATABASE_ID,
        api_token=API_TOKEN,
    )
    yield conn
    conn.close()


@pytest.fixture
def test_table_name():
    """Generate a unique test table name."""
    return f"test_sqlalchemy_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def d1_engine():
    """Create a SQLAlchemy engine connected to D1."""
    url = f"cloudflare_d1://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
    engine = create_engine(url)
    yield engine
    engine.dispose()


class TestD1Connection:
    """Test direct Connection class against real D1."""

    def test_connection_can_execute_select(self, d1_connection):
        """Test basic SELECT query."""
        cursor = d1_connection.cursor()
        cursor.execute("SELECT 1 as value")
        row = cursor.fetchone()

        assert row is not None
        assert row[0] == 1

    def test_connection_can_query_sqlite_master(self, d1_connection):
        """Test querying sqlite_master for table list."""
        cursor = d1_connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        rows = cursor.fetchall()

        # Should return a list (may be empty or have tables)
        assert isinstance(rows, list)

    def test_cursor_description_populated(self, d1_connection):
        """Test cursor description is set after SELECT."""
        cursor = d1_connection.cursor()
        cursor.execute("SELECT 1 as num, 'hello' as txt")

        assert cursor.description is not None
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "num"
        assert cursor.description[1][0] == "txt"

    def test_create_insert_select_drop(self, d1_connection, test_table_name):
        """Test full CRUD cycle: CREATE, INSERT, SELECT, DROP."""
        cursor = d1_connection.cursor()

        # CREATE TABLE
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        """
        )

        # INSERT
        cursor.execute(
            f"INSERT INTO {test_table_name} (name, value) VALUES (?, ?)",
            ("test_row", 42),
        )
        assert cursor.rowcount == 1

        # SELECT
        cursor.execute(f"SELECT id, name, value FROM {test_table_name}")
        rows = cursor.fetchall()

        assert len(rows) == 1
        assert rows[0][1] == "test_row"
        assert rows[0][2] == 42

        # DROP TABLE
        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_parameterized_query(self, d1_connection, test_table_name):
        """Test parameterized queries work correctly."""
        cursor = d1_connection.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """
        )

        # Insert multiple rows with parameters
        cursor.execute(f"INSERT INTO {test_table_name} (name) VALUES (?)", ("Alice",))
        cursor.execute(f"INSERT INTO {test_table_name} (name) VALUES (?)", ("Bob",))

        # Query with parameter
        cursor.execute(f"SELECT name FROM {test_table_name} WHERE name = ?", ("Alice",))
        rows = cursor.fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "Alice"

        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")


class TestSQLAlchemyEngine:
    """Test SQLAlchemy engine against real D1."""

    def test_engine_connect_select(self, d1_engine):
        """Test SQLAlchemy engine can execute SELECT."""
        with d1_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as value"))
            row = result.fetchone()

            assert row is not None
            assert row[0] == 1

    def test_engine_get_table_names(self, d1_engine):
        """Test dialect get_table_names works."""
        with d1_engine.connect() as conn:
            # Use the dialect's get_table_names method
            dialect = d1_engine.dialect
            tables = dialect.get_table_names(conn)

            assert isinstance(tables, list)

    def test_engine_create_table_with_metadata(self, d1_engine, test_table_name):
        """Test creating a table using SQLAlchemy metadata."""
        metadata = MetaData()

        Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("score", Integer),
        )

        # Create the table
        metadata.create_all(d1_engine)

        try:
            # Verify table exists
            with d1_engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{test_table_name}'"
                    )
                )
                row = result.fetchone()
                assert row is not None
                assert row[0] == test_table_name
        finally:
            # Clean up
            metadata.drop_all(d1_engine)

    def test_engine_insert_and_select(self, d1_engine, test_table_name):
        """Test INSERT and SELECT using SQLAlchemy ORM-style."""
        metadata = MetaData()

        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert
                conn.execute(test_table.insert().values(name="SQLAlchemy Test"))
                conn.commit()

                # Select
                result = conn.execute(test_table.select())
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][1] == "SQLAlchemy Test"
        finally:
            metadata.drop_all(d1_engine)

    def test_engine_upsert_on_conflict(self, d1_engine, test_table_name):
        """Test INSERT ... ON CONFLICT DO UPDATE (upsert)."""
        metadata = MetaData()

        test_table = Table(
            test_table_name,
            metadata,
            Column("id", String, primary_key=True),
            Column("name", String(100)),
            Column("count", Integer),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # First insert
                stmt = sqlite_insert(test_table).values(
                    id="key1", name="Original", count=1
                )
                conn.execute(stmt)
                conn.commit()

                # Upsert - should update existing row
                stmt = sqlite_insert(test_table).values(
                    id="key1", name="Updated", count=2
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={"name": stmt.excluded.name, "count": stmt.excluded.count},
                )
                conn.execute(stmt)
                conn.commit()

                # Verify update happened
                result = conn.execute(
                    test_table.select().where(test_table.c.id == "key1")
                )
                row = result.fetchone()

                assert row is not None
                assert row[1] == "Updated"
                assert row[2] == 2
        finally:
            metadata.drop_all(d1_engine)


class TestAsyncConnection:
    """Test AsyncConnection against real D1."""

    @pytest.mark.asyncio
    async def test_async_connection_select(self):
        """Test async connection can execute SELECT."""
        from sqlalchemy_cloudflare_d1 import AsyncConnection

        async with AsyncConnection(
            account_id=ACCOUNT_ID,
            database_id=DATABASE_ID,
            api_token=API_TOKEN,
        ) as conn:
            cursor = await conn.cursor()
            await cursor.execute("SELECT 1 as value, 'hello' as msg")
            row = await cursor.fetchone()

            assert row is not None
            assert row[0] == 1
            assert row[1] == "hello"

    @pytest.mark.asyncio
    async def test_async_cursor_fetchall(self):
        """Test async cursor fetchall."""
        from sqlalchemy_cloudflare_d1 import AsyncConnection

        async with AsyncConnection(
            account_id=ACCOUNT_ID,
            database_id=DATABASE_ID,
            api_token=API_TOKEN,
        ) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                "SELECT 1 as n UNION SELECT 2 UNION SELECT 3 ORDER BY n"
            )
            rows = await cursor.fetchall()

            assert len(rows) == 3
            assert rows[0][0] == 1
            assert rows[1][0] == 2
            assert rows[2][0] == 3


class TestAsyncSQLAlchemyEngine:
    """Test SQLAlchemy async engine (create_async_engine) against real D1."""

    @pytest.mark.asyncio
    async def test_async_engine_select(self):
        """Test create_async_engine can execute SELECT."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)

        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 1 as value"))
                row = result.fetchone()

                assert row is not None
                assert row[0] == 1
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_engine_multiple_rows(self):
        """Test async engine can fetch multiple rows."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)

        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT 1 as n UNION SELECT 2 UNION SELECT 3 ORDER BY n")
                )
                rows = result.fetchall()

                assert len(rows) == 3
                assert rows[0][0] == 1
                assert rows[1][0] == 2
                assert rows[2][0] == 3
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_engine_create_insert_select_drop(self):
        """Test full CRUD cycle with async engine."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)
        table_name = f"test_async_{uuid.uuid4().hex[:8]}"

        try:
            async with engine.connect() as conn:
                # CREATE TABLE
                await conn.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            value INTEGER
                        )
                    """
                    )
                )
                await conn.commit()

                # INSERT
                await conn.execute(
                    text(
                        f"INSERT INTO {table_name} (name, value) VALUES (:name, :val)"
                    ),
                    {"name": "async_test", "val": 99},
                )
                await conn.commit()

                # SELECT
                result = await conn.execute(
                    text(f"SELECT id, name, value FROM {table_name}")
                )
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][1] == "async_test"
                assert rows[0][2] == 99

                # DROP TABLE
                await conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                await conn.commit()
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_engine_with_metadata(self):
        """Test async engine with SQLAlchemy metadata and Table."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)
        table_name = f"test_meta_{uuid.uuid4().hex[:8]}"

        metadata = MetaData()
        test_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )

        try:
            async with engine.begin() as conn:
                await conn.run_sync(metadata.create_all)

            async with engine.connect() as conn:
                # Insert using Table construct
                await conn.execute(test_table.insert().values(name="Metadata Test"))
                await conn.commit()

                # Select
                result = await conn.execute(test_table.select())
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][1] == "Metadata Test"

            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
