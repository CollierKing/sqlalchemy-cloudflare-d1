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
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    func,
    select,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tests.test_utils import make_sqlite_method, make_sqlite_upsert_method


# Get credentials from environment
ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
API_TOKEN = os.environ.get("TEST_CF_API_TOKEN")
DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")

# Skip all tests if credentials not available
pytestmark = pytest.mark.skipif(
    not all([ACCOUNT_ID, API_TOKEN, DATABASE_ID]),
    reason="D1 credentials not set (CF_ACCOUNT_ID, TEST_CF_API_TOKEN, CF_D1_DATABASE_ID)",
)


# Note: d1_connection, d1_engine, and test_table_name fixtures are now in conftest.py


# MARK: - D1 Connection Tests (DBAPI Level)


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


# MARK: - SQL Injection Prevention Tests


class TestSQLInjectionPrevention:
    """Test that parameterized queries prevent SQL injection attacks."""

    def test_sqli_in_string_parameter(self, d1_connection, test_table_name):
        """Test SQL injection attempt in string parameter is safely escaped."""
        cursor = d1_connection.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT,
                secret TEXT
            )
        """
        )

        # Insert legitimate data
        cursor.execute(
            f"INSERT INTO {test_table_name} (name, secret) VALUES (?, ?)",
            ("alice", "secret123"),
        )
        cursor.execute(
            f"INSERT INTO {test_table_name} (name, secret) VALUES (?, ?)",
            ("bob", "secret456"),
        )

        # Attempt SQL injection via string parameter
        # This should NOT return all rows - it should look for literal "' OR '1'='1"
        malicious_input = "' OR '1'='1"
        cursor.execute(
            f"SELECT name FROM {test_table_name} WHERE name = ?", (malicious_input,)
        )
        rows = cursor.fetchall()

        # Should return 0 rows (no match), not all rows
        assert len(rows) == 0

        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_sqli_union_attack(self, d1_connection, test_table_name):
        """Test UNION-based SQL injection is prevented."""
        cursor = d1_connection.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                username TEXT
            )
        """
        )

        cursor.execute(
            f"INSERT INTO {test_table_name} (username) VALUES (?)", ("alice",)
        )

        # Attempt UNION injection to read sqlite_master
        malicious_input = "' UNION SELECT name FROM sqlite_master--"
        cursor.execute(
            f"SELECT username FROM {test_table_name} WHERE username = ?",
            (malicious_input,),
        )
        rows = cursor.fetchall()

        # Should return 0 rows, not table names from sqlite_master
        assert len(rows) == 0

        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_sqli_drop_table_attempt(self, d1_connection, test_table_name):
        """Test DROP TABLE injection is prevented."""
        cursor = d1_connection.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """
        )

        cursor.execute(f"INSERT INTO {test_table_name} (name) VALUES (?)", ("test",))

        # Attempt to drop table via injection
        malicious_input = "'; DROP TABLE " + test_table_name + ";--"
        cursor.execute(
            f"SELECT name FROM {test_table_name} WHERE name = ?", (malicious_input,)
        )
        rows = cursor.fetchall()

        # Should return 0 rows
        assert len(rows) == 0

        # Verify table still exists
        cursor.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_sqli_with_sqlalchemy_orm(self, d1_engine, test_table_name):
        """Test SQL injection prevention with SQLAlchemy ORM queries."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("username", String(100)),
            Column("password", String(100)),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert test data
                conn.execute(
                    test_table.insert().values(username="admin", password="secret")
                )
                conn.execute(
                    test_table.insert().values(username="user", password="pass123")
                )
                conn.commit()

                # Attempt SQL injection via ORM filter
                malicious_input = "admin' OR '1'='1"
                result = conn.execute(
                    select(test_table).where(test_table.c.username == malicious_input)
                )
                rows = result.fetchall()

                # Should return 0 rows, not bypass authentication
                assert len(rows) == 0

                # Verify legitimate query still works
                result = conn.execute(
                    select(test_table).where(test_table.c.username == "admin")
                )
                rows = result.fetchall()
                assert len(rows) == 1
                assert rows[0][1] == "admin"
        finally:
            metadata.drop_all(d1_engine)

    def test_sqli_in_like_clause(self, d1_engine, test_table_name):
        """Test SQL injection in LIKE clause is prevented."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", String(100)),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(test_table.insert().values(email="alice@example.com"))
                conn.execute(test_table.insert().values(email="bob@example.com"))
                conn.commit()

                # Attempt injection via LIKE pattern
                malicious_input = "%' OR '1'='1' --"
                result = conn.execute(
                    select(test_table).where(test_table.c.email.like(malicious_input))
                )
                rows = result.fetchall()

                # Should return 0 rows (literal match attempted)
                assert len(rows) == 0

                # Verify legitimate LIKE works
                result = conn.execute(
                    select(test_table).where(test_table.c.email.like("%@example.com"))
                )
                rows = result.fetchall()
                assert len(rows) == 2
        finally:
            metadata.drop_all(d1_engine)

    def test_sqli_numeric_parameter(self, d1_engine, test_table_name):
        """Test SQL injection via numeric parameter is prevented."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("data", String(100)),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(test_table.insert().values(user_id=1, data="user1_data"))
                conn.execute(test_table.insert().values(user_id=2, data="user2_data"))
                conn.commit()

                # Attempt injection - string passed where int expected
                # SQLAlchemy should handle type coercion safely
                malicious_input = "1 OR 1=1"
                result = conn.execute(
                    select(test_table).where(test_table.c.user_id == malicious_input)
                )
                rows = result.fetchall()

                # Should return 0 rows (type mismatch or literal comparison)
                assert len(rows) == 0
        finally:
            metadata.drop_all(d1_engine)


# MARK: - SQLAlchemy Engine Tests


class TestSQLAlchemyEngine:
    """Test SQLAlchemy engine against real D1."""

    def test_engine_connect_select(self, d1_engine):
        """Test SQLAlchemy engine can execute SELECT."""
        from sqlalchemy import literal_column

        with d1_engine.connect() as conn:
            # Use literal_column for simple SELECT without table
            result = conn.execute(select(literal_column("1").label("value")))
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
            # Verify table exists using dialect method
            with d1_engine.connect() as conn:
                tables = d1_engine.dialect.get_table_names(conn)
                assert test_table_name in tables
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


# MARK: - Async Connection Tests


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


# MARK: - Async SQLAlchemy Engine Tests


class TestAsyncSQLAlchemyEngine:
    """Test SQLAlchemy async engine (create_async_engine) against real D1."""

    @pytest.mark.asyncio
    async def test_async_engine_select(self):
        """Test create_async_engine can execute SELECT."""
        from sqlalchemy import literal_column
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)

        try:
            async with engine.connect() as conn:
                # Use literal_column for simple SELECT without table
                result = await conn.execute(select(literal_column("1").label("value")))
                row = result.fetchone()

                assert row is not None
                assert row[0] == 1
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_engine_multiple_rows(self):
        """Test async engine can fetch multiple rows."""
        from sqlalchemy import literal_column, union_all
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)

        try:
            async with engine.connect() as conn:
                # Use union_all for multiple literal rows
                stmt = union_all(
                    select(literal_column("1").label("n")),
                    select(literal_column("2").label("n")),
                    select(literal_column("3").label("n")),
                ).order_by("n")
                result = await conn.execute(stmt)
                rows = result.fetchall()

                assert len(rows) == 3
                assert rows[0][0] == 1
                assert rows[1][0] == 2
                assert rows[2][0] == 3
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_async_engine_create_insert_select_drop(self):
        """Test full CRUD cycle with async engine using ORM."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)
        table_name = f"test_async_{uuid.uuid4().hex[:8]}"

        metadata = MetaData()
        test_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), nullable=False),
            Column("value", Integer),
        )

        try:
            # CREATE TABLE
            async with engine.begin() as conn:
                await conn.run_sync(metadata.create_all)

            async with engine.connect() as conn:
                # INSERT
                await conn.execute(
                    test_table.insert().values(name="async_test", value=99)
                )
                await conn.commit()

                # SELECT
                result = await conn.execute(test_table.select())
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][1] == "async_test"
                assert rows[0][2] == 99

            # DROP TABLE
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
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


# MARK: - Empty Result Set Tests


class TestEmptyResultSet:
    """Test handling of empty result sets (fixes GitHub issue #4)."""

    def test_empty_result_has_description(self, d1_connection, test_table_name):
        """Test cursor.description is populated even with empty results.

        This is a regression test for GitHub issue #4:
        sqlalchemy.exc.NoSuchColumnError when query returns 0 matches.
        """
        cursor = d1_connection.cursor()

        # Create a table
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        """
        )

        # Query empty table - should not raise NoSuchColumnError
        cursor.execute(f"SELECT id, name, value FROM {test_table_name}")
        rows = cursor.fetchall()

        # Should have empty results but valid description
        assert len(rows) == 0
        assert cursor.description is not None
        assert len(cursor.description) == 3
        assert cursor.description[0][0] == "id"
        assert cursor.description[1][0] == "name"
        assert cursor.description[2][0] == "value"

        # Clean up
        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_empty_result_with_where_clause(self, d1_connection, test_table_name):
        """Test empty result from WHERE clause that matches nothing."""
        cursor = d1_connection.cursor()

        # Create and populate table
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """
        )
        cursor.execute(f"INSERT INTO {test_table_name} (name) VALUES (?)", ("Alice",))

        # Query with WHERE that matches nothing
        cursor.execute(
            f"SELECT id, name FROM {test_table_name} WHERE name = ?",
            ("NonExistent",),
        )
        rows = cursor.fetchall()

        # Should have empty results but valid description
        assert len(rows) == 0
        assert cursor.description is not None
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "id"
        assert cursor.description[1][0] == "name"

        # Clean up
        cursor.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def test_sqlalchemy_empty_result_no_error(self, d1_engine, test_table_name):
        """Test SQLAlchemy doesn't raise NoSuchColumnError on empty results."""
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
                # Query empty table using SQLAlchemy ORM-style
                result = conn.execute(test_table.select())
                rows = result.fetchall()

                # Should work without NoSuchColumnError
                assert len(rows) == 0
        finally:
            metadata.drop_all(d1_engine)

    def test_sqlalchemy_empty_result_with_filter(self, d1_engine, test_table_name):
        """Test SQLAlchemy filter returning no results doesn't error."""
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
                # Insert a row
                conn.execute(test_table.insert().values(name="Test"))
                conn.commit()

                # Query with filter that matches nothing
                result = conn.execute(
                    test_table.select().where(test_table.c.name == "NonExistent")
                )
                rows = result.fetchall()

                # Should work without NoSuchColumnError
                assert len(rows) == 0
        finally:
            metadata.drop_all(d1_engine)


# MARK: - Async Empty Result Set Tests


class TestAsyncEmptyResultSet:
    """Test async handling of empty result sets (fixes GitHub issue #4)."""

    @pytest.mark.asyncio
    async def test_async_empty_result_has_description(self):
        """Test async cursor.description is populated even with empty results."""
        from sqlalchemy_cloudflare_d1 import AsyncConnection

        table_name = f"test_async_empty_{uuid.uuid4().hex[:8]}"

        async with AsyncConnection(
            account_id=ACCOUNT_ID,
            database_id=DATABASE_ID,
            api_token=API_TOKEN,
        ) as conn:
            cursor = await conn.cursor()

            # Create a table
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """
            )

            # Query empty table
            await cursor.execute(f"SELECT id, name FROM {table_name}")
            rows = await cursor.fetchall()

            # Should have empty results but valid description
            assert len(rows) == 0
            assert cursor.description is not None
            assert len(cursor.description) == 2
            assert cursor.description[0][0] == "id"
            assert cursor.description[1][0] == "name"

            # Clean up
            await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    @pytest.mark.asyncio
    async def test_async_engine_empty_result_no_error(self):
        """Test async SQLAlchemy engine doesn't error on empty results."""
        from sqlalchemy.ext.asyncio import create_async_engine

        url = f"cloudflare_d1+async://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
        engine = create_async_engine(url)
        table_name = f"test_async_empty_{uuid.uuid4().hex[:8]}"

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
                # Query empty table using SQLAlchemy
                result = await conn.execute(test_table.select())
                rows = result.fetchall()

                # Should work without NoSuchColumnError
                assert len(rows) == 0

            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
        finally:
            await engine.dispose()


# MARK: - Pandas to_sql Tests


class TestPandasToSql:
    """Test pandas DataFrame.to_sql() with D1 engine.

    Uses shared helper methods from tests.test_utils:
    - make_sqlite_method(): For OR IGNORE/OR REPLACE conflict handling
    - make_sqlite_upsert_method(): For ON CONFLICT DO NOTHING
    """

    def test_to_sql_basic_append(self, d1_engine, test_table_name):
        """Test basic pandas to_sql with append mode."""
        import pandas as pd

        # Create test DataFrame
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "score": [85, 92, 78]})

        # Create table first
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("score", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            # Use to_sql to insert data
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Verify data was inserted
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.score).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 3
            assert rows[0] == ("Alice", 85)
            assert rows[1] == ("Bob", 92)
            assert rows[2] == ("Charlie", 78)
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_with_chunksize(self, d1_engine, test_table_name):
        """Test pandas to_sql with chunksize parameter."""
        import pandas as pd

        # Create larger DataFrame
        df = pd.DataFrame(
            {
                "name": [f"User{i}" for i in range(25)],
                "value": list(range(25)),
            }
        )

        # Create table first
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("value", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            # Use to_sql with chunksize
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                chunksize=10,
            )

            # Verify all data was inserted
            with d1_engine.connect() as conn:
                result = conn.execute(select(func.count()).select_from(test_table))
                count = result.fetchone()[0]

            assert count == 25
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_or_ignore(self, d1_engine, test_table_name):
        """Test pandas to_sql with OR IGNORE conflict handling."""
        import pandas as pd

        # Create table with unique constraint
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), unique=True),
            Column("score", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            # Insert initial data
            df1 = pd.DataFrame({"name": ["Alice", "Bob"], "score": [85, 92]})
            df1.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Try to insert with duplicate - should be ignored
            df2 = pd.DataFrame(
                {"name": ["Alice", "Charlie"], "score": [100, 78]}  # Alice is duplicate
            )
            df2.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                method=make_sqlite_method("OR IGNORE"),
            )

            # Verify: Alice should still have score 85, Charlie should be added
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.score).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 3
            assert rows[0] == ("Alice", 85)  # Original score preserved
            assert rows[1] == ("Bob", 92)
            assert rows[2] == ("Charlie", 78)
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_or_replace(self, d1_engine, test_table_name):
        """Test pandas to_sql with OR REPLACE conflict handling."""
        import pandas as pd

        # Create table with unique constraint
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), unique=True),
            Column("score", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            # Insert initial data
            df1 = pd.DataFrame({"name": ["Alice", "Bob"], "score": [85, 92]})
            df1.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Insert with duplicate - should replace
            df2 = pd.DataFrame(
                {"name": ["Alice", "Charlie"], "score": [100, 78]}  # Alice is duplicate
            )
            df2.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                method=make_sqlite_method("OR REPLACE"),
            )

            # Verify: Alice should have new score 100
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.score).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 3
            assert rows[0] == ("Alice", 100)  # Score replaced
            assert rows[1] == ("Bob", 92)
            assert rows[2] == ("Charlie", 78)
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_upsert_on_conflict_do_nothing(self, d1_engine, test_table_name):
        """Test pandas to_sql with ON CONFLICT DO NOTHING upsert method."""
        import pandas as pd

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("score", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            # Insert initial data with explicit IDs
            df1 = pd.DataFrame(
                {"id": [1, 2], "name": ["Alice", "Bob"], "score": [85, 92]}
            )
            df1.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Try to insert with conflicting id - should do nothing
            df2 = pd.DataFrame(
                {
                    "id": [1, 3],  # id=1 conflicts
                    "name": ["Alice Updated", "Charlie"],
                    "score": [100, 78],
                }
            )
            df2.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                method=make_sqlite_upsert_method(conflict_target=("id",)),
            )

            # Verify: id=1 should be unchanged, id=3 should be added
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(
                        test_table.c.id, test_table.c.name, test_table.c.score
                    ).order_by(test_table.c.id)
                )
                rows = result.fetchall()

            assert len(rows) == 3
            assert rows[0] == (1, "Alice", 85)  # Unchanged
            assert rows[1] == (2, "Bob", 92)
            assert rows[2] == (3, "Charlie", 78)  # New row added
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_empty_dataframe(self, d1_engine, test_table_name):
        """Test pandas to_sql with empty DataFrame doesn't error."""
        import pandas as pd

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
        )
        metadata.create_all(d1_engine)

        try:
            # Create empty DataFrame with correct columns
            df = pd.DataFrame({"name": []})

            # Should not raise an error
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                method=make_sqlite_method("OR IGNORE"),
            )

            # Verify table is still empty
            with d1_engine.connect() as conn:
                result = conn.execute(select(func.count()).select_from(test_table))
                count = result.fetchone()[0]

            assert count == 0
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_with_json_column(self, d1_engine, test_table_name):
        """Test pandas to_sql with stringified JSON column."""
        import json

        import pandas as pd

        # Create DataFrame with JSON data stored as strings
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "metadata": [
                    json.dumps({"role": "admin", "tags": ["active", "verified"]}),
                    json.dumps({"role": "user", "tags": ["new"]}),
                ],
            }
        )

        # Create table with TEXT column for JSON
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("metadata", String),  # JSON stored as TEXT
        )
        metadata.create_all(d1_engine)

        try:
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Verify data was inserted and JSON is valid
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.metadata).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 2
            assert rows[0][0] == "Alice"
            alice_meta = json.loads(rows[0][1])
            assert alice_meta["role"] == "admin"
            assert "active" in alice_meta["tags"]

            assert rows[1][0] == "Bob"
            bob_meta = json.loads(rows[1][1])
            assert bob_meta["role"] == "user"
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_upsert_with_json_column(self, d1_engine, test_table_name):
        """Test pandas to_sql upsert with stringified JSON column."""
        import json

        import pandas as pd

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), unique=True),
            Column("config", String),  # JSON stored as TEXT
        )
        metadata.create_all(d1_engine)

        try:
            # Insert initial data with JSON
            df1 = pd.DataFrame(
                {
                    "name": ["service_a", "service_b"],
                    "config": [
                        json.dumps({"enabled": True, "retries": 3}),
                        json.dumps({"enabled": False, "retries": 1}),
                    ],
                }
            )
            df1.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Upsert with updated JSON - service_a should be replaced
            df2 = pd.DataFrame(
                {
                    "name": ["service_a", "service_c"],
                    "config": [
                        json.dumps({"enabled": False, "retries": 5, "timeout": 30}),
                        json.dumps({"enabled": True, "retries": 2}),
                    ],
                }
            )
            df2.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
                method=make_sqlite_method("OR REPLACE"),
            )

            # Verify
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.config).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 3

            # service_a should have updated config
            assert rows[0][0] == "service_a"
            config_a = json.loads(rows[0][1])
            assert config_a["enabled"] is False
            assert config_a["retries"] == 5
            assert config_a["timeout"] == 30

            # service_b unchanged
            assert rows[1][0] == "service_b"
            config_b = json.loads(rows[1][1])
            assert config_b["enabled"] is False
            assert config_b["retries"] == 1

            # service_c is new
            assert rows[2][0] == "service_c"
            config_c = json.loads(rows[2][1])
            assert config_c["enabled"] is True
        finally:
            metadata.drop_all(d1_engine)

    def test_to_sql_with_nested_json(self, d1_engine, test_table_name):
        """Test pandas to_sql with deeply nested JSON structures."""
        import json

        import pandas as pd

        # Create DataFrame with complex nested JSON
        df = pd.DataFrame(
            {
                "doc_id": ["doc1", "doc2"],
                "content": [
                    json.dumps(
                        {
                            "title": "Document 1",
                            "sections": [
                                {"heading": "Intro", "paragraphs": ["p1", "p2"]},
                                {"heading": "Body", "paragraphs": ["p3"]},
                            ],
                            "metadata": {
                                "author": {
                                    "name": "Alice",
                                    "email": "alice@example.com",
                                },
                                "tags": ["draft", "review"],
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "title": "Document 2",
                            "sections": [],
                            "metadata": {"author": {"name": "Bob"}, "tags": []},
                        }
                    ),
                ],
            }
        )

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("doc_id", String(50), unique=True),
            Column("content", String),  # JSON stored as TEXT
        )
        metadata.create_all(d1_engine)

        try:
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Verify nested JSON is preserved
            with d1_engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.doc_id, test_table.c.content).order_by(
                        test_table.c.doc_id
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 2

            # Verify doc1 nested structure
            doc1 = json.loads(rows[0][1])
            assert doc1["title"] == "Document 1"
            assert len(doc1["sections"]) == 2
            assert doc1["sections"][0]["heading"] == "Intro"
            assert doc1["metadata"]["author"]["name"] == "Alice"
            assert "draft" in doc1["metadata"]["tags"]

            # Verify doc2
            doc2 = json.loads(rows[1][1])
            assert doc2["title"] == "Document 2"
            assert len(doc2["sections"]) == 0
        finally:
            metadata.drop_all(d1_engine)


# MARK: - JSON Column Filtering Tests


class TestJsonColumnFiltering:
    """Test filtering on JSON array columns using json_each and exists."""

    def test_json_array_filter_with_exists(self, d1_engine, test_table_name):
        """Test filtering rows where JSON array contains a specific value."""
        import json

        from sqlalchemy import exists, func, select

        # Create table with JSON array column
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("tags", String),  # JSON array stored as TEXT
        )
        metadata.create_all(d1_engine)

        try:
            # Insert test data with JSON arrays
            with d1_engine.connect() as conn:
                conn.execute(
                    test_table.insert().values(
                        name="Alice",
                        tags=json.dumps(["python", "sqlalchemy", "d1"]),
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        name="Bob",
                        tags=json.dumps(["javascript", "react"]),
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        name="Charlie",
                        tags=json.dumps(["python", "fastapi"]),
                    )
                )
                conn.commit()

            # Query: find rows where tags contains "python"
            with d1_engine.connect() as conn:
                # Use json_each to expand the JSON array and check for value
                je = func.json_each(test_table.c.tags).table_valued("value").alias("je")
                stmt = (
                    select(test_table.c.name, test_table.c.tags)
                    .where(
                        exists(
                            select(1)
                            .select_from(je)
                            .where(func.lower(je.c.value) == "python")
                        )
                    )
                    .order_by(test_table.c.name)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            assert len(rows) == 2
            assert rows[0][0] == "Alice"
            assert rows[1][0] == "Charlie"

        finally:
            metadata.drop_all(d1_engine)

    def test_json_array_filter_multiple_values(self, d1_engine, test_table_name):
        """Test filtering rows where JSON array contains any of multiple values."""
        import json

        from sqlalchemy import exists, func, select

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("product", String(100)),
            Column("categories", String),  # JSON array
        )
        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(
                    test_table.insert().values(
                        product="Widget A",
                        categories=json.dumps(["electronics", "gadgets"]),
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        product="Widget B",
                        categories=json.dumps(["home", "kitchen"]),
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        product="Widget C",
                        categories=json.dumps(["electronics", "office"]),
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        product="Widget D",
                        categories=json.dumps(["sports", "outdoor"]),
                    )
                )
                conn.commit()

            # Query: find products in "electronics" OR "home" categories
            with d1_engine.connect() as conn:
                je = (
                    func.json_each(test_table.c.categories)
                    .table_valued("value")
                    .alias("je")
                )
                stmt = (
                    select(test_table.c.product)
                    .where(
                        exists(
                            select(1)
                            .select_from(je)
                            .where(func.lower(je.c.value).in_(["electronics", "home"]))
                        )
                    )
                    .order_by(test_table.c.product)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            assert len(rows) == 3
            assert rows[0][0] == "Widget A"
            assert rows[1][0] == "Widget B"
            assert rows[2][0] == "Widget C"

        finally:
            metadata.drop_all(d1_engine)

    def test_json_array_expand_with_join(self, d1_engine, test_table_name):
        """Test expanding JSON array and joining for grouping/aggregation."""
        import json

        from sqlalchemy import func, select, true

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("post_id", String(50)),
            Column("tags", String),  # JSON array
            Column("score", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(
                    test_table.insert().values(
                        post_id="p1",
                        tags=json.dumps(["tech", "python"]),
                        score=10,
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        post_id="p2",
                        tags=json.dumps(["tech", "javascript"]),
                        score=20,
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        post_id="p3",
                        tags=json.dumps(["python", "data"]),
                        score=15,
                    )
                )
                conn.commit()

            # Query: aggregate scores by tag (expand JSON array)
            with d1_engine.connect() as conn:
                je = func.json_each(test_table.c.tags).table_valued("value").alias("je")

                stmt = (
                    select(
                        je.c.value.label("tag"),
                        func.sum(test_table.c.score).label("total_score"),
                        func.count().label("post_count"),
                    )
                    .select_from(test_table.join(je, true()))
                    .group_by(je.c.value)
                    .order_by(je.c.value)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            # Expected:
            # data: 15 (p3)
            # javascript: 20 (p2)
            # python: 25 (p1 + p3)
            # tech: 30 (p1 + p2)
            assert len(rows) == 4
            tag_scores = {row[0]: row[1] for row in rows}
            assert tag_scores["data"] == 15
            assert tag_scores["javascript"] == 20
            assert tag_scores["python"] == 25
            assert tag_scores["tech"] == 30

        finally:
            metadata.drop_all(d1_engine)

    def test_pandas_to_sql_with_json_filter(self, d1_engine, test_table_name):
        """Test inserting with pandas then filtering on JSON column."""
        import json

        import pandas as pd
        from sqlalchemy import exists, func, select

        # Create table
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("src", String(50)),
            Column("doc_id", String(50)),
            Column("products", String),  # JSON array
        )
        metadata.create_all(d1_engine)

        try:
            # Insert data using pandas
            df = pd.DataFrame(
                {
                    "src": ["twitter", "reddit", "twitter", "linkedin"],
                    "doc_id": ["d1", "d2", "d3", "d4"],
                    "products": [
                        json.dumps(["product_a", "product_b"]),
                        json.dumps(["product_b", "product_c"]),
                        json.dumps(["product_a"]),
                        json.dumps(["product_c", "product_d"]),
                    ],
                }
            )
            df.to_sql(
                test_table_name,
                con=d1_engine,
                if_exists="append",
                index=False,
            )

            # Query: find all docs containing "product_a"
            with d1_engine.connect() as conn:
                je = (
                    func.json_each(test_table.c.products)
                    .table_valued("value")
                    .alias("je")
                )
                stmt = (
                    select(test_table.c.src, test_table.c.doc_id)
                    .where(
                        exists(
                            select(1)
                            .select_from(je)
                            .where(func.lower(je.c.value) == "product_a")
                        )
                    )
                    .order_by(test_table.c.doc_id)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            assert len(rows) == 2
            assert rows[0] == ("twitter", "d1")
            assert rows[1] == ("twitter", "d3")

            # Query: count docs per source where product is "product_b" or "product_c"
            with d1_engine.connect() as conn:
                je = (
                    func.json_each(test_table.c.products)
                    .table_valued("value")
                    .alias("je")
                )
                stmt = (
                    select(
                        test_table.c.src,
                        func.count(func.distinct(test_table.c.doc_id)).label(
                            "doc_count"
                        ),
                    )
                    .where(
                        exists(
                            select(1)
                            .select_from(je)
                            .where(
                                func.lower(je.c.value).in_(["product_b", "product_c"])
                            )
                        )
                    )
                    .group_by(test_table.c.src)
                    .order_by(test_table.c.src)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            # Expected: linkedin: 1, reddit: 1, twitter: 1 (d1 has product_b)
            assert len(rows) == 3
            src_counts = {row[0]: row[1] for row in rows}
            assert src_counts["linkedin"] == 1
            assert src_counts["reddit"] == 1
            assert src_counts["twitter"] == 1

        finally:
            metadata.drop_all(d1_engine)


# MARK: - Boolean Column Tests


class TestBooleanColumn:
    """Test boolean column handling (fixes GitHub issue #6).

    D1/SQLite stores booleans as integers (0/1) or strings ("true"/"false").
    The D1Boolean type processor converts these back to Python booleans.
    """

    def test_boolean_column_returns_python_bool(self, d1_engine, test_table_name):
        """Test that boolean columns return Python bool, not str or int."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("username", String(100)),
            Column("is_admin", Boolean),
            Column("is_active", Boolean),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert users with boolean values
                conn.execute(
                    test_table.insert().values(
                        username="admin", is_admin=True, is_active=True
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        username="user", is_admin=False, is_active=True
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        username="inactive", is_admin=False, is_active=False
                    )
                )
                conn.commit()

                # Query and verify types
                result = conn.execute(
                    select(
                        test_table.c.username,
                        test_table.c.is_admin,
                        test_table.c.is_active,
                    ).order_by(test_table.c.username)
                )
                rows = result.fetchall()

            assert len(rows) == 3

            # Check admin user
            assert rows[0][0] == "admin"
            assert rows[0][1] is True
            assert rows[0][2] is True
            assert isinstance(rows[0][1], bool)
            assert isinstance(rows[0][2], bool)

            # Check inactive user
            assert rows[1][0] == "inactive"
            assert rows[1][1] is False
            assert rows[1][2] is False
            assert isinstance(rows[1][1], bool)
            assert isinstance(rows[1][2], bool)

            # Check regular user
            assert rows[2][0] == "user"
            assert rows[2][1] is False
            assert rows[2][2] is True
            assert isinstance(rows[2][1], bool)
            assert isinstance(rows[2][2], bool)

        finally:
            metadata.drop_all(d1_engine)

    def test_boolean_filter_with_python_bool(self, d1_engine, test_table_name):
        """Test filtering by boolean values works correctly."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("enabled", Boolean),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(test_table.insert().values(name="Feature A", enabled=True))
                conn.execute(
                    test_table.insert().values(name="Feature B", enabled=False)
                )
                conn.execute(test_table.insert().values(name="Feature C", enabled=True))
                conn.commit()

                # Filter for enabled=True
                result = conn.execute(
                    select(test_table.c.name)
                    .where(test_table.c.enabled == True)  # noqa: E712
                    .order_by(test_table.c.name)
                )
                enabled_rows = result.fetchall()

                # Filter for enabled=False
                result = conn.execute(
                    select(test_table.c.name).where(
                        test_table.c.enabled == False  # noqa: E712
                    )
                )
                disabled_rows = result.fetchall()

            assert len(enabled_rows) == 2
            assert enabled_rows[0][0] == "Feature A"
            assert enabled_rows[1][0] == "Feature C"

            assert len(disabled_rows) == 1
            assert disabled_rows[0][0] == "Feature B"

        finally:
            metadata.drop_all(d1_engine)

    def test_boolean_nullable_column(self, d1_engine, test_table_name):
        """Test nullable boolean columns handle NULL correctly."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("verified", Boolean, nullable=True),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(test_table.insert().values(name="User A", verified=True))
                conn.execute(test_table.insert().values(name="User B", verified=False))
                conn.execute(
                    test_table.insert().values(name="User C", verified=None)
                )  # NULL
                conn.commit()

                result = conn.execute(
                    select(test_table.c.name, test_table.c.verified).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 3

            # User A - verified=True
            assert rows[0][0] == "User A"
            assert rows[0][1] is True
            assert isinstance(rows[0][1], bool)

            # User B - verified=False
            assert rows[1][0] == "User B"
            assert rows[1][1] is False
            assert isinstance(rows[1][1], bool)

            # User C - verified=NULL
            assert rows[2][0] == "User C"
            assert rows[2][1] is None

        finally:
            metadata.drop_all(d1_engine)

    def test_boolean_update(self, d1_engine, test_table_name):
        """Test updating boolean values works correctly."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("active", Boolean),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert with active=True
                conn.execute(test_table.insert().values(name="Test", active=True))
                conn.commit()

                # Verify initial value
                result = conn.execute(
                    select(test_table.c.active).where(test_table.c.name == "Test")
                )
                row = result.fetchone()
                assert row[0] is True
                assert isinstance(row[0], bool)

                # Update to active=False
                conn.execute(
                    test_table.update()
                    .where(test_table.c.name == "Test")
                    .values(active=False)
                )
                conn.commit()

                # Verify updated value
                result = conn.execute(
                    select(test_table.c.active).where(test_table.c.name == "Test")
                )
                row = result.fetchone()
                assert row[0] is False
                assert isinstance(row[0], bool)

        finally:
            metadata.drop_all(d1_engine)


# MARK: - LargeBinary Column Tests


class TestLargeBinaryColumn:
    """Test LargeBinary column handling (fixes GitHub issue #8).

    D1 stores binary data as BLOB. The D1LargeBinary type processor handles
    base64 decoding when reading data back from D1.
    """

    def test_largebinary_column_stores_and_retrieves(self, d1_engine, test_table_name):
        """Test that LargeBinary columns can store and retrieve binary data."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("data", LargeBinary),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert binary data
                binary_data = b"\x00\x01\x02\x03\xff\xfe\xfd"
                conn.execute(
                    test_table.insert().values(name="test_file", data=binary_data)
                )
                conn.commit()

                # Retrieve and verify
                result = conn.execute(
                    select(test_table).where(test_table.c.name == "test_file")
                )
                row = result.fetchone()

                assert row is not None
                assert isinstance(row[2], bytes)
                assert row[2] == binary_data
        finally:
            metadata.drop_all(d1_engine)

    def test_largebinary_with_image_data(self, d1_engine, test_table_name):
        """Test storing simulated image data (PNG header)."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("filename", String(100)),
            Column("image_data", LargeBinary),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Simulate PNG header + some data
                png_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100

                conn.execute(
                    test_table.insert().values(filename="test.png", image_data=png_data)
                )
                conn.commit()

                # Retrieve
                result = conn.execute(
                    select(test_table).where(test_table.c.filename == "test.png")
                )
                row = result.fetchone()

                assert row is not None
                assert row[2] == png_data
                assert row[2][:8] == b"\x89PNG\r\n\x1a\n"
        finally:
            metadata.drop_all(d1_engine)

    def test_largebinary_nullable(self, d1_engine, test_table_name):
        """Test nullable LargeBinary columns handle NULL correctly."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("data", LargeBinary, nullable=True),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert with NULL
                conn.execute(test_table.insert().values(name="no_data", data=None))
                # Insert with data
                conn.execute(
                    test_table.insert().values(name="has_data", data=b"\xab\xcd")
                )
                conn.commit()

                # Verify
                result = conn.execute(select(test_table).order_by(test_table.c.id))
                rows = result.fetchall()

                assert len(rows) == 2
                assert rows[0][2] is None
                assert rows[1][2] == b"\xab\xcd"
        finally:
            metadata.drop_all(d1_engine)

    def test_largebinary_large_payload(self, d1_engine, test_table_name):
        """Test storing larger binary payloads."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("blob_data", LargeBinary),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Create a larger binary payload (10KB of random-ish data)
                large_data = bytes(range(256)) * 40  # 10,240 bytes

                conn.execute(
                    test_table.insert().values(name="large_blob", blob_data=large_data)
                )
                conn.commit()

                # Retrieve and verify
                result = conn.execute(
                    select(test_table).where(test_table.c.name == "large_blob")
                )
                row = result.fetchone()

                assert row is not None
                assert isinstance(row[2], bytes)
                assert len(row[2]) == 10240
                assert row[2] == large_data
        finally:
            metadata.drop_all(d1_engine)


# MARK: - ON CONFLICT Advanced Tests


class TestOnConflictAdvanced:
    """Test advanced ON CONFLICT clause variations (GitHub issue #9).

    Note: Basic ON CONFLICT support was added in v0.3.0. These tests cover
    additional edge cases and usage patterns.
    """

    def test_on_conflict_do_nothing(self, d1_engine, test_table_name):
        """Test INSERT ... ON CONFLICT DO NOTHING."""
        metadata = MetaData()

        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100), unique=True),
            Column("count", Integer),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # First insert
                stmt = sqlite_insert(test_table).values(
                    id=1, name="unique_name", count=10
                )
                conn.execute(stmt)
                conn.commit()

                # Try to insert duplicate - should do nothing
                stmt = sqlite_insert(test_table).values(
                    id=2, name="unique_name", count=20
                )
                stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
                conn.execute(stmt)
                conn.commit()

                # Verify only first row exists with original values
                result = conn.execute(select(test_table))
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][0] == 1  # id
                assert rows[0][1] == "unique_name"
                assert rows[0][2] == 10  # count unchanged
        finally:
            metadata.drop_all(d1_engine)

    def test_on_conflict_composite_key(self, d1_engine, test_table_name):
        """Test ON CONFLICT with composite unique constraint."""
        from sqlalchemy import UniqueConstraint

        metadata = MetaData()

        test_table = Table(
            test_table_name,
            metadata,
            Column("user_id", Integer),
            Column("resource_id", Integer),
            Column("access_level", String(50)),
            Column("granted_at", String(50)),
            UniqueConstraint("user_id", "resource_id", name="unique_user_resource"),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert permission
                stmt = sqlite_insert(test_table).values(
                    user_id=1,
                    resource_id=100,
                    access_level="read",
                    granted_at="2024-01-01",
                )
                conn.execute(stmt)
                conn.commit()

                # Update permission with upsert
                stmt = sqlite_insert(test_table).values(
                    user_id=1,
                    resource_id=100,
                    access_level="write",
                    granted_at="2024-01-02",
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["user_id", "resource_id"],
                    set_={
                        "access_level": stmt.excluded.access_level,
                        "granted_at": stmt.excluded.granted_at,
                    },
                )
                conn.execute(stmt)
                conn.commit()

                # Verify update
                result = conn.execute(select(test_table))
                rows = result.fetchall()

                assert len(rows) == 1
                assert rows[0][2] == "write"  # access_level updated
                assert rows[0][3] == "2024-01-02"  # granted_at updated
        finally:
            metadata.drop_all(d1_engine)

    def test_on_conflict_with_where_clause(self, d1_engine, test_table_name):
        """Test ON CONFLICT with WHERE clause (conditional update)."""
        metadata = MetaData()

        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", String(100), unique=True),
            Column("is_verified", Boolean),
            Column("updated_at", String(50)),
        )

        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                # Insert unverified email
                stmt = sqlite_insert(test_table).values(
                    email="test@example.com", is_verified=False, updated_at="2024-01-01"
                )
                conn.execute(stmt)
                conn.commit()

                # Upsert with WHERE clause - only update if not verified
                stmt = sqlite_insert(test_table).values(
                    email="test@example.com", is_verified=True, updated_at="2024-01-02"
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["email"],
                    set_={
                        "is_verified": stmt.excluded.is_verified,
                        "updated_at": stmt.excluded.updated_at,
                    },
                    where=(test_table.c.is_verified == False),  # noqa: E712
                )
                conn.execute(stmt)
                conn.commit()

                # Verify update happened
                result = conn.execute(
                    select(test_table).where(test_table.c.email == "test@example.com")
                )
                row = result.fetchone()

                assert row is not None
                assert row[2] is True  # is_verified
                assert row[3] == "2024-01-02"
        finally:
            metadata.drop_all(d1_engine)


# MARK: - Single-Row Result Tests


class TestSingleRowResult:
    """Test that single-row query results are returned correctly.

    Regression tests for bug where single-row results end up in
    cursor.description instead of being returned by fetchall().
    """

    def test_single_row_via_cursor(self, d1_connection):
        """Test single-row SELECT returns the row via DBAPI cursor."""
        cursor = d1_connection.cursor()
        table_name = f"test_single_{__import__('uuid').uuid4().hex[:8]}"

        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"(id INTEGER PRIMARY KEY, name TEXT NOT NULL, value INTEGER)"
            )
            cursor.execute(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("only_row", 99),
            )

            cursor.execute(f"SELECT id, name, value FROM {table_name}")
            description = cursor.description
            rows = cursor.fetchall()

            # Description should have column names
            desc_names = [d[0] for d in description] if description else []
            assert desc_names == ["id", "name", "value"]

            # fetchall should return the single row
            assert len(rows) == 1
            assert rows[0][1] == "only_row"
            assert rows[0][2] == 99
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    def test_single_row_via_sqlalchemy(self, d1_engine, test_table_name):
        """Test single-row result via SQLAlchemy engine."""
        metadata = MetaData()
        test_table = Table(
            test_table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("value", Integer),
        )
        metadata.create_all(d1_engine)

        try:
            with d1_engine.connect() as conn:
                conn.execute(test_table.insert().values(name="only_row", value=99))
                conn.commit()

                result = conn.execute(select(test_table))
                rows = result.fetchall()
                columns = list(result.keys())

            assert len(rows) == 1
            assert rows[0][1] == "only_row"
            assert rows[0][2] == 99
            assert columns == ["id", "name", "value"]
        finally:
            metadata.drop_all(d1_engine)

    def test_multi_row_description_has_column_names(self, d1_connection):
        """Test multi-row SELECT has correct column names in description."""
        cursor = d1_connection.cursor()
        table_name = f"test_multi_{__import__('uuid').uuid4().hex[:8]}"

        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"(id INTEGER PRIMARY KEY, name TEXT NOT NULL, value INTEGER)"
            )
            for name, val in [("row_one", 10), ("row_two", 20), ("row_three", 30)]:
                cursor.execute(
                    f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                    (name, val),
                )

            cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
            description = cursor.description
            rows = cursor.fetchall()

            desc_names = [d[0] for d in description] if description else []
            assert desc_names == ["id", "name", "value"]
            assert len(rows) == 3
            assert rows[0][1] == "row_one"
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
