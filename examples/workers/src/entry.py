"""
Example Python Worker using sqlalchemy-cloudflare-d1.

This Worker demonstrates the WorkerConnection class and provides
endpoints that mirror the REST API integration tests for parity.

It also demonstrates the new create_engine_from_binding() function
for using SQLAlchemy Core/ORM patterns without raw SQL.

Note: Python Workers are currently in beta.
"""

import uuid
from workers import WorkerEntrypoint, Response
from sqlalchemy_cloudflare_d1 import WorkerConnection, create_engine_from_binding


class Default(WorkerEntrypoint):
    """Default Worker entrypoint that handles HTTP requests."""

    async def fetch(self, request, env):
        """Handle incoming HTTP requests."""
        url = request.url
        path = url.split("/")[-1].split("?")[0] if "/" in url else ""

        # Core test endpoints (matching REST API tests)
        if path == "select":
            return await self.test_select()
        elif path == "sqlite-master":
            return await self.test_sqlite_master()
        elif path == "cursor-description":
            return await self.test_cursor_description()
        elif path == "crud":
            return await self.test_crud()
        elif path == "parameterized":
            return await self.test_parameterized()
        elif path == "health":
            return await self.health_check()
        # SQLAlchemy Core endpoints (no raw SQL)
        elif path == "sqlalchemy-select":
            return await self.test_sqlalchemy_select()
        elif path == "sqlalchemy-crud":
            return await self.test_sqlalchemy_crud()
        elif path == "sqlalchemy-reflect":
            return await self.test_sqlalchemy_reflect()
        else:
            return await self.index()

    def get_connection(self) -> WorkerConnection:
        """Get a WorkerConnection wrapping the D1 binding."""
        return WorkerConnection(self.env.DB)

    async def index(self):
        """Return API documentation."""
        endpoints = {
            "endpoints": {
                "/": "This help message",
                "/health": "Health check - SELECT 1",
                "/select": "Test basic SELECT query",
                "/sqlite-master": "Query sqlite_master for tables",
                "/cursor-description": "Test cursor description population",
                "/crud": "Test CREATE, INSERT, SELECT, DROP cycle",
                "/parameterized": "Test parameterized queries",
                "/sqlalchemy-select": "Test SQLAlchemy Core SELECT (no raw SQL)",
                "/sqlalchemy-crud": "Test SQLAlchemy Core CRUD (no raw SQL)",
                "/sqlalchemy-reflect": "Test SQLAlchemy table reflection",
            },
            "package": "sqlalchemy-cloudflare-d1",
            "connection_type": "WorkerConnection (D1 binding)",
        }
        return Response.json(endpoints)

    async def health_check(self):
        """Health check - mirrors REST API test_connection_can_execute_select."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async("SELECT 1 as value")
            row = cursor.fetchone()
            conn.close()

            return Response.json(
                {
                    "status": "healthy",
                    "database": "connected",
                    "value": row[0] if row else None,
                }
            )
        except Exception as e:
            return Response.json({"status": "unhealthy", "error": str(e)}, status=500)

    async def test_select(self):
        """Test basic SELECT - mirrors test_connection_can_execute_select."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async("SELECT 1 as value")
            row = cursor.fetchone()
            conn.close()

            success = row is not None and row[0] == 1
            return Response.json(
                {
                    "test": "select",
                    "success": success,
                    "row": row,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "select", "success": False, "error": str(e)}, status=500
            )

    async def test_sqlite_master(self):
        """Query sqlite_master - mirrors test_connection_can_query_sqlite_master."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            rows = cursor.fetchall()
            conn.close()

            return Response.json(
                {
                    "test": "sqlite_master",
                    "success": isinstance(rows, list),
                    "tables": [row[0] for row in rows],
                    "count": len(rows),
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "sqlite_master", "success": False, "error": str(e)}, status=500
            )

    async def test_cursor_description(self):
        """Test cursor description - mirrors test_cursor_description_populated."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async("SELECT 1 as num, 'hello' as txt")
            conn.close()

            description = cursor.description
            success = (
                description is not None
                and len(description) == 2
                and description[0][0] == "num"
                and description[1][0] == "txt"
            )

            return Response.json(
                {
                    "test": "cursor_description",
                    "success": success,
                    "description": [d[0] for d in description] if description else None,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "cursor_description", "success": False, "error": str(e)},
                status=500,
            )

    async def test_crud(self):
        """Test CRUD cycle - mirrors test_create_insert_select_drop."""
        table_name = f"test_worker_{uuid.uuid4().hex[:8]}"
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # CREATE TABLE
            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)

            # INSERT
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("test_row", 42),
            )
            insert_rowcount = cursor.rowcount

            # SELECT
            await cursor.execute_async(f"SELECT id, name, value FROM {table_name}")
            rows = cursor.fetchall()

            # DROP TABLE
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            success = (
                insert_rowcount == 1
                and len(rows) == 1
                and rows[0][1] == "test_row"
                and rows[0][2] == 42
            )

            return Response.json(
                {
                    "test": "crud",
                    "success": success,
                    "table_name": table_name,
                    "insert_rowcount": insert_rowcount,
                    "select_rows": rows,
                }
            )
        except Exception as e:
            # Try to clean up
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "crud", "success": False, "error": str(e)}, status=500
            )

    async def test_parameterized(self):
        """Test parameterized queries - mirrors test_parameterized_query."""
        table_name = f"test_param_{uuid.uuid4().hex[:8]}"
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # CREATE TABLE
            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)

            # Insert multiple rows with parameters
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name) VALUES (?)", ("Alice",)
            )
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name) VALUES (?)", ("Bob",)
            )

            # Query with parameter
            await cursor.execute_async(
                f"SELECT name FROM {table_name} WHERE name = ?", ("Alice",)
            )
            rows = cursor.fetchall()

            # DROP TABLE
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            success = len(rows) == 1 and rows[0][0] == "Alice"

            return Response.json(
                {
                    "test": "parameterized",
                    "success": success,
                    "table_name": table_name,
                    "query_result": rows,
                }
            )
        except Exception as e:
            # Try to clean up
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "parameterized", "success": False, "error": str(e)}, status=500
            )

    # ========== SQLAlchemy Core Endpoints (no raw SQL) ==========

    def get_engine(self):
        """Get a SQLAlchemy engine from the D1 binding."""
        return create_engine_from_binding(self.env.DB)

    async def test_sqlalchemy_select(self):
        """Test SQLAlchemy Core SELECT - no raw SQL.

        Demonstrates using select() with text() for simple queries.
        """
        try:
            from sqlalchemy import text

            engine = self.get_engine()

            with engine.connect() as conn:
                # Use text() for simple SELECT
                result = conn.execute(text("SELECT 1 as value"))
                row = result.fetchone()

            success = row is not None and row[0] == 1

            return Response.json(
                {
                    "test": "sqlalchemy_select",
                    "success": success,
                    "row": list(row) if row else None,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "sqlalchemy_select", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqlalchemy_crud(self):
        """Test SQLAlchemy Core CRUD - no raw SQL.

        Demonstrates using Table, MetaData, insert(), select() without raw SQL.
        """
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_sa_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            # Define table using SQLAlchemy Core
            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(50), nullable=False),
                Column("value", Integer),
            )

            # CREATE TABLE
            metadata.create_all(engine)

            with engine.connect() as conn:
                # INSERT using SQLAlchemy Core (no raw SQL)
                conn.execute(test_table.insert().values(name="test_row", value=42))
                conn.commit()

                # SELECT using SQLAlchemy Core (no raw SQL)
                result = conn.execute(select(test_table))
                rows = result.fetchall()

                # Get column names from result
                columns = list(result.keys())

            # DROP TABLE
            metadata.drop_all(engine)

            success = len(rows) == 1 and rows[0][1] == "test_row" and rows[0][2] == 42

            return Response.json(
                {
                    "test": "sqlalchemy_crud",
                    "success": success,
                    "table_name": table_name,
                    "columns": columns,
                    "rows": [list(row) for row in rows],
                }
            )
        except Exception as e:
            # Try to clean up
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "sqlalchemy_crud", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqlalchemy_reflect(self):
        """Test SQLAlchemy table reflection.

        Creates a table with raw SQL, then reflects it using SQLAlchemy
        to demonstrate autoload_with functionality.
        """
        table_name = f"test_reflect_{uuid.uuid4().hex[:8]}"

        try:
            from sqlalchemy import MetaData, Table, select

            # First create table with WorkerConnection (raw SQL)
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async(f"""
                CREATE TABLE {table_name} (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    email TEXT
                )
            """)
            await cursor.execute_async(
                f"INSERT INTO {table_name} (username, email) VALUES (?, ?)",
                ("alice", "alice@example.com"),
            )
            conn.close()

            # Now reflect the table using SQLAlchemy
            engine = self.get_engine()
            metadata = MetaData()

            # Reflect existing table (autoload_with)
            reflected_table = Table(table_name, metadata, autoload_with=engine)

            # Query using reflected table
            with engine.connect() as sa_conn:
                result = sa_conn.execute(select(reflected_table))
                rows = result.fetchall()
                columns = list(result.keys())

            # Get reflected column info
            reflected_columns = [
                {"name": col.name, "type": str(col.type)}
                for col in reflected_table.columns
            ]

            # Clean up
            raw_conn = self.get_connection()
            cursor = raw_conn.cursor()
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            raw_conn.close()

            success = (
                len(rows) == 1 and rows[0][1] == "alice" and len(reflected_columns) == 3
            )

            return Response.json(
                {
                    "test": "sqlalchemy_reflect",
                    "success": success,
                    "table_name": table_name,
                    "reflected_columns": reflected_columns,
                    "columns": columns,
                    "rows": [list(row) for row in rows],
                }
            )
        except Exception as e:
            # Try to clean up
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "sqlalchemy_reflect", "success": False, "error": str(e)},
                status=500,
            )
