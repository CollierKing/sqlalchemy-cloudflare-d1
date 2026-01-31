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
        # Empty result set tests (GitHub issue #4)
        elif path == "empty-result":
            return await self.test_empty_result()
        elif path == "empty-result-sqlalchemy":
            return await self.test_empty_result_sqlalchemy()
        # JSON column filtering tests
        elif path == "json-filter":
            return await self.test_json_filter()
        elif path == "json-aggregate":
            return await self.test_json_aggregate()
        # Pandas to_sql tests
        elif path == "pandas-to-sql":
            return await self.test_pandas_to_sql()
        elif path == "pandas-to-sql-upsert":
            return await self.test_pandas_to_sql_upsert()
        elif path == "pandas-to-sql-json":
            return await self.test_pandas_to_sql_json()
        # SQL injection prevention tests
        elif path == "sqli-string":
            return await self.test_sqli_string()
        elif path == "sqli-union":
            return await self.test_sqli_union()
        elif path == "sqli-drop":
            return await self.test_sqli_drop()
        elif path == "sqli-orm":
            return await self.test_sqli_orm()
        elif path == "sqli-like":
            return await self.test_sqli_like()
        # Additional SQLAlchemy tests
        elif path == "sqlalchemy-upsert":
            return await self.test_sqlalchemy_upsert()
        elif path == "sqlalchemy-get-tables":
            return await self.test_sqlalchemy_get_tables()
        # Additional empty result tests
        elif path == "empty-result-where":
            return await self.test_empty_result_where()
        # Additional JSON tests
        elif path == "json-multiple-values":
            return await self.test_json_multiple_values()
        # Boolean column tests
        elif path == "boolean-column":
            return await self.test_boolean_column()
        elif path == "boolean-filter":
            return await self.test_boolean_filter()
        elif path == "boolean-nullable":
            return await self.test_boolean_nullable()
        # NULL parameter tests
        elif path == "null-string":
            return await self.test_null_string()
        elif path == "null-integer":
            return await self.test_null_integer()
        # LargeBinary column tests
        elif path == "largebinary-basic":
            return await self.test_largebinary_basic()
        elif path == "largebinary-image":
            return await self.test_largebinary_image()
        elif path == "largebinary-nullable":
            return await self.test_largebinary_nullable()
        # ON CONFLICT advanced tests
        elif path == "on-conflict-do-nothing":
            return await self.test_on_conflict_do_nothing()
        elif path == "on-conflict-composite":
            return await self.test_on_conflict_composite()
        elif path == "on-conflict-where":
            return await self.test_on_conflict_where()
        # Single-row result tests (bug: single-row results lost in description)
        elif path == "single-row-result":
            return await self.test_single_row_result()
        elif path == "single-row-sqlalchemy":
            return await self.test_single_row_sqlalchemy()
        elif path == "multi-row-result":
            return await self.test_multi_row_result()
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
                "/empty-result": "Test empty result set description (issue #4)",
                "/empty-result-sqlalchemy": "Test SQLAlchemy empty result (issue #4)",
                "/json-filter": "Test filtering on JSON array columns",
                "/json-aggregate": "Test aggregation on JSON array columns",
                "/pandas-to-sql": "Test pandas DataFrame.to_sql()",
                "/pandas-to-sql-upsert": "Test pandas to_sql with OR REPLACE",
                "/pandas-to-sql-json": "Test pandas to_sql with JSON columns",
                "/sqli-string": "Test SQL injection prevention (string param)",
                "/sqli-union": "Test SQL injection prevention (UNION attack)",
                "/sqli-drop": "Test SQL injection prevention (DROP TABLE)",
                "/sqli-orm": "Test SQL injection prevention (ORM filter)",
                "/sqli-like": "Test SQL injection prevention (LIKE clause)",
                "/sqlalchemy-upsert": "Test SQLAlchemy ON CONFLICT upsert",
                "/sqlalchemy-get-tables": "Test dialect get_table_names()",
                "/empty-result-where": "Test empty result with WHERE clause",
                "/json-multiple-values": "Test JSON filter with multiple values",
                "/boolean-column": "Test boolean column returns Python bool",
                "/boolean-filter": "Test filtering by boolean values",
                "/boolean-nullable": "Test nullable boolean columns",
                "/null-string": "Test NULL parameter with String column",
                "/null-integer": "Test NULL parameter with Integer column",
                "/largebinary-basic": "Test LargeBinary column basic usage",
                "/largebinary-image": "Test LargeBinary with image data",
                "/largebinary-nullable": "Test nullable LargeBinary columns",
                "/on-conflict-do-nothing": "Test ON CONFLICT DO NOTHING",
                "/on-conflict-composite": "Test ON CONFLICT with composite key",
                "/on-conflict-where": "Test ON CONFLICT with WHERE clause",
                "/single-row-result": "Test single-row SELECT returns data correctly",
                "/single-row-sqlalchemy": "Test single-row via SQLAlchemy engine",
                "/multi-row-result": "Test multi-row SELECT returns correct data",
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

    # MARK: - Empty Result Set Tests (GitHub issue #4)

    async def test_empty_result(self):
        """Test cursor.description is populated even with empty results.

        Regression test for GitHub issue #4.
        """
        table_name = f"test_empty_{uuid.uuid4().hex[:8]}"

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

            # Query empty table - should not raise error
            await cursor.execute_async(f"SELECT id, name, value FROM {table_name}")
            rows = cursor.fetchall()

            # Capture description before cleanup
            description = cursor.description

            # DROP TABLE
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            # Verify empty results with valid description
            success = (
                len(rows) == 0
                and description is not None
                and len(description) == 3
                and description[0][0] == "id"
                and description[1][0] == "name"
                and description[2][0] == "value"
            )

            return Response.json(
                {
                    "test": "empty_result",
                    "success": success,
                    "row_count": len(rows),
                    "description": [d[0] for d in description] if description else None,
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
                {"test": "empty_result", "success": False, "error": str(e)},
                status=500,
            )

    async def test_empty_result_sqlalchemy(self):
        """Test SQLAlchemy doesn't raise NoSuchColumnError on empty results.

        Regression test for GitHub issue #4.
        """
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_sa_empty_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
            )

            # CREATE TABLE
            metadata.create_all(engine)

            with engine.connect() as conn:
                # Query empty table - should not raise NoSuchColumnError
                result = conn.execute(select(test_table))
                rows = result.fetchall()

            # DROP TABLE
            metadata.drop_all(engine)

            success = len(rows) == 0

            return Response.json(
                {
                    "test": "empty_result_sqlalchemy",
                    "success": success,
                    "row_count": len(rows),
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
                {"test": "empty_result_sqlalchemy", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - JSON Column Filtering Tests

    async def test_json_filter(self):
        """Test filtering rows where JSON array contains a specific value."""
        import json
        from sqlalchemy import (
            MetaData,
            Table,
            Column,
            Integer,
            String,
            select,
            exists,
            func,
        )

        table_name = f"test_json_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("tags", String),  # JSON array stored as TEXT
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                # Insert test data with JSON arrays
                conn.execute(
                    test_table.insert().values(
                        name="Alice", tags=json.dumps(["python", "sqlalchemy"])
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        name="Bob", tags=json.dumps(["javascript", "react"])
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        name="Charlie", tags=json.dumps(["python", "fastapi"])
                    )
                )
                conn.commit()

                # Query: find rows where tags contains "python"
                je = func.json_each(test_table.c.tags).table_valued("value").alias("je")
                stmt = (
                    select(test_table.c.name)
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

            metadata.drop_all(engine)

            # Should find Alice and Charlie
            success = (
                len(rows) == 2 and rows[0][0] == "Alice" and rows[1][0] == "Charlie"
            )

            return Response.json(
                {
                    "test": "json_filter",
                    "success": success,
                    "matching_names": [row[0] for row in rows],
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
                {"test": "json_filter", "success": False, "error": str(e)},
                status=500,
            )

    async def test_json_aggregate(self):
        """Test aggregation after expanding JSON array with json_each."""
        import json
        from sqlalchemy import (
            MetaData,
            Table,
            Column,
            Integer,
            String,
            select,
            func,
            true,
        )

        table_name = f"test_json_agg_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("post_id", String(50)),
                Column("tags", String),  # JSON array
                Column("score", Integer),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                # Insert test data
                conn.execute(
                    test_table.insert().values(
                        post_id="p1", tags=json.dumps(["tech", "python"]), score=10
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        post_id="p2", tags=json.dumps(["tech", "javascript"]), score=20
                    )
                )
                conn.execute(
                    test_table.insert().values(
                        post_id="p3", tags=json.dumps(["python", "data"]), score=15
                    )
                )
                conn.commit()

                # Aggregate scores by tag (expand JSON array)
                je = func.json_each(test_table.c.tags).table_valued("value").alias("je")

                stmt = (
                    select(
                        je.c.value.label("tag"),
                        func.sum(test_table.c.score).label("total_score"),
                    )
                    .select_from(test_table.join(je, true()))
                    .group_by(je.c.value)
                    .order_by(je.c.value)
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

            metadata.drop_all(engine)

            # Build dict for verification
            tag_scores = {row[0]: row[1] for row in rows}

            # Expected:
            # data: 15 (p3)
            # javascript: 20 (p2)
            # python: 25 (p1 + p3)
            # tech: 30 (p1 + p2)
            success = (
                len(rows) == 4
                and tag_scores.get("data") == 15
                and tag_scores.get("javascript") == 20
                and tag_scores.get("python") == 25
                and tag_scores.get("tech") == 30
            )

            return Response.json(
                {
                    "test": "json_aggregate",
                    "success": success,
                    "tag_scores": tag_scores,
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
                {"test": "json_aggregate", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Pandas to_sql Tests

    @staticmethod
    def make_sqlite_method(conflict_prefix: str = "OR IGNORE"):
        """Return a pandas.to_sql(method=...) that inserts with a given prefix."""
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        def _method(table, conn, keys, data_iter):
            sa_table = getattr(table, "table", table)
            rows = [dict(zip(keys, row)) for row in data_iter]
            if not rows:
                return
            stmt = sqlite_insert(sa_table).values(rows)
            if conflict_prefix:
                stmt = stmt.prefix_with(conflict_prefix)
            conn.execute(stmt)

        return _method

    async def test_pandas_to_sql(self):
        """Test pandas DataFrame.to_sql() with D1 engine."""
        import pandas as pd
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_pandas_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("score", Integer),
            )

            metadata.create_all(engine)

            # Create DataFrame
            df = pd.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie"],
                    "score": [85, 92, 78],
                }
            )

            # Use to_sql to insert data
            df.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False,
            )

            # Verify data was inserted
            with engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.score).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = (
                len(rows) == 3
                and rows[0] == ("Alice", 85)
                and rows[1] == ("Bob", 92)
                and rows[2] == ("Charlie", 78)
            )

            return Response.json(
                {
                    "test": "pandas_to_sql",
                    "success": success,
                    "rows": [list(row) for row in rows],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "pandas_to_sql", "success": False, "error": str(e)},
                status=500,
            )

    async def test_pandas_to_sql_upsert(self):
        """Test pandas to_sql with OR REPLACE conflict handling."""
        import pandas as pd
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_pandas_upsert_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100), unique=True),
                Column("score", Integer),
            )

            metadata.create_all(engine)

            # Insert initial data
            df1 = pd.DataFrame({"name": ["Alice", "Bob"], "score": [85, 92]})
            df1.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False,
            )

            # Upsert with OR REPLACE - Alice's score should be updated
            df2 = pd.DataFrame({"name": ["Alice", "Charlie"], "score": [100, 78]})
            df2.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False,
                method=self.make_sqlite_method("OR REPLACE"),
            )

            # Verify
            with engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.score).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = (
                len(rows) == 3
                and rows[0] == ("Alice", 100)  # Score replaced
                and rows[1] == ("Bob", 92)
                and rows[2] == ("Charlie", 78)
            )

            return Response.json(
                {
                    "test": "pandas_to_sql_upsert",
                    "success": success,
                    "rows": [list(row) for row in rows],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "pandas_to_sql_upsert", "success": False, "error": str(e)},
                status=500,
            )

    async def test_pandas_to_sql_json(self):
        """Test pandas to_sql with stringified JSON columns."""
        import json
        import pandas as pd
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_pandas_json_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("config", String),  # JSON stored as TEXT
            )

            metadata.create_all(engine)

            # Create DataFrame with JSON data
            df = pd.DataFrame(
                {
                    "name": ["service_a", "service_b"],
                    "config": [
                        json.dumps({"enabled": True, "retries": 3}),
                        json.dumps({"enabled": False, "retries": 1}),
                    ],
                }
            )

            df.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False,
            )

            # Verify data and JSON parsing
            with engine.connect() as conn:
                result = conn.execute(
                    select(test_table.c.name, test_table.c.config).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            # Parse JSON to verify
            config_a = json.loads(rows[0][1])
            config_b = json.loads(rows[1][1])

            success = (
                len(rows) == 2
                and rows[0][0] == "service_a"
                and config_a["enabled"] is True
                and config_a["retries"] == 3
                and rows[1][0] == "service_b"
                and config_b["enabled"] is False
            )

            return Response.json(
                {
                    "test": "pandas_to_sql_json",
                    "success": success,
                    "rows": [
                        {"name": row[0], "config": json.loads(row[1])} for row in rows
                    ],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "pandas_to_sql_json", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - SQL Injection Prevention Tests

    async def test_sqli_string(self):
        """Test SQL injection attempt in string parameter is safely escaped."""
        table_name = f"test_sqli_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    secret TEXT
                )
            """)

            # Insert legitimate data
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, secret) VALUES (?, ?)",
                ("alice", "secret123"),
            )
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, secret) VALUES (?, ?)",
                ("bob", "secret456"),
            )

            # Attempt SQL injection via string parameter
            malicious_input = "' OR '1'='1"
            await cursor.execute_async(
                f"SELECT name FROM {table_name} WHERE name = ?", (malicious_input,)
            )
            rows = cursor.fetchall()

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            # Should return 0 rows (no match), not all rows
            success = len(rows) == 0

            return Response.json(
                {
                    "test": "sqli_string",
                    "success": success,
                    "row_count": len(rows),
                    "malicious_input": malicious_input,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "sqli_string", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqli_union(self):
        """Test UNION-based SQL injection is prevented."""
        table_name = f"test_sqli_union_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    username TEXT
                )
            """)

            await cursor.execute_async(
                f"INSERT INTO {table_name} (username) VALUES (?)", ("alice",)
            )

            # Attempt UNION injection to read sqlite_master
            malicious_input = "' UNION SELECT name FROM sqlite_master--"
            await cursor.execute_async(
                f"SELECT username FROM {table_name} WHERE username = ?",
                (malicious_input,),
            )
            rows = cursor.fetchall()

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            # Should return 0 rows, not table names from sqlite_master
            success = len(rows) == 0

            return Response.json(
                {
                    "test": "sqli_union",
                    "success": success,
                    "row_count": len(rows),
                    "malicious_input": malicious_input,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "sqli_union", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqli_drop(self):
        """Test DROP TABLE injection is prevented."""
        table_name = f"test_sqli_drop_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)

            await cursor.execute_async(
                f"INSERT INTO {table_name} (name) VALUES (?)", ("test",)
            )

            # Attempt to drop table via injection
            malicious_input = f"'; DROP TABLE {table_name};--"
            await cursor.execute_async(
                f"SELECT name FROM {table_name} WHERE name = ?", (malicious_input,)
            )
            rows = cursor.fetchall()

            # Should return 0 rows
            row_count = len(rows)

            # Verify table still exists
            await cursor.execute_async(f"SELECT COUNT(*) FROM {table_name}")
            count_row = cursor.fetchone()
            table_count = count_row[0] if count_row else 0

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            success = row_count == 0 and table_count == 1

            return Response.json(
                {
                    "test": "sqli_drop",
                    "success": success,
                    "row_count": row_count,
                    "table_still_exists": table_count == 1,
                    "malicious_input": malicious_input,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "sqli_drop", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqli_orm(self):
        """Test SQL injection prevention with SQLAlchemy ORM queries."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_sqli_orm_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("username", String(100)),
                Column("password", String(100)),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
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

                # Verify legitimate query still works
                result2 = conn.execute(
                    select(test_table).where(test_table.c.username == "admin")
                )
                legitimate_rows = result2.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 0 and len(legitimate_rows) == 1

            return Response.json(
                {
                    "test": "sqli_orm",
                    "success": success,
                    "malicious_row_count": len(rows),
                    "legitimate_row_count": len(legitimate_rows),
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "sqli_orm", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqli_like(self):
        """Test SQL injection in LIKE clause is prevented."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_sqli_like_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("email", String(100)),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(email="alice@example.com"))
                conn.execute(test_table.insert().values(email="bob@example.com"))
                conn.commit()

                # Attempt injection via LIKE pattern
                malicious_input = "%' OR '1'='1' --"
                result = conn.execute(
                    select(test_table).where(test_table.c.email.like(malicious_input))
                )
                malicious_rows = result.fetchall()

                # Verify legitimate LIKE works
                result2 = conn.execute(
                    select(test_table).where(test_table.c.email.like("%@example.com"))
                )
                legitimate_rows = result2.fetchall()

            metadata.drop_all(engine)

            success = len(malicious_rows) == 0 and len(legitimate_rows) == 2

            return Response.json(
                {
                    "test": "sqli_like",
                    "success": success,
                    "malicious_row_count": len(malicious_rows),
                    "legitimate_row_count": len(legitimate_rows),
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "sqli_like", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Additional SQLAlchemy Tests

    async def test_sqlalchemy_upsert(self):
        """Test INSERT ... ON CONFLICT DO UPDATE (upsert)."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, select
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        table_name = f"test_upsert_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", String, primary_key=True),
                Column("name", String(100)),
                Column("count", Integer),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
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
                    select(test_table).where(test_table.c.id == "key1")
                )
                row = result.fetchone()

            metadata.drop_all(engine)

            success = row is not None and row[1] == "Updated" and row[2] == 2

            return Response.json(
                {
                    "test": "sqlalchemy_upsert",
                    "success": success,
                    "row": list(row) if row else None,
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "sqlalchemy_upsert", "success": False, "error": str(e)},
                status=500,
            )

    async def test_sqlalchemy_get_tables(self):
        """Test dialect get_table_names works."""
        from sqlalchemy import MetaData, Table, Column, Integer, String

        table_name = f"test_tables_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            # Create table (registers in metadata for create_all/drop_all)
            Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
            )

            # Create the table
            metadata.create_all(engine)

            try:
                # Verify table exists using dialect method
                with engine.connect() as conn:
                    tables = engine.dialect.get_table_names(conn)
                    table_exists = table_name in tables
            finally:
                # Clean up
                metadata.drop_all(engine)

            return Response.json(
                {
                    "test": "sqlalchemy_get_tables",
                    "success": table_exists,
                    "table_name": table_name,
                    "table_exists": table_exists,
                    "table_count": len(tables),
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                Table(table_name, metadata)  # Register table in metadata for drop
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "sqlalchemy_get_tables", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Additional Empty Result Tests

    async def test_empty_result_where(self):
        """Test empty result from WHERE clause that matches nothing."""
        table_name = f"test_empty_where_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create and populate table
            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name) VALUES (?)", ("Alice",)
            )

            # Query with WHERE that matches nothing
            await cursor.execute_async(
                f"SELECT id, name FROM {table_name} WHERE name = ?",
                ("NonExistent",),
            )
            rows = cursor.fetchall()
            description = cursor.description

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            # Should have empty results but valid description
            success = (
                len(rows) == 0
                and description is not None
                and len(description) == 2
                and description[0][0] == "id"
                and description[1][0] == "name"
            )

            return Response.json(
                {
                    "test": "empty_result_where",
                    "success": success,
                    "row_count": len(rows),
                    "description": [d[0] for d in description] if description else None,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "empty_result_where", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Additional JSON Tests

    async def test_json_multiple_values(self):
        """Test filtering rows where JSON array contains any of multiple values."""
        import json
        from sqlalchemy import (
            MetaData,
            Table,
            Column,
            Integer,
            String,
            select,
            exists,
            func,
        )

        table_name = f"test_json_multi_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("product", String(100)),
                Column("categories", String),  # JSON array
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
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

            metadata.drop_all(engine)

            success = (
                len(rows) == 3
                and rows[0][0] == "Widget A"
                and rows[1][0] == "Widget B"
                and rows[2][0] == "Widget C"
            )

            return Response.json(
                {
                    "test": "json_multiple_values",
                    "success": success,
                    "matching_products": [row[0] for row in rows],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "json_multiple_values", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Boolean Column Tests

    async def test_boolean_column(self):
        """Test that boolean columns return Python bool, not str or int."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, select

        table_name = f"test_bool_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("username", String(100)),
                Column("is_admin", Boolean),
                Column("is_active", Boolean),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
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

                result = conn.execute(
                    select(
                        test_table.c.username,
                        test_table.c.is_admin,
                        test_table.c.is_active,
                    ).order_by(test_table.c.username)
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            # Verify booleans are actual Python bools
            success = (
                len(rows) == 3
                and rows[0][1] is True  # admin.is_admin
                and rows[0][2] is True  # admin.is_active
                and rows[1][1] is False  # inactive.is_admin
                and rows[1][2] is False  # inactive.is_active
                and rows[2][1] is False  # user.is_admin
                and rows[2][2] is True  # user.is_active
            )

            return Response.json(
                {
                    "test": "boolean_column",
                    "success": success,
                    "admin_is_admin": rows[0][1],
                    "admin_is_active": rows[0][2],
                    "inactive_is_admin": rows[1][1],
                    "inactive_is_active": rows[1][2],
                    "user_is_admin": rows[2][1],
                    "user_is_active": rows[2][2],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "boolean_column", "success": False, "error": str(e)},
                status=500,
            )

    async def test_boolean_filter(self):
        """Test filtering by boolean values works correctly."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, select

        table_name = f"test_bool_filter_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("enabled", Boolean),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
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

            metadata.drop_all(engine)

            success = (
                len(enabled_rows) == 2
                and enabled_rows[0][0] == "Feature A"
                and enabled_rows[1][0] == "Feature C"
                and len(disabled_rows) == 1
                and disabled_rows[0][0] == "Feature B"
            )

            return Response.json(
                {
                    "test": "boolean_filter",
                    "success": success,
                    "enabled_features": [row[0] for row in enabled_rows],
                    "disabled_features": [row[0] for row in disabled_rows],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "boolean_filter", "success": False, "error": str(e)},
                status=500,
            )

    async def test_boolean_nullable(self):
        """Test nullable boolean columns handle NULL correctly."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, select

        table_name = f"test_bool_null_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("verified", Boolean, nullable=True),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(name="User A", verified=True))
                conn.execute(test_table.insert().values(name="User B", verified=False))
                conn.execute(test_table.insert().values(name="User C", verified=None))
                conn.commit()

                result = conn.execute(
                    select(test_table.c.name, test_table.c.verified).order_by(
                        test_table.c.name
                    )
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = (
                len(rows) == 3
                and rows[0][1] is True  # User A
                and rows[1][1] is False  # User B
                and rows[2][1] is None  # User C (NULL)
            )

            return Response.json(
                {
                    "test": "boolean_nullable",
                    "success": success,
                    "user_a_verified": rows[0][1],
                    "user_b_verified": rows[1][1],
                    "user_c_verified": rows[2][1],
                }
            )
        except Exception as e:
            try:
                engine = self.get_engine()
                metadata = MetaData()
                test_table = Table(table_name, metadata)
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "boolean_nullable", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - NULL Parameter Tests

    async def test_null_string(self):
        """Test inserting NULL into a String column."""
        from sqlalchemy import MetaData, Table, Column, Integer, String, select

        table_name = f"test_null_str_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("value", String(100), nullable=True),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(value="hello"))
                conn.execute(test_table.insert().values(value=None))
                conn.commit()

                result = conn.execute(
                    select(test_table.c.value).order_by(test_table.c.id)
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 2 and rows[0][0] == "hello" and rows[1][0] is None

            return Response.json(
                {
                    "test": "null_string",
                    "success": success,
                    "row_with_value": rows[0][0],
                    "row_with_null": rows[1][0],
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "null_string", "success": False, "error": str(e)},
                status=500,
            )

    async def test_null_integer(self):
        """Test inserting NULL into an Integer column."""
        from sqlalchemy import MetaData, Table, Column, Integer, select

        table_name = f"test_null_int_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("value", Integer, nullable=True),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(value=42))
                conn.execute(test_table.insert().values(value=None))
                conn.commit()

                result = conn.execute(
                    select(test_table.c.value).order_by(test_table.c.id)
                )
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 2 and rows[0][0] == 42 and rows[1][0] is None

            return Response.json(
                {
                    "test": "null_integer",
                    "success": success,
                    "row_with_value": rows[0][0],
                    "row_with_null": rows[1][0],
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "null_integer", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - LargeBinary Column Tests

    async def test_largebinary_basic(self):
        """Test basic LargeBinary column functionality."""
        from sqlalchemy import (
            Column,
            Integer,
            LargeBinary,
            MetaData,
            String,
            Table,
            select,
        )

        table_name = f"test_largebinary_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("data", LargeBinary),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                binary_data = b"\x00\x01\x02\x03\xff\xfe\xfd"
                conn.execute(
                    test_table.insert().values(name="test_file", data=binary_data)
                )
                conn.commit()

                result = conn.execute(
                    select(test_table).where(test_table.c.name == "test_file")
                )
                row = result.fetchone()

            metadata.drop_all(engine)

            success = row is not None and row[2] == binary_data

            return Response.json(
                {
                    "test": "largebinary_basic",
                    "success": success,
                    "data_matches": row[2] == binary_data if row else False,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "largebinary_basic", "success": False, "error": str(e)},
                status=500,
            )

    async def test_largebinary_image(self):
        """Test storing simulated image data."""
        from sqlalchemy import (
            Column,
            Integer,
            LargeBinary,
            MetaData,
            String,
            Table,
            select,
        )

        table_name = f"test_largebinary_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("filename", String(100)),
                Column("image_data", LargeBinary),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                png_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
                conn.execute(
                    test_table.insert().values(filename="test.png", image_data=png_data)
                )
                conn.commit()

                result = conn.execute(
                    select(test_table).where(test_table.c.filename == "test.png")
                )
                row = result.fetchone()

            metadata.drop_all(engine)

            success = row is not None and row[2] == png_data
            has_png_header = row[2][:8] == b"\x89PNG\r\n\x1a\n" if row else False

            return Response.json(
                {
                    "test": "largebinary_image",
                    "success": success,
                    "has_png_header": has_png_header,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "largebinary_image", "success": False, "error": str(e)},
                status=500,
            )

    async def test_largebinary_nullable(self):
        """Test nullable LargeBinary columns."""
        from sqlalchemy import (
            Column,
            Integer,
            LargeBinary,
            MetaData,
            String,
            Table,
            select,
        )

        table_name = f"test_largebinary_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("data", LargeBinary, nullable=True),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(name="no_data", data=None))
                conn.execute(
                    test_table.insert().values(name="has_data", data=b"\xab\xcd")
                )
                conn.commit()

                result = conn.execute(select(test_table).order_by(test_table.c.id))
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 2
            null_is_none = rows[0][2] is None if len(rows) > 0 else False
            data_is_bytes = rows[1][2] == b"\xab\xcd" if len(rows) > 1 else False

            return Response.json(
                {
                    "test": "largebinary_nullable",
                    "success": success,
                    "null_is_none": null_is_none,
                    "data_is_bytes": data_is_bytes,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "largebinary_nullable", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - ON CONFLICT Advanced Tests

    async def test_on_conflict_do_nothing(self):
        """Test INSERT ... ON CONFLICT DO NOTHING."""
        from sqlalchemy import Column, Integer, MetaData, String, Table, select
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        table_name = f"test_conflict_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100), unique=True),
                Column("count", Integer),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                stmt = sqlite_insert(test_table).values(
                    id=1, name="unique_name", count=10
                )
                conn.execute(stmt)
                conn.commit()

                stmt = sqlite_insert(test_table).values(
                    id=2, name="unique_name", count=20
                )
                stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
                conn.execute(stmt)
                conn.commit()

                result = conn.execute(select(test_table))
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 1
            original_preserved = rows[0][2] == 10 if rows else False

            return Response.json(
                {
                    "test": "on_conflict_do_nothing",
                    "success": success,
                    "row_count": len(rows),
                    "original_value_preserved": original_preserved,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "on_conflict_do_nothing", "success": False, "error": str(e)},
                status=500,
            )

    async def test_on_conflict_composite(self):
        """Test ON CONFLICT with composite unique constraint."""
        from sqlalchemy import (
            Column,
            Integer,
            MetaData,
            String,
            Table,
            UniqueConstraint,
            select,
        )
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        table_name = f"test_conflict_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("user_id", Integer),
                Column("resource_id", Integer),
                Column("access_level", String(50)),
                UniqueConstraint("user_id", "resource_id", name="unique_user_resource"),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                stmt = sqlite_insert(test_table).values(
                    user_id=1, resource_id=100, access_level="read"
                )
                conn.execute(stmt)
                conn.commit()

                stmt = sqlite_insert(test_table).values(
                    user_id=1, resource_id=100, access_level="write"
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["user_id", "resource_id"],
                    set_={"access_level": stmt.excluded.access_level},
                )
                conn.execute(stmt)
                conn.commit()

                result = conn.execute(select(test_table))
                rows = result.fetchall()

            metadata.drop_all(engine)

            success = len(rows) == 1
            value_updated = rows[0][2] == "write" if rows else False

            return Response.json(
                {
                    "test": "on_conflict_composite",
                    "success": success,
                    "value_updated": value_updated,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "on_conflict_composite", "success": False, "error": str(e)},
                status=500,
            )

    # MARK: - Single-Row Result Tests

    async def test_single_row_result(self):
        """Test that a single-row SELECT returns data in fetchall, not description."""
        table_name = f"test_single_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create table and insert exactly 1 row
            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("only_row", 99),
            )

            # SELECT the single row
            await cursor.execute_async(f"SELECT id, name, value FROM {table_name}")
            description = cursor.description
            rows = cursor.fetchall()
            rowcount = cursor.rowcount

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            # Verify: description should have column names, not data values
            desc_names = [d[0] for d in description] if description else []
            expected_columns = ["id", "name", "value"]

            success = (
                len(rows) == 1
                and rows[0][1] == "only_row"
                and rows[0][2] == 99
                and desc_names == expected_columns
            )

            return Response.json(
                {
                    "test": "single_row_result",
                    "success": success,
                    "rows": rows,
                    "row_count": len(rows),
                    "rowcount": rowcount,
                    "description_names": desc_names,
                    "expected_columns": expected_columns,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "single_row_result", "success": False, "error": str(e)},
                status=500,
            )

    async def test_single_row_sqlalchemy(self):
        """Test single-row result via SQLAlchemy engine."""
        from sqlalchemy import Column, Integer, MetaData, String, Table, select

        table_name = f"test_single_sa_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                Column("value", Integer),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(test_table.insert().values(name="only_row", value=99))
                conn.commit()

                result = conn.execute(select(test_table))
                rows = result.fetchall()
                columns = list(result.keys())

            metadata.drop_all(engine)

            success = len(rows) == 1 and rows[0][1] == "only_row" and rows[0][2] == 99

            return Response.json(
                {
                    "test": "single_row_sqlalchemy",
                    "success": success,
                    "rows": [list(row) for row in rows],
                    "columns": columns,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "single_row_sqlalchemy", "success": False, "error": str(e)},
                status=500,
            )

    async def test_multi_row_result(self):
        """Test that multi-row SELECT returns correct data and description."""
        table_name = f"test_multi_{uuid.uuid4().hex[:8]}"

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("row_one", 10),
            )
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("row_two", 20),
            )
            await cursor.execute_async(
                f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
                ("row_three", 30),
            )

            # SELECT all rows
            await cursor.execute_async(
                f"SELECT id, name, value FROM {table_name} ORDER BY id"
            )
            description = cursor.description
            rows = cursor.fetchall()
            rowcount = cursor.rowcount

            # Clean up
            await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()

            desc_names = [d[0] for d in description] if description else []
            expected_columns = ["id", "name", "value"]

            success = (
                len(rows) == 3
                and desc_names == expected_columns
                and rows[0][1] == "row_one"
                and rows[1][1] == "row_two"
                and rows[2][1] == "row_three"
            )

            return Response.json(
                {
                    "test": "multi_row_result",
                    "success": success,
                    "rows": rows,
                    "row_count": len(rows),
                    "rowcount": rowcount,
                    "description_names": desc_names,
                    "expected_columns": expected_columns,
                }
            )
        except Exception as e:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                await cursor.execute_async(f"DROP TABLE IF EXISTS {table_name}")
                conn.close()
            except Exception:
                pass
            return Response.json(
                {"test": "multi_row_result", "success": False, "error": str(e)},
                status=500,
            )

    async def test_on_conflict_where(self):
        """Test ON CONFLICT with WHERE clause."""
        from sqlalchemy import Boolean, Column, Integer, MetaData, String, Table, select
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        table_name = f"test_conflict_{uuid.uuid4().hex[:8]}"

        try:
            engine = self.get_engine()
            metadata = MetaData()

            test_table = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("email", String(100), unique=True),
                Column("is_verified", Boolean),
            )

            metadata.create_all(engine)

            with engine.connect() as conn:
                stmt = sqlite_insert(test_table).values(
                    email="test@example.com", is_verified=False
                )
                conn.execute(stmt)
                conn.commit()

                stmt = sqlite_insert(test_table).values(
                    email="test@example.com", is_verified=True
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["email"],
                    set_={"is_verified": stmt.excluded.is_verified},
                    where=(test_table.c.is_verified == False),  # noqa: E712
                )
                conn.execute(stmt)
                conn.commit()

                result = conn.execute(select(test_table))
                row = result.fetchone()

            metadata.drop_all(engine)

            success = row is not None and row[2] is True

            return Response.json(
                {
                    "test": "on_conflict_where",
                    "success": success,
                    "is_verified": row[2] if row else None,
                }
            )
        except Exception as e:
            try:
                metadata.drop_all(engine)
            except Exception:
                pass
            return Response.json(
                {"test": "on_conflict_where", "success": False, "error": str(e)},
                status=500,
            )
