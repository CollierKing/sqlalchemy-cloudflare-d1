"""
Example Python Worker using SQLAlchemy over Cloudflare Hyperdrive.

Hyperdrive fronts a regular PostgreSQL database, so this Worker uses stock
SQLAlchemy Core against the ``postgresql+pg8000`` dialect -- no D1 dialect is
involved. ``create_engine_from_hyperdrive()`` builds the engine from the
binding, and ``hyperdrive_connection()`` serializes synchronous driver I/O.

Only synchronous SQLAlchemy works here: the async engine needs greenlet, which
is unavailable in the Python Workers runtime.

Note: Python Workers and Hyperdrive-in-Python-Workers are both in beta.
"""

import asyncio
import traceback

from workers import WorkerEntrypoint, Response

from sqlalchemy_cloudflare_d1.hyperdrive import (
    create_engine_from_hyperdrive,
    hyperdrive_connection,
)

# MARK: - Schema

TABLE_NAME = "sqlalchemy_hyperdrive_example"


class Default(WorkerEntrypoint):
    """Default Worker entrypoint that handles HTTP requests."""

    async def fetch(self, request):
        """Handle incoming HTTP requests."""
        url = request.url
        path = url.split("/")[-1].split("?")[0] if "/" in url else ""

        if path in ("setup", "teardown"):
            denied = self.check_admin(request)
            if denied is not None:
                return denied
            if path == "setup":
                return await self.setup_schema()
            return await self.teardown_schema()
        elif path == "drivers":
            return await self.test_driver_matrix()
        elif path == "health":
            return await self.health_check()
        elif path == "select":
            return await self.test_select()
        elif path == "crud":
            return await self.test_crud()
        elif path == "reflect":
            return await self.test_reflect()
        elif path == "concurrent":
            return await self.test_concurrent()
        elif path == "driver":
            return await self.test_driver()

        return Response.json(
            {
                "service": "sqlalchemy-hyperdrive-worker",
                "endpoints": [
                    "/setup",
                    "/health",
                    "/select",
                    "/crud",
                    "/reflect",
                    "/concurrent",
                    "/driver",
                    "/drivers",
                    "/teardown",
                ],
            }
        )

    # MARK: - Authorization

    def check_admin(self, request):
        """Gate the schema-changing endpoints.

        /setup and /teardown run DDL, and a Hyperdrive config often points at a
        shared production database. When ADMIN_TOKEN is configured -- as it
        should be on any deployed Worker -- callers must present it. Locally the
        variable is usually absent and the endpoints stay open.

        Returns None when the request may proceed, or a 403 Response.
        """
        expected = getattr(self.env, "ADMIN_TOKEN", None)
        if not expected:
            return None

        provided = request.headers.get("x-admin-token")
        if provided and str(provided) == str(expected):
            return None

        return Response.json(
            {"success": False, "error": "x-admin-token required"}, status=403
        )

    # MARK: - Engine

    def get_engine(self):
        """Build a SQLAlchemy engine from the Hyperdrive binding."""
        return create_engine_from_hyperdrive(self.env.HYPERDRIVE)

    # MARK: - Driver Matrix

    async def test_driver_matrix(self):
        """Exercise every driver in _DRIVERS against this binding.

        Reports which drivers actually connect from inside a Worker, rather
        than which ones merely import.
        """
        from sqlalchemy import literal_column, select

        from sqlalchemy_cloudflare_d1.hyperdrive import _DRIVERS, _detect_driver

        # Only probe drivers matching this binding's family. Pointing a MySQL
        # driver at a PostgreSQL origin does not fail fast -- it blocks on a
        # handshake that never completes and takes the whole Worker with it.
        family = _DRIVERS[_detect_driver(self.env.HYPERDRIVE)]["drivername"]
        family = family.split("+", 1)[0]

        results = {}
        for name in sorted(_DRIVERS):
            if not _DRIVERS[name]["drivername"].startswith(family):
                results[name] = f"skipped (binding is {family})"
                continue
            try:
                engine = create_engine_from_hyperdrive(self.env.HYPERDRIVE, driver=name)
                async with hyperdrive_connection(engine) as conn:
                    value = conn.execute(select(literal_column("1"))).scalar()
                results[name] = f"OK {value}"
            except Exception as e:
                msg = str(e).splitlines()[0][:160]
                results[name] = f"{type(e).__name__}: {msg}"

        return Response.json({"test": "drivers", "results": results})

    # MARK: - Endpoints

    async def setup_schema(self):
        """Create the example table if it does not already exist.

        Equivalent to running db_init.sql, for when you cannot reach the origin
        database directly. Creates only its own namespaced table and never
        drops or alters anything pre-existing -- a Hyperdrive config often
        points at a shared database.
        """
        try:
            from sqlalchemy import (
                Column,
                Integer,
                MetaData,
                String,
                Table,
                func,
                insert,
                select,
            )

            engine = self.get_engine()
            metadata = MetaData()
            items = Table(
                TABLE_NAME,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String, nullable=False),
                Column("quantity", Integer, nullable=False, server_default="0"),
            )

            seeded = 0
            async with hyperdrive_connection(engine) as conn:
                metadata.create_all(conn, checkfirst=True)

                existing = conn.execute(
                    select(func.count()).select_from(items)
                ).scalar()

                if not existing:
                    conn.execute(
                        insert(items),
                        [
                            {"name": "widget", "quantity": 10},
                            {"name": "gadget", "quantity": 5},
                            {"name": "sprocket", "quantity": 0},
                        ],
                    )
                    seeded = 3

                conn.commit()

            return Response.json(
                {
                    "test": "setup",
                    "success": True,
                    "table": TABLE_NAME,
                    "rows_seeded": seeded,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "setup", "success": False, "error": str(e)}, status=500
            )

    async def teardown_schema(self):
        """Drop the example table.

        Counterpart to /setup, so a test run can leave the origin database
        exactly as it found it. Only ever drops its own namespaced table.
        """
        try:
            from sqlalchemy import Column, Integer, MetaData, String, Table

            engine = self.get_engine()
            metadata = MetaData()
            Table(
                TABLE_NAME,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String),
                Column("quantity", Integer),
            )

            async with hyperdrive_connection(engine) as conn:
                metadata.drop_all(conn, checkfirst=True)
                conn.commit()

            return Response.json(
                {"test": "teardown", "success": True, "table": TABLE_NAME}
            )
        except Exception as e:
            return Response.json(
                {"test": "teardown", "success": False, "error": str(e)}, status=500
            )

    async def health_check(self):
        """Confirm the Worker can open a Hyperdrive connection and query."""
        try:
            from sqlalchemy import literal_column, select

            engine = self.get_engine()

            async with hyperdrive_connection(engine) as conn:
                row = conn.execute(select(literal_column("1"))).fetchone()

            return Response.json(
                {"test": "health", "success": row is not None and row[0] == 1}
            )
        except Exception as e:
            return Response.json(
                {
                    "test": "health",
                    "success": False,
                    "error": f"{type(e).__name__}: {e}",
                    "traceback": traceback.format_exc().splitlines()[-18:],
                },
                status=500,
            )

    async def test_driver(self):
        """Report the dialect and pool the binding resolved to."""
        try:
            engine = self.get_engine()

            return Response.json(
                {
                    "test": "driver",
                    "success": True,
                    "drivername": engine.url.drivername,
                    "poolclass": engine.pool.__class__.__name__,
                    "host": engine.url.host,
                    "database": engine.url.database,
                    # Does the binding expose connectionString? Driver
                    # auto-detection depends on its scheme.
                    "binding_attrs": sorted(
                        a
                        for a in dir(self.env.HYPERDRIVE)
                        if not a.startswith("_") and "password" not in a.lower()
                    ),
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "driver", "success": False, "error": str(e)}, status=500
            )

    async def test_select(self):
        """Run a SELECT through SQLAlchemy Core."""
        try:
            from sqlalchemy import MetaData, Table, select

            engine = self.get_engine()
            metadata = MetaData()

            async with hyperdrive_connection(engine) as conn:
                items = Table(TABLE_NAME, metadata, autoload_with=conn)
                rows = conn.execute(select(items).order_by(items.c.id).limit(5))
                results = [dict(r._mapping) for r in rows]

            return Response.json({"test": "select", "success": True, "rows": results})
        except Exception as e:
            return Response.json(
                {"test": "select", "success": False, "error": str(e)}, status=500
            )

    async def test_crud(self):
        """Insert, read back, update and delete a row via SQLAlchemy Core."""
        try:
            from sqlalchemy import MetaData, Table, select

            engine = self.get_engine()
            metadata = MetaData()

            async with hyperdrive_connection(engine) as conn:
                items = Table(TABLE_NAME, metadata, autoload_with=conn)

                inserted = conn.execute(
                    items.insert().values(name="worker-crud", quantity=1)
                )
                new_id = inserted.inserted_primary_key[0]

                after_insert = conn.execute(
                    select(items).where(items.c.id == new_id)
                ).fetchone()

                conn.execute(
                    items.update().where(items.c.id == new_id).values(quantity=42)
                )
                after_update = conn.execute(
                    select(items.c.quantity).where(items.c.id == new_id)
                ).fetchone()

                conn.execute(items.delete().where(items.c.id == new_id))
                after_delete = conn.execute(
                    select(items).where(items.c.id == new_id)
                ).fetchone()

                conn.commit()

            return Response.json(
                {
                    "test": "crud",
                    "success": (
                        after_insert is not None
                        and after_update[0] == 42
                        and after_delete is None
                    ),
                    "inserted_name": after_insert[1] if after_insert else None,
                    "updated_quantity": after_update[0] if after_update else None,
                    "deleted": after_delete is None,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "crud", "success": False, "error": str(e)}, status=500
            )

    async def test_reflect(self):
        """Reflect the table and report its columns."""
        try:
            from sqlalchemy import MetaData, Table

            engine = self.get_engine()
            metadata = MetaData()

            async with hyperdrive_connection(engine) as conn:
                items = Table(TABLE_NAME, metadata, autoload_with=conn)
                columns = [c.name for c in items.columns]
                pk = [c.name for c in items.primary_key.columns]

            return Response.json(
                {
                    "test": "reflect",
                    "success": "id" in columns and "name" in columns,
                    "columns": columns,
                    "primary_key": pk,
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "reflect", "success": False, "error": str(e)}, status=500
            )

    async def test_concurrent(self):
        """Fire overlapping queries to confirm the I/O lock serializes them."""
        try:
            from sqlalchemy import literal_column, select

            engine = self.get_engine()

            async def query(n):
                async with hyperdrive_connection(engine) as conn:
                    row = conn.execute(select(literal_column(str(n)))).fetchone()
                    return row[0]

            results = await asyncio.gather(*(query(n) for n in range(1, 6)))

            return Response.json(
                {
                    "test": "concurrent",
                    "success": list(results) == [1, 2, 3, 4, 5],
                    "results": list(results),
                }
            )
        except Exception as e:
            return Response.json(
                {"test": "concurrent", "success": False, "error": str(e)}, status=500
            )
