"""
Example Python Worker using sqlalchemy-cloudflare-d1.

This example demonstrates how to use the SQLAlchemy D1 dialect
inside a Cloudflare Python Worker using the WorkerConnection class.

Note: Python Workers are currently in beta.
"""

from workers import WorkerEntrypoint, Response
from sqlalchemy_cloudflare_d1 import WorkerConnection


class Default(WorkerEntrypoint):
    """Default Worker entrypoint that handles HTTP requests."""

    async def fetch(self, request, env):
        """Handle incoming HTTP requests."""
        url = request.url
        path = url.split("/")[-1] if "/" in url else ""

        if path == "users":
            return await self.list_users()
        elif path == "random":
            return await self.get_random_user()
        elif path == "health":
            return await self.health_check()
        elif path == "create":
            return await self.create_user()
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
                "/users": "List all users (using WorkerConnection)",
                "/random": "Get a random user",
                "/health": "Health check",
                "/create": "Create a test user",
            },
            "package": "sqlalchemy-cloudflare-d1",
            "note": "Python Workers are in beta. APIs may change.",
        }
        return Response.json(endpoints)

    async def health_check(self):
        """Perform a health check using WorkerConnection."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            await cursor.execute_async("SELECT 1 as health")
            result = cursor.fetchone()
            conn.close()

            return Response.json(
                {
                    "status": "healthy",
                    "database": "connected",
                    "result": result[0] if result else None,
                }
            )
        except Exception as e:
            return Response.json({"status": "unhealthy", "error": str(e)}, status=500)

    async def list_users(self):
        """List all users using WorkerConnection and WorkerCursor."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async("""
                SELECT id, name, email, created_at
                FROM users
                ORDER BY created_at DESC
            """)

            # Fetch all results
            rows = cursor.fetchall()

            # Get column names from cursor description
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            # Convert to list of dicts
            users = [dict(zip(columns, row)) for row in rows]

            conn.close()

            return Response.json({"users": users, "count": len(users)})
        except Exception as e:
            return Response.json({"error": str(e)}, status=500)

    async def get_random_user(self):
        """Get a random user using WorkerConnection."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            await cursor.execute_async("""
                SELECT id, name, email, created_at
                FROM users
                ORDER BY RANDOM()
                LIMIT 1
            """)

            row = cursor.fetchone()

            if row:
                columns = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )
                user = dict(zip(columns, row))
                conn.close()
                return Response.json(user)
            else:
                conn.close()
                return Response.json({"error": "No users found"}, status=404)

        except Exception as e:
            return Response.json({"error": str(e)}, status=500)

    async def create_user(self):
        """Create a test user using parameterized query."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Use parameterized query to safely insert data
            await cursor.execute_async(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                ("Test User", f"test{id(self)}@example.com"),
            )

            # Get the last inserted row id
            last_id = cursor.lastrowid

            conn.close()

            return Response.json(
                {"success": True, "message": "User created", "user_id": last_id}
            )
        except Exception as e:
            return Response.json({"error": str(e)}, status=500)
