# SQLAlchemy D1 Python Worker Example

This example demonstrates how to use the `sqlalchemy-cloudflare-d1` package inside a Cloudflare Python Worker.

> **Note**: Python Workers are currently in beta. APIs may change before official release.

## What This Example Shows

The example uses `WorkerConnection` from `sqlalchemy-cloudflare-d1` to wrap the D1 binding and provide a DBAPI-compatible interface:

```python
from workers import WorkerEntrypoint, Response
from sqlalchemy_cloudflare_d1 import WorkerConnection

class MyWorker(WorkerEntrypoint):
    async def fetch(self, request, env):
        # Wrap the D1 binding with WorkerConnection
        conn = WorkerConnection(self.env.DB)
        cursor = conn.cursor()

        # Execute queries asynchronously
        await cursor.execute_async("SELECT * FROM users")
        rows = cursor.fetchall()

        conn.close()
        return Response.json({"users": rows})
```

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- A Cloudflare account with Workers enabled

## Setup

1. **Install dependencies**:
   ```bash
   uv sync
   ```

2. **Create a D1 database** (if you haven't already):
   ```bash
   uv run pywrangler d1 create example-db
   ```

3. **Update `wrangler.jsonc`** with your database ID:
   - Copy the database ID from the output of the create command
   - Replace `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` in `wrangler.jsonc`

4. **Initialize the database**:
   ```bash
   uv run pywrangler d1 execute example-db --local --file db_init.sql
   ```

## Development

Run the worker locally:

```bash
uv run pywrangler dev
```

The worker will be available at `http://localhost:8787`.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/` | API documentation |
| `/users` | List all users (using WorkerConnection) |
| `/random` | Get a random user |
| `/health` | Health check |
| `/create` | Create a test user |

## Key Concepts

### WorkerConnection

`WorkerConnection` wraps the D1 binding provided by the Worker environment:

```python
from sqlalchemy_cloudflare_d1 import WorkerConnection

conn = WorkerConnection(self.env.DB)
```

### Async Execution

Inside Workers, all database operations must be async. Use `execute_async()`:

```python
cursor = conn.cursor()
await cursor.execute_async("SELECT * FROM users WHERE id = ?", (user_id,))
```

### Parameterized Queries

Use `?` placeholders for safe parameterized queries:

```python
await cursor.execute_async(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    ("John Doe", "john@example.com")
)
```

## Deployment

Deploy to Cloudflare:

```bash
# First, initialize the production database
uv run pywrangler d1 execute example-db --file db_init.sql

# Then deploy the worker
uv run pywrangler deploy
```

## Project Structure

```
examples/workers/
├── src/
│   └── entry.py       # Worker entry point using WorkerConnection
├── db_init.sql        # Database initialization script
├── pyproject.toml     # Python dependencies (includes sqlalchemy-cloudflare-d1)
├── wrangler.jsonc     # Cloudflare Worker configuration
└── README.md          # This file
```

## Running Tests

The integration tests are in the main `tests/` directory. From the project root:

```bash
uv sync
uv run pytest tests/test_worker_integration.py -v
```

The test suite will:
1. Initialize the local D1 database with `db_init.sql`
2. Start a `pywrangler dev --local` server on a random port
3. Run HTTP requests against the endpoints
4. Tear down the server after each test

## Resources

- [sqlalchemy-cloudflare-d1 Documentation](../../README.md)
- [Python Workers Documentation](https://developers.cloudflare.com/workers/languages/python/)
- [D1 Documentation](https://developers.cloudflare.com/d1/)
- [Python Workers Examples](https://github.com/cloudflare/python-workers-examples)
