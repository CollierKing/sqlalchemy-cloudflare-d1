# SQLAlchemy over Hyperdrive — Python Worker example

Uses SQLAlchemy Core against a PostgreSQL database through a
[Hyperdrive](https://developers.cloudflare.com/hyperdrive/examples/python-workers/)
binding, inside a Python Worker.

Unlike the D1 example in `../workers`, no custom dialect is involved —
Hyperdrive speaks the PostgreSQL wire protocol, so SQLAlchemy's own
`postgresql+pg8000` dialect does the work. This package only supplies the
binding-to-engine glue.

## Requirements

- `compatibility_date` of `2026-09-08` or later, plus the `python_workers` flag
- A Hyperdrive config pointing at a reachable PostgreSQL database
- A **synchronous** driver: `pg8000` for PostgreSQL (the supported path), or
  `pymysql` for MySQL (best effort, no automated coverage). Of
  the five drivers Cloudflare documents, these are the only two that can back a
  SQLAlchemy engine — `psycopg` needs libpq (absent from the Workers Pyodide
  build) and `asyncpg`/`aiomysql` are async-only, which SQLAlchemy cannot drive
  without greenlet. `/drivers` re-checks this matrix against your own binding.

## Setup

1. Create a Hyperdrive config and put its ID in `wrangler.jsonc`:

   ```bash
   npx wrangler hyperdrive create sqlalchemy-example \
     --connection-string="$POSTGRES_URL"
   ```

2. Sync dependencies, then copy the local package into `python_modules`:

   ```bash
   uv lock && uv sync
   uv run pywrangler sync --force
   mkdir -p python_modules/sqlalchemy_cloudflare_d1
   cp ../../src/sqlalchemy_cloudflare_d1/*.py python_modules/sqlalchemy_cloudflare_d1/
   ```

   `sqlalchemy-cloudflare-d1` is deliberately absent from `pyproject.toml`.
   pywrangler installs Worker dependencies with `--no-build`, so an editable
   local path has no wheel to install and the sync fails outright.

3. Create the table, either by running `db_init.sql` against the origin
   database or by hitting `/setup` once the Worker is up.

## Run

Hyperdrive has **no local emulation**. `wrangler dev` always connects to the
Postgres named by an environment variable, never to the real Hyperdrive config,
and `"remote": true` is not valid on a hyperdrive binding. So local dev needs:

```bash
export CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE="postgresql://user:pass@host:5432/db?sslmode=require"
uv run pywrangler dev --port 8788
```

The Worker still sees a normal Hyperdrive binding — wrangler proxies it through
a local `*.hyperdrive.local` endpoint. Only a deployed Worker exercises the real
config, including its pooling and caching.

## Endpoints

```bash
curl http://localhost:8788/setup       # create table + seed rows (idempotent)
curl http://localhost:8788/health      # open a connection, SELECT 1
curl http://localhost:8788/driver      # report resolved dialect and pool
curl http://localhost:8788/select      # SELECT via Table + select()
curl http://localhost:8788/crud        # insert / update / delete round-trip
curl http://localhost:8788/reflect     # reflect columns and primary key
curl http://localhost:8788/concurrent  # overlapping queries, serialized
curl http://localhost:8788/drivers     # which drivers connect from a Worker
curl http://localhost:8788/teardown    # drop the table again
```

## Notes

- **TLS must be explicitly disabled, and each driver spells it differently.**
  pg8000 treats `ssl_context=None` as "attempt SSL" (only `False` disables it),
  and pymysql needs `ssl_disabled=True`. Otherwise the handshake over the
  Workers socket shim corrupts the connection — `KeyError: b'\x00'` for pg8000,
  a `secureTransport` error for pymysql. `create_engine_from_hyperdrive()`
  handles both. Hyperdrive terminates TLS itself, so the Worker hop needs no
  encryption.
- `create_engine_from_hyperdrive()` uses `NullPool`: a Worker cannot reuse
  sockets across requests, and Hyperdrive maintains the real pool server-side.
- Wrap database work in `hyperdrive_connection()` rather than calling
  `engine.connect()` directly. It holds an isolate-wide `asyncio.Lock` so
  synchronous driver I/O from overlapping requests is serialized.
