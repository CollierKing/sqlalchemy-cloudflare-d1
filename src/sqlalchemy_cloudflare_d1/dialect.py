"""
SQLAlchemy dialect for Cloudflare D1.
"""

import base64
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.engine import default
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql.sqltypes import (
    Boolean,
    INTEGER,
    LargeBinary,
    NUMERIC,
    REAL,
    TEXT,
)
from sqlalchemy import text

from .connection import CloudflareD1DBAPI
from .compiler import (
    CloudflareD1Compiler,
    CloudflareD1DDLCompiler,
    CloudflareD1TypeCompiler,
)


# MARK: - Custom Type Processors


class D1Boolean(Boolean):
    """Custom Boolean type for Cloudflare D1.

    D1's API converts Python bools to strings. We use bind_processor to
    send integers (1/0) instead, so comparisons like `WHERE col = 1` work.
    The result_processor handles both string and integer responses.
    """

    def bind_processor(
        self, dialect: Dialect
    ) -> Callable[[Optional[bool]], Optional[int]]:
        """Convert Python bool to integer for D1."""

        def process(value: Optional[bool]) -> Optional[int]:
            if value is None:
                return None
            return 1 if value else 0

        return process

    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> Callable[[Any], Optional[bool]]:
        """Convert D1 boolean values to Python bool."""

        def process(value: Any) -> Optional[bool]:
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, int):
                return bool(value)
            if isinstance(value, str):
                return value.lower() == "true"
            return bool(value)

        return process


class D1LargeBinary(LargeBinary):
    """Custom LargeBinary type for Cloudflare D1.

    D1 stores BLOB data, but the REST API requires base64-encoded strings for
    binary data transfer. This type processor handles:
    - bind_processor: Encodes bytes to base64 strings before sending to D1
    - result_processor: Decodes base64 strings back to bytes when reading from D1
    """

    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Optional[str]]:
        """Convert bytes to base64 strings before sending to D1."""

        def process(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, bytes):
                return base64.b64encode(value).decode("ascii")
            return value

        return process

    def result_processor(
        self, dialect: Dialect, coltype: Any
    ) -> Callable[[Any], Optional[bytes]]:
        """Convert base64 strings back to bytes when reading from D1."""

        def process(value: Any) -> Optional[bytes]:
            if value is None:
                return None
            if isinstance(value, bytes):
                return value
            if isinstance(value, str):
                # D1 returns binary data as base64-encoded strings
                try:
                    return base64.b64decode(value)
                except Exception:
                    # If not valid base64, return as encoded bytes
                    return value.encode("utf-8")
            return value

        return process


# MARK: - Dialect


class CloudflareD1Dialect(default.DefaultDialect):
    """SQLAlchemy dialect for Cloudflare D1 database."""

    name = "cloudflare_d1"
    driver = "httpx"
    supports_alter = False
    supports_pk_autoincrement = True
    supports_default_values = True
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_native_decimal = False
    supports_native_boolean = True
    supports_native_enum = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_statement_cache = True

    # SQLite/D1 specific capabilities
    supports_cast = True
    supports_multivalues_insert = True

    default_paramstyle = "qmark"

    # Compiler classes
    statement_compiler = CloudflareD1Compiler
    ddl_compiler = CloudflareD1DDLCompiler
    type_compiler = CloudflareD1TypeCompiler

    # Type mapping from SQLAlchemy to D1/SQLite
    colspecs = {
        Boolean: D1Boolean,
        LargeBinary: D1LargeBinary,
    }

    # Reserved words (SQLite keywords)
    reserved_words = {
        "abort",
        "action",
        "add",
        "after",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "attach",
        "autoincrement",
        "before",
        "begin",
        "between",
        "by",
        "cascade",
        "case",
        "cast",
        "check",
        "collate",
        "column",
        "commit",
        "conflict",
        "constraint",
        "create",
        "cross",
        "current_date",
        "current_time",
        "current_timestamp",
        "database",
        "default",
        "deferrable",
        "deferred",
        "delete",
        "desc",
        "detach",
        "distinct",
        "drop",
        "each",
        "else",
        "end",
        "escape",
        "except",
        "exclusive",
        "exists",
        "explain",
        "fail",
        "for",
        "foreign",
        "from",
        "full",
        "glob",
        "group",
        "having",
        "if",
        "ignore",
        "immediate",
        "in",
        "index",
        "indexed",
        "initially",
        "inner",
        "insert",
        "instead",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "key",
        "left",
        "like",
        "limit",
        "match",
        "natural",
        "no",
        "not",
        "notnull",
        "null",
        "of",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "plan",
        "pragma",
        "primary",
        "query",
        "raise",
        "recursive",
        "references",
        "regexp",
        "reindex",
        "release",
        "rename",
        "replace",
        "restrict",
        "right",
        "rollback",
        "row",
        "savepoint",
        "select",
        "set",
        "table",
        "temp",
        "temporary",
        "then",
        "to",
        "transaction",
        "trigger",
        "union",
        "unique",
        "update",
        "using",
        "vacuum",
        "values",
        "view",
        "virtual",
        "when",
        "where",
        "with",
        "without",
    }

    @classmethod
    def import_dbapi(cls) -> Any:
        """Import the DBAPI module."""
        return CloudflareD1DBAPI

    def create_connect_args(self, url: Any) -> tuple:
        """Extract connection arguments from the URL."""
        opts = {
            "account_id": url.username,
            "database_id": url.host,
            "api_token": url.password,
        }

        # Handle additional query parameters
        if url.query:
            opts.update(url.query)

        return (), opts

    def get_isolation_level(self, connection: Any) -> Optional[str]:
        """D1 doesn't support isolation levels."""
        return None

    def set_isolation_level(self, connection: Any, level: Optional[str]) -> None:
        """D1 doesn't support isolation levels."""
        pass

    def get_table_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """Get a list of table names."""
        query = text("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        result = connection.execute(query)
        return [row[0] for row in result]

    def has_table(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> bool:
        """Check if a table exists."""
        query = text("""
            SELECT name FROM sqlite_master
            WHERE type=:table_type AND name=:table_name AND name NOT LIKE 'sqlite_%'
        """)
        result = connection.execute(
            query, {"table_type": "table", "table_name": table_name}
        )
        return bool(result.fetchone())

    def get_columns(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        query = text(
            f"PRAGMA table_info({self.identifier_preparer.quote_identifier(table_name)})"
        )
        result = connection.execute(query)

        columns = []
        for row in result:
            # SQLite PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            columns.append(
                {
                    "name": row[1],
                    "type": self._get_column_type(row[2]),
                    "nullable": not bool(row[3]),
                    "default": row[4],
                    "primary_key": bool(row[5]),
                }
            )

        return columns

    def _get_column_type(self, type_string: str) -> Any:
        """Convert SQLite type string to SQLAlchemy type."""
        type_string = type_string.upper()

        # Handle common SQLite type mappings
        if "INT" in type_string:
            return INTEGER()
        elif any(x in type_string for x in ["CHAR", "CLOB", "TEXT"]):
            return TEXT()
        elif any(x in type_string for x in ["REAL", "FLOA", "DOUBLE"]):
            return REAL()
        elif "BLOB" in type_string:
            return LargeBinary()
        elif "NUMERIC" in type_string:
            return NUMERIC()
        else:
            return TEXT()  # Default to TEXT for unknown types

    def get_pk_constraint(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> Dict[str, Any]:
        """Get primary key constraint information."""
        columns = self.get_columns(connection, table_name, schema, **kw)
        pk_columns = [col["name"] for col in columns if col["primary_key"]]

        return {
            "constrained_columns": pk_columns,
            "name": None,  # SQLite doesn't name PK constraints
        }

    def get_foreign_keys(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints."""
        query = text(
            f"PRAGMA foreign_key_list({self.identifier_preparer.quote_identifier(table_name)})"
        )
        result = connection.execute(query)

        # Group foreign keys by constraint
        fks = {}
        for row in result:
            # PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
            fk_id = row[0]
            if fk_id not in fks:
                fks[fk_id] = {
                    "name": None,
                    "constrained_columns": [],
                    "referred_table": row[2],
                    "referred_columns": [],
                    "options": {"onupdate": row[5], "ondelete": row[6]},
                }

            fks[fk_id]["constrained_columns"].append(row[3])
            fks[fk_id]["referred_columns"].append(row[4])

        return list(fks.values())

    def get_indexes(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        """Get index information."""
        query = text(
            f"PRAGMA index_list({self.identifier_preparer.quote_identifier(table_name)})"
        )
        result = connection.execute(query)

        indexes = []
        for row in result:
            # PRAGMA index_list returns: seq, name, unique, origin, partial
            index_name = row[1]
            if index_name.startswith("sqlite_autoindex_"):
                continue  # Skip auto-generated indexes

            # Get column information for this index
            col_query = text(
                f"PRAGMA index_info({self.identifier_preparer.quote_identifier(index_name)})"
            )
            col_result = connection.execute(col_query)

            column_names = []
            for col_row in col_result:
                # PRAGMA index_info returns: seqno, cid, name
                column_names.append(col_row[2])

            indexes.append(
                {
                    "name": index_name,
                    "column_names": column_names,
                    "unique": bool(row[2]),
                }
            )

        return indexes
