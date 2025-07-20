"""
SQL compilation for Cloudflare D1 dialect.
"""

from sqlalchemy.sql import compiler
from sqlalchemy import schema
from sqlalchemy.sql.sqltypes import (
    String,
    Integer,
    Numeric,
    DateTime,
    Boolean,
)


class CloudflareD1Compiler(compiler.SQLCompiler):
    """SQL compiler for Cloudflare D1 (SQLite-based)."""

    def limit_clause(self, select, **kw):
        """Handle LIMIT clause for SQLite."""
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                # SQLite requires LIMIT when using OFFSET
                text += "\n LIMIT -1"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_true(self, element, **kw):
        """Handle boolean TRUE."""
        return "1"

    def visit_false(self, element, **kw):
        """Handle boolean FALSE."""
        return "0"

    def visit_mod_binary(self, binary, operator, **kw):
        """Handle modulo operator."""
        return (
            self.process(binary.left, **kw) + " % " + self.process(binary.right, **kw)
        )

    def visit_now_func(self, fn, **kw):
        """Handle CURRENT_TIMESTAMP function."""
        return "CURRENT_TIMESTAMP"

    def visit_char_length_func(self, fn, **kw):
        """Handle CHAR_LENGTH function."""
        return "length" + self.function_argspec(fn, **kw)

    def visit_cast(self, cast, **kw):
        """Handle CAST operations."""
        type_ = cast.typeclause.type

        # Map SQLAlchemy types to SQLite types
        if isinstance(type_, String):
            sqlite_type = "TEXT"
        elif isinstance(type_, Integer):
            sqlite_type = "INTEGER"
        elif isinstance(type_, Numeric):
            sqlite_type = "NUMERIC"
        elif isinstance(type_, DateTime):
            sqlite_type = "TEXT"  # SQLite stores datetime as TEXT
        elif isinstance(type_, Boolean):
            sqlite_type = "INTEGER"  # SQLite stores boolean as INTEGER
        else:
            sqlite_type = "TEXT"  # Default to TEXT for unknown types

        return "CAST(%s AS %s)" % (self.process(cast.clause, **kw), sqlite_type)

    def visit_extract(self, extract, **kw):
        """Handle EXTRACT function."""
        field = extract.field
        expr = self.process(extract.expr, **kw)

        # SQLite datetime extraction functions
        if field == "year":
            return f"CAST(strftime('%Y', {expr}) AS INTEGER)"
        elif field == "month":
            return f"CAST(strftime('%m', {expr}) AS INTEGER)"
        elif field == "day":
            return f"CAST(strftime('%d', {expr}) AS INTEGER)"
        elif field == "hour":
            return f"CAST(strftime('%H', {expr}) AS INTEGER)"
        elif field == "minute":
            return f"CAST(strftime('%M', {expr}) AS INTEGER)"
        elif field == "second":
            return f"CAST(strftime('%S', {expr}) AS INTEGER)"
        elif field == "dow":  # day of week
            return f"CAST(strftime('%w', {expr}) AS INTEGER)"
        elif field == "doy":  # day of year
            return f"CAST(strftime('%j', {expr}) AS INTEGER)"
        else:
            return f"strftime('%{field}', {expr})"

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        """Handle REGEXP operator."""
        return "%s REGEXP %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        """Handle REGEXP_REPLACE (not natively supported in SQLite)."""
        # SQLite doesn't have native REGEXP_REPLACE, would need custom function
        raise NotImplementedError("REGEXP_REPLACE not supported in SQLite/D1")


class CloudflareD1DDLCompiler(compiler.DDLCompiler):
    """DDL compiler for Cloudflare D1."""

    def visit_create_table(self, create, **kw):
        """Handle CREATE TABLE statements."""
        # Use the base implementation but ensure SQLite compatibility
        table = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if create.if_not_exists:
            text += "TABLE IF NOT EXISTS "
        else:
            text += "TABLE "

        text += preparer.format_table(table)
        text += " ("

        # Column definitions
        separator = "\n"
        for column in table.columns:
            text += separator
            text += "\t" + self.get_column_specification(column, **kw)
            separator = ", \n"

        # Constraints
        for constraint in table.constraints:
            if constraint.name is not None or not isinstance(
                constraint, schema.PrimaryKeyConstraint
            ):
                text += separator
                text += "\t" + self.process(constraint, **kw)
                separator = ", \n"

        text += "\n)"
        return text

    def get_column_specification(self, column, **kw):
        """Get column specification for CREATE TABLE."""
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(
            column.type, type_expression=column
        )

        # Handle primary key
        if column.primary_key:
            if column.autoincrement:
                colspec += " PRIMARY KEY AUTOINCREMENT"
            else:
                colspec += " PRIMARY KEY"

        # Handle nullable
        if not column.nullable:
            colspec += " NOT NULL"

        # Handle default
        if column.default is not None:
            colspec += " DEFAULT " + self.process(column.default.arg, **kw)

        return colspec

    def visit_drop_table(self, drop, **kw):
        """Handle DROP TABLE statements."""
        text = "\nDROP TABLE "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_table(drop.element)
        return text

    def visit_create_index(self, create, **kw):
        """Handle CREATE INDEX statements."""
        index = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX "

        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += preparer.quote_identifier(index.name)
        text += " ON " + preparer.format_table(index.table)
        text += " ("

        text += ", ".join(preparer.quote_identifier(c.name) for c in index.columns)
        text += ")"

        return text

    def visit_drop_index(self, drop, **kw):
        """Handle DROP INDEX statements."""
        text = "\nDROP INDEX "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.quote_identifier(drop.element.name)
        return text


class CloudflareD1TypeCompiler(compiler.GenericTypeCompiler):
    """Type compiler for Cloudflare D1."""

    def visit_TEXT(self, type_, **kw):
        """Handle TEXT type."""
        return "TEXT"

    def visit_STRING(self, type_, **kw):
        """Handle STRING/VARCHAR type."""
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "TEXT"

    def visit_VARCHAR(self, type_, **kw):
        """Handle VARCHAR type."""
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "TEXT"

    def visit_CHAR(self, type_, **kw):
        """Handle CHAR type."""
        if type_.length:
            return f"CHAR({type_.length})"
        return "TEXT"

    def visit_INTEGER(self, type_, **kw):
        """Handle INTEGER type."""
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        """Handle BIGINT type."""
        return "INTEGER"  # SQLite treats all integers the same

    def visit_SMALLINT(self, type_, **kw):
        """Handle SMALLINT type."""
        return "INTEGER"  # SQLite treats all integers the same

    def visit_NUMERIC(self, type_, **kw):
        """Handle NUMERIC type."""
        if type_.precision is not None and type_.scale is not None:
            return f"NUMERIC({type_.precision}, {type_.scale})"
        elif type_.precision is not None:
            return f"NUMERIC({type_.precision})"
        return "NUMERIC"

    def visit_DECIMAL(self, type_, **kw):
        """Handle DECIMAL type."""
        return self.visit_NUMERIC(type_, **kw)

    def visit_REAL(self, type_, **kw):
        """Handle REAL type."""
        return "REAL"

    def visit_FLOAT(self, type_, **kw):
        """Handle FLOAT type."""
        return "REAL"  # SQLite uses REAL for floating point

    def visit_BOOLEAN(self, type_, **kw):
        """Handle BOOLEAN type."""
        return "INTEGER"  # SQLite stores boolean as INTEGER

    def visit_DATE(self, type_, **kw):
        """Handle DATE type."""
        return "TEXT"  # SQLite stores dates as TEXT

    def visit_TIME(self, type_, **kw):
        """Handle TIME type."""
        return "TEXT"  # SQLite stores times as TEXT

    def visit_DATETIME(self, type_, **kw):
        """Handle DATETIME type."""
        return "TEXT"  # SQLite stores datetimes as TEXT

    def visit_TIMESTAMP(self, type_, **kw):
        """Handle TIMESTAMP type."""
        return "TEXT"  # SQLite stores timestamps as TEXT

    def visit_BLOB(self, type_, **kw):
        """Handle BLOB type."""
        return "TEXT"  # D1 doesn't support true BLOB, use TEXT

    def visit_CLOB(self, type_, **kw):
        """Handle CLOB type."""
        return "TEXT"
