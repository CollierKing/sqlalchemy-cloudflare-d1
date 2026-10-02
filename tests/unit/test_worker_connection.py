"""Worker cursor regressions using canned D1 binding responses."""

import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest
from sqlalchemy import text

from sqlalchemy_cloudflare_d1.connection import (
    SyncWorkerConnection,
    WorkerConnection,
    create_engine_from_binding,
)


# MARK: - Binding Fixtures


class Statement:
    def __init__(self, raw_result, all_result):
        self.raw_result = raw_result
        self.all_result = all_result
        self.calls = []
        self.parameters = None

    def bind(self, *parameters):
        self.parameters = parameters
        return self

    async def raw(self, options):
        assert options["columnNames"] is True
        self.calls.append("raw")
        return self.raw_result

    async def all(self):
        self.calls.append("all")
        return self.all_result


class Binding:
    def __init__(self, statement):
        self.statement = statement

    def prepare(self, query):
        return self.statement


@pytest.fixture(autouse=True)
def worker_runtime(monkeypatch):
    """Supply only the Pyodide runtime boundary needed by these unit tests."""
    js = ModuleType("js")
    js.JSON = SimpleNamespace(parse=json.loads)
    pyodide = ModuleType("pyodide")
    ffi = ModuleType("pyodide.ffi")
    ffi.run_sync = asyncio.run
    monkeypatch.setitem(sys.modules, "js", js)
    monkeypatch.setitem(sys.modules, "pyodide", pyodide)
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi)


def execute(statement, sql, sync, parameters=None):
    binding = Binding(statement)
    connection = SyncWorkerConnection(binding) if sync else WorkerConnection(binding)
    cursor = connection.cursor()
    if sync:
        cursor.execute(sql, parameters)
    else:
        asyncio.run(cursor.execute_async(sql, parameters))
    return cursor


# MARK: - Positional Results


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("rows", [[[1, "x", 7]], [[1, None, 7], [2, "y", 8]]])
def test_duplicate_columns_keep_positional_values(sync, rows):
    statement = Statement(
        [["id", "name", "id"], *rows],
        {"results": [{"id": 7, "name": "x"}], "meta": {}},
    )
    cursor = execute(statement, "SELECT a.id, a.name, b.id FROM a JOIN b", sync)

    assert [column[0] for column in cursor.description] == ["id", "name", "id"]
    assert cursor.fetchone() == tuple(rows[0])
    assert cursor.fetchmany(2) == [tuple(row) for row in rows[1:]]
    assert cursor.fetchone() is None
    assert statement.calls == ["raw"]


def test_worker_engine_keeps_duplicate_columns():
    statement = Statement([["id", "id"], [1, 7]], {"results": [{"id": 7}], "meta": {}})
    engine = create_engine_from_binding(Binding(statement))
    try:
        with engine.connect() as connection:
            result = connection.execute(text("/* note */ SELECT 1 AS id, 7 AS id"))
            assert list(result.keys()) == ["id", "id"]
            assert result.fetchall() == [(1, 7)]
    finally:
        engine.dispose()


# MARK: - Empty Results


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        "-- note\nSELECT 1 AS x WHERE ?",
        "/* note */ SELECT 1 AS x WHERE ?",
        "WITH c AS (SELECT 1 AS x) SELECT x FROM c WHERE ?",
        'WITH "select" AS (SELECT 1 AS x) SELECT x FROM "select" WHERE ?',
        "WITH c(x) AS (SELECT '(') SELECT x FROM c WHERE ?",
        "WITH a AS (SELECT 1), select2 AS (SELECT 1 AS x) SELECT x FROM select2 WHERE ?",
    ],
)
def test_empty_select_preserves_columns_without_reexecution(sync, sql):
    statement = Statement([["x"]], {"results": [], "meta": {}})
    cursor = execute(statement, sql, sync, parameters=(0,))

    assert [column[0] for column in cursor.description] == ["x"]
    assert cursor.fetchall() == []
    assert statement.parameters == (0,)
    assert statement.calls == ["raw"]


# MARK: - Mutation Metadata


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (?)",
        "/* SELECT */ INSERT INTO t VALUES (?)",
        "WITH c AS (SELECT ?) INSERT INTO t SELECT * FROM c",
        "WITH select2 AS (SELECT ?) INSERT INTO t SELECT * FROM select2",
    ],
)
def test_mutations_keep_metadata_and_execute_once(sync, sql):
    statement = Statement(
        [], {"results": [], "meta": {"changes": 1, "last_row_id": 41}}
    )
    cursor = execute(statement, sql, sync, parameters=(1,))

    assert cursor.rowcount == 1
    assert cursor.lastrowid == 41
    assert statement.parameters == (1,)
    assert statement.calls == ["all"]


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("sql", ["VALUES (1, 2)", "EXPLAIN SELECT 1"])
def test_other_read_statements_keep_column_header(sync, sql):
    statement = Statement([["a", "b"], [1, 2]], {"results": [], "meta": {}})
    cursor = execute(statement, sql, sync)
    assert [column[0] for column in cursor.description] == ["a", "b"]
    assert cursor.fetchall() == [(1, 2)]
    assert statement.calls == ["raw"]
