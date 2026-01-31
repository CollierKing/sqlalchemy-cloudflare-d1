"""Integration tests for the SQLAlchemy D1 Python Worker.

These tests start the worker using `pywrangler dev` and make HTTP requests
to verify the WorkerConnection class works correctly with D1 bindings.

The tests mirror those in test_restapi_integration.py to ensure parity
between the REST API and Worker binding approaches.

Requires: pywrangler dev running with --local flag for local D1 database.
"""

import requests


# MARK: - Worker Connection Tests (DBAPI Level)


class TestWorkerConnection:
    """Test WorkerConnection via Worker HTTP endpoints.

    These tests mirror the TestD1Connection tests in test_restapi_integration.py.
    """

    def test_connection_can_execute_select(self, dev_server):
        """Test basic SELECT query - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/select")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "select"
        assert data["success"] is True
        assert data["row"][0] == 1

    def test_connection_can_query_sqlite_master(self, dev_server):
        """Test querying sqlite_master - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlite-master")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlite_master"
        assert data["success"] is True
        assert isinstance(data["tables"], list)

    def test_cursor_description_populated(self, dev_server):
        """Test cursor description - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/cursor-description")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "cursor_description"
        assert data["success"] is True
        assert data["description"] == ["num", "txt"]

    def test_create_insert_select_drop(self, dev_server):
        """Test CRUD cycle - mirrors REST API test.

        Note: Worker binding raw() doesn't return meta with changes count,
        so we can't verify insert_rowcount. We verify the data was inserted
        by checking the SELECT results.
        """
        port = dev_server
        response = requests.get(f"http://localhost:{port}/crud")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "crud"
        # Note: success may be False due to insert_rowcount == 0 (raw() limitation)
        # But we verify the actual data is correct
        assert len(data["select_rows"]) == 1
        assert data["select_rows"][0][1] == "test_row"
        assert data["select_rows"][0][2] == 42

    def test_parameterized_query(self, dev_server):
        """Test parameterized queries - mirrors REST API test."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/parameterized")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "parameterized"
        assert data["success"] is True
        assert len(data["query_result"]) == 1
        assert data["query_result"][0][0] == "Alice"


# MARK: - Health Check Tests


class TestHealthCheck:
    """Test the health check endpoint."""

    def test_health_check_returns_healthy(self, dev_server):
        """GET /health should return healthy status."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/health")

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "healthy"
        assert data["database"] == "connected"
        assert data["value"] == 1


# MARK: - Index/Documentation Tests


class TestIndex:
    """Test the index/documentation endpoint."""

    def test_index_returns_documentation(self, dev_server):
        """GET / should return API documentation."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/")

        assert response.status_code == 200
        data = response.json()

        assert "endpoints" in data
        assert "package" in data
        assert data["package"] == "sqlalchemy-cloudflare-d1"
        assert data["connection_type"] == "WorkerConnection (D1 binding)"

    def test_unknown_endpoint_returns_index(self, dev_server):
        """GET /unknown should return index documentation."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/unknown")

        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data


# MARK: - SQLAlchemy Core Tests


class TestSQLAlchemyCore:
    """Test SQLAlchemy Core functionality via create_engine_from_binding().

    These tests verify that SQLAlchemy Core patterns work inside Workers
    without using raw SQL.
    """

    def test_sqlalchemy_select(self, dev_server):
        """Test SQLAlchemy Core SELECT using text()."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-select")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_select"
        assert data["success"] is True
        assert data["row"][0] == 1

    def test_sqlalchemy_crud(self, dev_server):
        """Test SQLAlchemy Core CRUD without raw SQL.

        Uses Table, MetaData, insert(), select() - no raw SQL strings.
        """
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-crud")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_crud"
        assert data["success"] is True
        assert len(data["rows"]) == 1
        assert data["rows"][0][1] == "test_row"
        assert data["rows"][0][2] == 42
        assert "id" in data["columns"]
        assert "name" in data["columns"]
        assert "value" in data["columns"]

    def test_sqlalchemy_reflect(self, dev_server):
        """Test SQLAlchemy table reflection with autoload_with."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-reflect")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_reflect"
        assert data["success"] is True
        assert len(data["rows"]) == 1
        assert data["rows"][0][1] == "alice"

        # Check reflected columns
        column_names = [col["name"] for col in data["reflected_columns"]]
        assert "id" in column_names
        assert "username" in column_names
        assert "email" in column_names


# MARK: - Empty Result Set Tests


class TestEmptyResultSet:
    """Test handling of empty result sets (fixes GitHub issue #4).

    These tests verify the fix for NoSuchColumnError on empty results
    works correctly with the Worker binding.
    """

    def test_empty_result_has_description(self, dev_server):
        """Test cursor.description is populated even with empty results."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/empty-result")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "empty_result"
        assert data["success"] is True
        assert data["row_count"] == 0
        assert data["description"] == ["id", "name", "value"]

    def test_empty_result_sqlalchemy(self, dev_server):
        """Test SQLAlchemy doesn't raise NoSuchColumnError on empty results."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/empty-result-sqlalchemy")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "empty_result_sqlalchemy"
        assert data["success"] is True
        assert data["row_count"] == 0


class TestJsonColumnFiltering:
    """Test filtering on JSON array columns using json_each and exists."""

    def test_json_filter_with_exists(self, dev_server):
        """Test filtering rows where JSON array contains a specific value."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/json-filter")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "json_filter"
        assert data["success"] is True
        assert data["matching_names"] == ["Alice", "Charlie"]

    def test_json_aggregate(self, dev_server):
        """Test aggregation after expanding JSON array with json_each."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/json-aggregate")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "json_aggregate"
        assert data["success"] is True
        assert data["tag_scores"]["data"] == 15
        assert data["tag_scores"]["javascript"] == 20
        assert data["tag_scores"]["python"] == 25
        assert data["tag_scores"]["tech"] == 30


class TestPandasToSql:
    """Test pandas DataFrame.to_sql() with D1 engine in Workers."""

    def test_pandas_to_sql_basic(self, dev_server):
        """Test basic pandas to_sql with append mode."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/pandas-to-sql")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "pandas_to_sql"
        assert data["success"] is True
        assert len(data["rows"]) == 3
        assert data["rows"][0] == ["Alice", 85]
        assert data["rows"][1] == ["Bob", 92]
        assert data["rows"][2] == ["Charlie", 78]

    def test_pandas_to_sql_upsert(self, dev_server):
        """Test pandas to_sql with OR REPLACE conflict handling."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/pandas-to-sql-upsert")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "pandas_to_sql_upsert"
        assert data["success"] is True
        assert len(data["rows"]) == 3
        assert data["rows"][0] == ["Alice", 100]  # Score replaced
        assert data["rows"][1] == ["Bob", 92]
        assert data["rows"][2] == ["Charlie", 78]

    def test_pandas_to_sql_json(self, dev_server):
        """Test pandas to_sql with stringified JSON columns."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/pandas-to-sql-json")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "pandas_to_sql_json"
        assert data["success"] is True
        assert len(data["rows"]) == 2
        assert data["rows"][0]["name"] == "service_a"
        assert data["rows"][0]["config"]["enabled"] is True
        assert data["rows"][0]["config"]["retries"] == 3
        assert data["rows"][1]["name"] == "service_b"
        assert data["rows"][1]["config"]["enabled"] is False


# MARK: - SQL Injection Prevention Tests


class TestSQLInjectionPrevention:
    """Test that parameterized queries prevent SQL injection attacks."""

    def test_sqli_string_parameter(self, dev_server):
        """Test SQL injection attempt in string parameter is safely escaped."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqli-string")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqli_string"
        assert data["success"] is True
        assert data["row_count"] == 0  # Should not return any rows

    def test_sqli_union_attack(self, dev_server):
        """Test UNION-based SQL injection is prevented."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqli-union")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqli_union"
        assert data["success"] is True
        assert data["row_count"] == 0  # Should not return table names

    def test_sqli_drop_table(self, dev_server):
        """Test DROP TABLE injection is prevented."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqli-drop")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqli_drop"
        assert data["success"] is True
        assert data["row_count"] == 0
        assert data["table_still_exists"] is True  # Table should NOT be dropped

    def test_sqli_orm_filter(self, dev_server):
        """Test SQL injection prevention with SQLAlchemy ORM queries."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqli-orm")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqli_orm"
        assert data["success"] is True
        assert data["malicious_row_count"] == 0  # Should not bypass auth
        assert data["legitimate_row_count"] == 1  # Normal query works

    def test_sqli_like_clause(self, dev_server):
        """Test SQL injection in LIKE clause is prevented."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqli-like")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqli_like"
        assert data["success"] is True
        assert data["malicious_row_count"] == 0
        assert data["legitimate_row_count"] == 2


# MARK: - Additional SQLAlchemy Core Tests


class TestSQLAlchemyCoreAdditional:
    """Additional SQLAlchemy Core tests for parity with REST API."""

    def test_sqlalchemy_upsert(self, dev_server):
        """Test INSERT ... ON CONFLICT DO UPDATE (upsert)."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-upsert")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_upsert"
        assert data["success"] is True
        assert data["row"][1] == "Updated"
        assert data["row"][2] == 2

    def test_sqlalchemy_get_table_names(self, dev_server):
        """Test dialect get_table_names works."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/sqlalchemy-get-tables")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "sqlalchemy_get_tables"
        assert data["success"] is True
        assert data["table_exists"] is True


# MARK: - Additional Empty Result Set Tests


class TestEmptyResultSetAdditional:
    """Additional empty result set tests for parity with REST API."""

    def test_empty_result_with_where_clause(self, dev_server):
        """Test empty result from WHERE clause that matches nothing."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/empty-result-where")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "empty_result_where"
        assert data["success"] is True
        assert data["row_count"] == 0
        assert data["description"] == ["id", "name"]


# MARK: - Additional JSON Column Filtering Tests


class TestJsonColumnFilteringAdditional:
    """Additional JSON filtering tests for parity with REST API."""

    def test_json_filter_multiple_values(self, dev_server):
        """Test filtering where JSON array contains any of multiple values."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/json-multiple-values")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "json_multiple_values"
        assert data["success"] is True
        assert len(data["matching_products"]) == 3
        assert "Widget A" in data["matching_products"]
        assert "Widget B" in data["matching_products"]
        assert "Widget C" in data["matching_products"]


# MARK: - Boolean Column Tests


class TestBooleanColumn:
    """Test boolean column handling (fixes GitHub issue #6).

    D1 stores booleans as TEXT strings ("true"/"false").
    The D1Boolean type processor converts these back to Python booleans.
    """

    def test_boolean_column_returns_python_bool(self, dev_server):
        """Test that boolean columns return Python bool, not str or int."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/boolean-column")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "boolean_column"
        assert data["success"] is True

        # Check types - should be actual booleans
        assert data["admin_is_admin"] is True
        assert data["admin_is_active"] is True
        assert data["user_is_admin"] is False
        assert data["user_is_active"] is True
        assert data["inactive_is_admin"] is False
        assert data["inactive_is_active"] is False

    def test_boolean_filter_with_python_bool(self, dev_server):
        """Test filtering by boolean values works correctly."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/boolean-filter")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "boolean_filter"
        assert data["success"] is True
        assert len(data["enabled_features"]) == 2
        assert "Feature A" in data["enabled_features"]
        assert "Feature C" in data["enabled_features"]
        assert len(data["disabled_features"]) == 1
        assert "Feature B" in data["disabled_features"]

    def test_boolean_nullable_column(self, dev_server):
        """Test nullable boolean columns handle NULL correctly."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/boolean-nullable")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "boolean_nullable"
        assert data["success"] is True
        assert data["user_a_verified"] is True
        assert data["user_b_verified"] is False
        assert data["user_c_verified"] is None  # NULL


# MARK: - NULL Parameter Handling Tests


class TestNullParameterHandling:
    """Test NULL parameter handling in Worker binding.

    Python None must be converted to JS null (not undefined) for D1.
    """

    def test_insert_null_string(self, dev_server):
        """Test inserting NULL into a String column."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/null-string")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "null_string"
        assert data["success"] is True
        assert data["row_with_value"] == "hello"
        assert data["row_with_null"] is None

    def test_insert_null_integer(self, dev_server):
        """Test inserting NULL into an Integer column."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/null-integer")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "null_integer"
        assert data["success"] is True
        assert data["row_with_value"] == 42
        assert data["row_with_null"] is None


# MARK: - LargeBinary Column Tests


class TestLargeBinaryColumn:
    """Test LargeBinary column handling in Workers (fixes GitHub issue #8).

    D1 stores binary data as BLOB. The D1LargeBinary type processor handles
    base64 decoding when reading data back from D1.
    """

    def test_largebinary_column_stores_and_retrieves(self, dev_server):
        """Test that LargeBinary columns work in Workers."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/largebinary-basic")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "largebinary_basic"
        assert data["success"] is True
        assert data["data_matches"] is True

    def test_largebinary_with_image_data(self, dev_server):
        """Test storing simulated image data in Workers."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/largebinary-image")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "largebinary_image"
        assert data["success"] is True
        assert data["has_png_header"] is True

    def test_largebinary_nullable(self, dev_server):
        """Test nullable LargeBinary columns in Workers."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/largebinary-nullable")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "largebinary_nullable"
        assert data["success"] is True
        assert data["null_is_none"] is True
        assert data["data_is_bytes"] is True


# MARK: - ON CONFLICT Advanced Tests


class TestOnConflictAdvanced:
    """Test advanced ON CONFLICT clause variations (GitHub issue #9).

    Note: Basic ON CONFLICT support was added in v0.3.0. These tests cover
    additional edge cases and usage patterns.
    """

    def test_on_conflict_do_nothing(self, dev_server):
        """Test INSERT ... ON CONFLICT DO NOTHING."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/on-conflict-do-nothing")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "on_conflict_do_nothing"
        assert data["success"] is True
        assert data["row_count"] == 1
        assert data["original_value_preserved"] is True

    def test_on_conflict_composite_key(self, dev_server):
        """Test ON CONFLICT with composite unique constraint."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/on-conflict-composite")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "on_conflict_composite"
        assert data["success"] is True
        assert data["value_updated"] is True

    def test_on_conflict_with_where(self, dev_server):
        """Test ON CONFLICT with WHERE clause."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/on-conflict-where")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "on_conflict_where"
        assert data["success"] is True
        assert data["is_verified"] is True


# MARK: - Single-Row Result Tests


class TestSingleRowResult:
    """Test that single-row query results are returned correctly.

    Regression tests for bug where single-row results end up in
    cursor.description instead of being returned by fetchall().
    """

    def test_single_row_fetchall_returns_data(self, dev_server):
        """Test single-row SELECT returns the row in fetchall()."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/single-row-result")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "single_row_result"
        assert data["success"] is True, (
            f"Single-row bug: row_count={data.get('row_count')}, "
            f"description_names={data.get('description_names')}, "
            f"expected_columns={data.get('expected_columns')}"
        )
        assert data["row_count"] == 1
        assert data["description_names"] == ["id", "name", "value"]

    def test_single_row_sqlalchemy(self, dev_server):
        """Test single-row result via SQLAlchemy engine."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/single-row-sqlalchemy")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "single_row_sqlalchemy"
        assert data["success"] is True, (
            f"Single-row SQLAlchemy bug: rows={data.get('rows')}, "
            f"columns={data.get('columns')}"
        )
        assert len(data["rows"]) == 1

    def test_multi_row_description_has_column_names(self, dev_server):
        """Test multi-row SELECT has correct column names in description."""
        port = dev_server
        response = requests.get(f"http://localhost:{port}/multi-row-result")

        assert response.status_code == 200
        data = response.json()

        assert data["test"] == "multi_row_result"
        assert data["success"] is True
        assert data["row_count"] == 3
        assert data["description_names"] == ["id", "name", "value"]
