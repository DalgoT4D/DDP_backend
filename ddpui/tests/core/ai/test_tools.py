"""Tests for Chat with Data tools.

Tools are exercised via their raw functions (`tool.func`) with a stubbed
ToolRuntime carrying a fake RunContext — no LLM, no warehouse, no Django.
"""

from types import SimpleNamespace

import pytest

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import get_tools
from ddpui.core.ai.tools.schema_tools import (
    get_table_details,
    list_schemas,
    list_tables,
)
from ddpui.core.ai.tools.profile_tools import profile_column
from ddpui.core.ai.tools.sql_tools import execute_sql


class FakeWarehouse:
    """Slim Warehouse double: canned catalog + recorded execute() calls."""

    def __init__(self, rows=None):
        self.rows = rows if rows is not None else []
        self.executed = []
        self.columns = [
            {"name": "district", "data_type": "text"},
            {"name": "surveyed_at", "data_type": "date"},
        ]
        self.catalog_rows = [{"table_name": "surveys", "approx_rows": 1284}]

    def execute(self, sql):
        self.executed.append(sql)
        if "pg_catalog.pg_class" in sql or "__TABLES__" in sql:
            return self.catalog_rows
        return self.rows

    def get_table_columns(self, db_schema, db_table):
        return self.columns

    def column_exists(self, db_schema, db_table, column_name):
        return column_name in {col["name"] for col in self.columns}


def make_runtime(warehouse=None, dialect="postgres"):
    ctx = RunContext(
        org_id=1,
        org_slug="ngo",
        dialect=dialect,
        allowed_schemas=["prod"],
        max_result_rows=100,
        query_timeout_s=30,
        warehouse=warehouse or FakeWarehouse(),
    )
    return SimpleNamespace(context=ctx)


def test_execute_sql_happy_path_returns_content_and_artifact():
    warehouse = FakeWarehouse(rows=[{"district": "Pune", "n": 1284}])
    content, artifact = execute_sql.func(
        sql="SELECT district, COUNT(*) AS n FROM prod.surveys GROUP BY district",
        runtime=make_runtime(warehouse),
    )
    assert "1 rows" in content and "Pune" in content
    assert artifact["status"] == "success"
    assert artifact["columns"] == ["district", "n"]
    assert artifact["rows"] == [["Pune", "1284"]]
    assert "LIMIT 100" in artifact["sql"]


def test_execute_sql_rejects_writes_without_touching_warehouse():
    warehouse = FakeWarehouse()
    content, artifact = execute_sql.func(
        sql="DELETE FROM prod.surveys",
        runtime=make_runtime(warehouse),
    )
    assert content.startswith("SQL rejected:")
    assert artifact["status"] == "rejected"
    assert warehouse.executed == []


def test_execute_sql_returns_warehouse_error_as_feedback():
    class ExplodingWarehouse(FakeWarehouse):
        def execute(self, sql):
            raise RuntimeError('column "districtname" does not exist\nLINE 1: ...')

    content, artifact = execute_sql.func(
        sql="SELECT districtname FROM prod.surveys",
        runtime=make_runtime(ExplodingWarehouse()),
    )
    assert content.startswith("Query failed:")
    assert 'column "districtname" does not exist' in content
    assert "LINE 1" not in content  # only the first line goes back to the model
    assert artifact["status"] == "error"


def test_list_schemas_returns_allowlist():
    result = list_schemas.func(runtime=make_runtime())
    assert "prod" in result


def test_list_tables_shows_names_and_row_estimates():
    result = list_tables.func(schema_name="prod", runtime=make_runtime())
    assert "surveys" in result and "~1284 rows" in result


def test_list_tables_rejects_unknown_schema_with_guidance():
    result = list_tables.func(schema_name="secret", runtime=make_runtime())
    assert "not available" in result and "prod" in result


def test_get_table_details_renders_columns_without_row_data():
    warehouse = FakeWarehouse(rows=[{"district": "Pune", "surveyed_at": "2026-06-01"}])
    result = get_table_details.func(
        schema_name="prod", table_name="surveys", runtime=make_runtime(warehouse)
    )
    assert "district: text" in result
    # metadata only: no sample rows means no warehouse values reach the model here
    assert "Pune" not in result
    # only the catalog's pg_catalog lookup ran — no query touched the table's rows
    assert not any('prod"."surveys' in sql or "SELECT *" in sql for sql in warehouse.executed)


def test_get_table_details_rejects_unknown_table():
    result = get_table_details.func(schema_name="prod", table_name="nope", runtime=make_runtime())
    assert "not found" in result and "surveys" in result


def test_profile_column_quotes_identifiers_and_renders_values():
    warehouse = FakeWarehouse(rows=[{"value": "MH", "occurrences": 900}])
    result = profile_column.func(
        schema_name="prod",
        table_name="surveys",
        column_name="district",
        runtime=make_runtime(warehouse),
    )
    assert "MH" in result
    profile_sql = warehouse.executed[-1]
    assert '"district"' in profile_sql and '"prod"."surveys"' in profile_sql


def test_profile_column_unknown_column_gives_guidance():
    result = profile_column.func(
        schema_name="prod",
        table_name="surveys",
        column_name="districtname",
        runtime=make_runtime(),
    )
    assert "does not exist" in result and "get_table_details" in result


def test_execute_sql_sets_postgres_statement_timeout_on_same_connection():
    executed = []

    class FakeConnection:
        def execute(self, sql):
            executed.append(str(sql))
            return SimpleNamespace(fetchall=lambda: [])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class EngineWarehouse(FakeWarehouse):
        engine = SimpleNamespace(connect=FakeConnection)

    content, artifact = execute_sql.func(
        sql="SELECT * FROM prod.surveys",
        runtime=make_runtime(EngineWarehouse()),
    )
    assert artifact["status"] == "success"
    assert executed[0] == "SET statement_timeout = 30000"
    assert "LIMIT 100" in executed[1]


def test_registry_exposes_all_tools():
    names = {t.name for t in get_tools()}
    assert names == {
        "list_schemas",
        "list_tables",
        "get_table_details",
        "profile_column",
        "execute_sql",
        "create_chart",
        "list_dashboards",
        "create_dashboard",
        "add_charts_to_dashboard",
        "ask_user",
        "get_dalgo_help",
        "list_metrics",
        "list_kpis",
        "list_charts",
        "list_reports",
        "create_metric",
        "create_kpi",
        "create_report",
    }


def test_registry_names_filter_selects_a_subset_and_rejects_typos():
    import pytest as _pytest

    subset = get_tools(names=("execute_sql", "ask_user"))
    assert [t.name for t in subset] == ["execute_sql", "ask_user"]
    with _pytest.raises(KeyError, match="no_such_tool"):
        get_tools(names=("no_such_tool",))
