"""Unit tests for generate_cast_sql() on PostgresClient and BigqueryClient."""
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from ddpui.utils.warehouse.client.postgres import PostgresClient, POSTGRES_CAST_TYPE_MAP
from ddpui.utils.warehouse.client.bigquery import BigqueryClient, BIGQUERY_CAST_TYPE_MAP


# ---------------------------------------------------------------------------
# Helpers — build clients with mocked engines so no real DB connections
# ---------------------------------------------------------------------------


def _postgres_client():
    """PostgresClient with a mocked engine + dialect preparer."""
    with patch("ddpui.utils.warehouse.client.postgres.create_engine"), patch(
        "ddpui.utils.warehouse.client.postgres.inspect"
    ):
        client = PostgresClient.__new__(PostgresClient)
        preparer = MagicMock()
        # Simulate double-quote quoting (standard Postgres behaviour)
        preparer.quote.side_effect = lambda s: f'"{s}"'
        preparer.quote_schema.side_effect = lambda s: f'"{s}"'
        engine = MagicMock()
        engine.dialect.identifier_preparer = preparer
        client.engine = engine
        client.inspect_obj = MagicMock()
        return client


def _bigquery_client(columns: list[str]):
    """BigqueryClient with a mocked engine + dialect preparer."""
    with patch("ddpui.utils.warehouse.client.bigquery.create_engine"), patch(
        "ddpui.utils.warehouse.client.bigquery.inspect"
    ):
        client = BigqueryClient.__new__(BigqueryClient)
        preparer = MagicMock()
        # Simulate backtick quoting (BigQuery behaviour)
        preparer.quote.side_effect = lambda s: f"`{s}`"
        engine = MagicMock()
        engine.dialect.identifier_preparer = preparer
        engine.url.host = "my-project"
        client.engine = engine
        client.inspect_obj = MagicMock()
        # Stub get_table_columns to return the provided column list
        client.get_table_columns = MagicMock(return_value=[{"name": c} for c in columns])
        return client


# ---------------------------------------------------------------------------
# PostgresClient.generate_cast_sql
# ---------------------------------------------------------------------------


def test_postgres_empty_casts():
    client = _postgres_client()
    assert client.generate_cast_sql("myschema", "mytable", {}) == ""


def test_postgres_single_cast():
    client = _postgres_client()
    sql = client.generate_cast_sql("dest", "orders", {"amount": "numeric"})
    assert (
        sql
        == 'ALTER TABLE "dest"."orders"\n  ALTER COLUMN "amount" TYPE numeric USING "amount"::numeric'
    )


def test_postgres_multiple_casts():
    client = _postgres_client()
    sql = client.generate_cast_sql(
        "dest", "orders", {"amount": "numeric", "created_at": "timestamp"}
    )
    assert '"amount"' in sql
    assert '"created_at"' in sql
    assert "ALTER TABLE" in sql


def test_postgres_all_supported_types():
    client = _postgres_client()
    for type_key in POSTGRES_CAST_TYPE_MAP:
        sql = client.generate_cast_sql("s", "t", {"col": type_key})
        assert sql != ""


def test_postgres_unknown_type_raises():
    client = _postgres_client()
    with pytest.raises(ValueError, match="Unsupported cast type for Postgres"):
        client.generate_cast_sql("s", "t", {"col": "jsonb"})


# ---------------------------------------------------------------------------
# BigqueryClient.generate_cast_sql
# ---------------------------------------------------------------------------


def test_bigquery_empty_casts_no_columns():
    client = _bigquery_client([])
    assert client.generate_cast_sql("dest", "orders", {}) == ""


def test_bigquery_no_casts_passthrough():
    client = _bigquery_client(["id", "name"])
    sql = client.generate_cast_sql("dest", "orders", {})
    # no CASTs — all columns pass through unchanged
    assert "CREATE OR REPLACE TABLE" in sql
    assert "CAST" not in sql
    assert "`id`" in sql
    assert "`name`" in sql


def test_bigquery_single_cast():
    client = _bigquery_client(["id", "amount"])
    sql = client.generate_cast_sql("dest", "orders", {"amount": "numeric"})
    assert "CREATE OR REPLACE TABLE `my-project.dest.orders`" in sql
    assert "CAST(`amount` AS NUMERIC) AS `amount`" in sql
    assert "`id`" in sql  # non-cast column passes through


def test_bigquery_includes_airbyte_meta_columns():
    client = _bigquery_client(["id", "amount", "_airbyte_raw_id", "_airbyte_extracted_at"])
    sql = client.generate_cast_sql("dest", "orders", {"amount": "numeric"})
    assert "`_airbyte_raw_id`" in sql
    assert "`_airbyte_extracted_at`" in sql


def test_bigquery_all_supported_types():
    for type_key, bq_type in BIGQUERY_CAST_TYPE_MAP.items():
        client = _bigquery_client(["col"])
        sql = client.generate_cast_sql("s", "t", {"col": type_key})
        assert f"CAST(`col` AS {bq_type})" in sql


def test_bigquery_unknown_type_raises():
    client = _bigquery_client(["col"])
    with pytest.raises(ValueError, match="Unsupported cast type for BigQuery"):
        client.generate_cast_sql("s", "t", {"col": "STRUCT"})


def test_bigquery_fetches_live_columns():
    """generate_cast_sql must call get_table_columns so Airbyte meta cols are included."""
    client = _bigquery_client(["id"])
    client.generate_cast_sql("dest", "orders", {})
    client.get_table_columns.assert_called_once_with("dest", "orders")
