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


def _bigquery_client():
    """BigqueryClient with a mocked engine + dialect preparer.
    No live column fetching — the SQL uses SELECT * REPLACE(...) so the
    warehouse doesn't need to be queried at SQL-generation time."""
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


def test_bigquery_empty_casts_returns_empty_string():
    """No casts → empty string, no SQL to run."""
    client = _bigquery_client()
    assert client.generate_cast_sql("dest", "orders", {}) == ""


def test_bigquery_single_cast_uses_select_star_replace():
    client = _bigquery_client()
    sql = client.generate_cast_sql("dest", "orders", {"amount": "numeric"})
    assert "CREATE OR REPLACE TABLE `my-project.dest.orders`" in sql
    assert "SELECT * REPLACE" in sql
    assert "CAST(`amount` AS NUMERIC) AS `amount`" in sql


def test_bigquery_multiple_casts():
    client = _bigquery_client()
    sql = client.generate_cast_sql(
        "dest", "orders", {"amount": "numeric", "created_at": "timestamp"}
    )
    assert "SELECT * REPLACE" in sql
    assert "CAST(`amount` AS NUMERIC) AS `amount`" in sql
    assert "CAST(`created_at` AS TIMESTAMP) AS `created_at`" in sql


def test_bigquery_all_supported_types():
    for type_key, bq_type in BIGQUERY_CAST_TYPE_MAP.items():
        client = _bigquery_client()
        sql = client.generate_cast_sql("s", "t", {"col": type_key})
        assert f"CAST(`col` AS {bq_type})" in sql


def test_bigquery_unknown_type_raises():
    client = _bigquery_client()
    with pytest.raises(ValueError, match="Unsupported cast type for BigQuery"):
        client.generate_cast_sql("s", "t", {"col": "STRUCT"})


def test_bigquery_column_name_normalization():
    """Airbyte Destinations V2: non-alphanumeric chars → underscore, case preserved."""
    client = _bigquery_client()
    sql = client.generate_cast_sql("dest", "orders", {"Measure 7": "numeric"})
    assert "CAST(`Measure_7` AS NUMERIC) AS `Measure_7`" in sql
