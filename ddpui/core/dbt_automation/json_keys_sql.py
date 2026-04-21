from collections.abc import Iterable
import re
from typing import Any

from ddpui.core.dbt_automation.utils.columnutils import quote_columnname
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


DEFAULT_JSON_KEY_SAMPLE_LIMIT = 1000

_PG_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_BQ_DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BQ_PROJECT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]*$")


def _quote_pg_identifier(identifier: str) -> str:
    """Quote a Postgres identifier with double quotes and escaping."""
    return '"' + identifier.replace('"', '""') + '"'


def _sanitize_bq_identifier(identifier: str) -> str:
    """Strip BigQuery backtick characters from an identifier fragment."""
    return identifier.replace("`", "")


def validate_json_key_source_identifiers(
    wtype: str,
    schema: str,
    table: str,
    column: str,
) -> None:
    """Validate schema/table/column identifiers before building dynamic SQL."""
    if wtype == WarehouseType.POSTGRES.value:
        if not _PG_IDENTIFIER_RE.fullmatch(schema):
            raise ValueError(f"Invalid postgres schema name: {schema}")
        if not _PG_IDENTIFIER_RE.fullmatch(table):
            raise ValueError(f"Invalid postgres table name: {table}")
        if not _PG_IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"Invalid postgres column name: {column}")
        return

    if wtype == WarehouseType.BIGQUERY.value:
        if "." in schema:
            project, dataset = schema.split(".", 1)
            if not _BQ_PROJECT_RE.fullmatch(project):
                raise ValueError(f"Invalid bigquery project name: {project}")
            if not _BQ_DATASET_RE.fullmatch(dataset):
                raise ValueError(f"Invalid bigquery dataset name: {dataset}")
        elif not _BQ_DATASET_RE.fullmatch(schema):
            raise ValueError(f"Invalid bigquery dataset name: {schema}")

        if not _BQ_DATASET_RE.fullmatch(table):
            raise ValueError(f"Invalid bigquery table name: {table}")
        if not _BQ_DATASET_RE.fullmatch(column):
            raise ValueError(f"Invalid bigquery column name: {column}")


def format_bq_table(schema: str, table: str) -> str:
    """Build a BigQuery table reference from dataset/table or project.dataset/table."""
    table_name = _sanitize_bq_identifier(table)
    if "." in schema:
        project, dataset = schema.split(".", 1)
        project_name = _sanitize_bq_identifier(project)
        dataset_name = _sanitize_bq_identifier(dataset)
        return f"`{project_name}.{dataset_name}.{table_name}`"

    dataset_name = _sanitize_bq_identifier(schema)
    return f"`{dataset_name}.{table_name}`"


def json_keys_query(
    wtype: str,
    schema: str,
    table: str,
    column: str,
    limit: int = DEFAULT_JSON_KEY_SAMPLE_LIMIT,
) -> str:
    """Generate warehouse-specific SQL to infer top-level JSON keys from sampled rows."""
    validate_json_key_source_identifiers(wtype, schema, table, column)

    sample_limit = limit if isinstance(limit, int) and limit > 0 else DEFAULT_JSON_KEY_SAMPLE_LIMIT

    if wtype == WarehouseType.POSTGRES.value:
        quoted_column = quote_columnname(column, WarehouseType.POSTGRES.value)
        quoted_schema = _quote_pg_identifier(schema)
        quoted_table = _quote_pg_identifier(table)
        return f"""
            WITH sample AS (
                SELECT {quoted_column} AS payload
                FROM {quoted_schema}.{quoted_table}
                WHERE {quoted_column} IS NOT NULL
                LIMIT {sample_limit}
            )
            SELECT DISTINCT key
            FROM sample
            CROSS JOIN LATERAL jsonb_object_keys(
                CASE
                    WHEN sample.payload IS NULL THEN '{{}}'::jsonb
                    WHEN NULLIF(BTRIM(sample.payload::text), '') IS NULL THEN '{{}}'::jsonb
                    WHEN LEFT(BTRIM(sample.payload::text), 1) <> '{{' THEN '{{}}'::jsonb
                    WHEN jsonb_typeof(COALESCE(NULLIF(BTRIM(sample.payload::text), '')::jsonb, '{{}}'::jsonb)) = 'object'
                        THEN COALESCE(NULLIF(BTRIM(sample.payload::text), '')::jsonb, '{{}}'::jsonb)
                    ELSE '{{}}'::jsonb
                END
            ) AS key
        """

    if wtype == WarehouseType.BIGQUERY.value:
        quoted_column = quote_columnname(column, WarehouseType.BIGQUERY.value)
        bq_table = format_bq_table(schema, table)
        return f"""
            WITH sample AS (
                SELECT {quoted_column} AS payload
                FROM {bq_table}
                WHERE {quoted_column} IS NOT NULL
                LIMIT {sample_limit}
            )
            SELECT DISTINCT key
            FROM sample
            CROSS JOIN UNNEST(
                IFNULL(JSON_KEYS(SAFE.PARSE_JSON(CAST(sample.payload AS STRING)), 1), [])
            ) AS key
        """

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def _extract_key(row: Any) -> str | None:
    """Extract a key value from dict, tuple/list, or row mapping objects."""
    if isinstance(row, dict):
        return row.get("key")

    if isinstance(row, (list, tuple)):
        return row[0] if row else None

    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping.get("key")

    return None


def extract_json_keys(rows: Iterable[Any]) -> list[str]:
    """Extract non-empty key values from an iterable of query result rows."""
    return [key for key in (_extract_key(row) for row in rows) if key]


def infer_json_keys(
    client,
    wtype: str,
    schema: str,
    table: str,
    column: str,
    limit: int = DEFAULT_JSON_KEY_SAMPLE_LIMIT,
) -> list[str]:
    """Execute inferred-key SQL and return distinct JSON keys."""
    query = json_keys_query(wtype, schema, table, column, limit=limit)
    rows = client.execute(query)
    return extract_json_keys(rows)
