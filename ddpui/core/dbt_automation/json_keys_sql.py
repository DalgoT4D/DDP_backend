from collections.abc import Iterable
from typing import Any

from ddpui.core.dbt_automation.utils.columnutils import quote_columnname
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


DEFAULT_JSON_KEY_SAMPLE_LIMIT = 1000


def format_bq_table(schema: str, table: str) -> str:
    # Keep table rendering centralized so project.dataset handling can evolve in one place.
    if "." in schema:
        return f"`{schema}.{table}`"
    return f"`{schema}.{table}`"


def json_keys_query(
    wtype: str,
    schema: str,
    table: str,
    column: str,
    limit: int = DEFAULT_JSON_KEY_SAMPLE_LIMIT,
) -> str:
    sample_limit = limit if isinstance(limit, int) and limit > 0 else DEFAULT_JSON_KEY_SAMPLE_LIMIT

    if wtype == WarehouseType.POSTGRES.value:
        quoted_column = quote_columnname(column, WarehouseType.POSTGRES.value)
        return f"""
            WITH sample AS (
                SELECT {quoted_column} AS payload
                FROM "{schema}"."{table}"
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
                IFNULL(JSON_KEYS(SAFE.PARSE_JSON(CAST(sample.payload AS STRING))), [])
            ) AS key
        """

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def _extract_key(row: Any) -> str | None:
    if isinstance(row, dict):
        return row.get("key")

    if isinstance(row, (list, tuple)):
        return row[0] if row else None

    try:
        return row["key"]
    except Exception:
        return None


def extract_json_keys(rows: Iterable[Any]) -> list[str]:
    return [key for key in (_extract_key(row) for row in rows) if key]


def infer_json_keys(
    client,
    wtype: str,
    schema: str,
    table: str,
    column: str,
    limit: int = DEFAULT_JSON_KEY_SAMPLE_LIMIT,
) -> list[str]:
    query = json_keys_query(wtype, schema, table, column, limit=limit)
    rows = client.execute(query)
    return extract_json_keys(rows)
