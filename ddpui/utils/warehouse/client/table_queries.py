from typing import Any

from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


TABLE_QUERIES: dict[str, str] = {
    WarehouseType.POSTGRES.value: """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        AND table_type = 'BASE TABLE'
    """,
    WarehouseType.BIGQUERY.value: """
        SELECT table_name
        FROM `{schema}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
    """,
}


def get_table_query(wtype: str) -> str:
    query = TABLE_QUERIES.get(wtype)
    if not query:
        raise ValueError(f"Unsupported warehouse type: {wtype}")
    return query


def _run_table_query(client, wtype: str, schema: str):
    query = get_table_query(wtype)
    if wtype == WarehouseType.POSTGRES.value:
        return client.execute(query, {"schema": schema})
    if wtype == WarehouseType.BIGQUERY.value:
        return client.execute(query.format(schema=schema))

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def _extract_table_name(row: Any) -> str | None:
    if isinstance(row, dict):
        return row.get("table_name")

    if isinstance(row, (list, tuple)):
        return row[0] if row else None

    try:
        return row["table_name"]
    except Exception:
        return None


def list_table_names(client, wtype: str, schema: str) -> list[str]:
    rows = _run_table_query(client, wtype, schema)
    return [table_name for table_name in (_extract_table_name(row) for row in rows) if table_name]
