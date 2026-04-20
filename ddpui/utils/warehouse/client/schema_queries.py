from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


SCHEMA_QUERIES: dict[str, str] = {
    WarehouseType.POSTGRES.value: """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'airbyte_internal')
    """,
    WarehouseType.BIGQUERY.value: """
        SELECT schema_name
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE schema_name != 'airbyte_internal'
    """,
}


def get_schema_query(wtype: str) -> str:
    query = SCHEMA_QUERIES.get(wtype)
    if not query:
        raise ValueError(f"Unsupported warehouse type: {wtype}")
    return query
