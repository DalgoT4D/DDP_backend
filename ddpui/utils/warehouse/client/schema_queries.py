from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


SCHEMA_QUERIES: dict[str, str] = {
    WarehouseType.POSTGRES.value: """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'airbyte_internal')
            AND schema_name NOT LIKE 'pg\\_%'
    """,
    WarehouseType.BIGQUERY.value: """
        SELECT schema_name
        FROM {schema_ref}.SCHEMATA
        WHERE schema_name != 'airbyte_internal'
    """,
}


def get_schema_query(wtype: str, bq_location: str | None = None) -> str:
    query = SCHEMA_QUERIES.get(wtype)
    if query is None:
        raise ValueError(f"Unsupported warehouse type: {wtype}")

    if wtype == WarehouseType.BIGQUERY.value:
        schema_ref = "INFORMATION_SCHEMA"
        if bq_location:
            normalized_location = bq_location.strip().lower()
            if normalized_location != "us" and not normalized_location.startswith("region-"):
                normalized_location = f"region-{normalized_location}"
            schema_ref = f"`{normalized_location}`.INFORMATION_SCHEMA"
        return query.format(schema_ref=schema_ref)

    return query
