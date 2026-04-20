from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


def fetch_table_data(
    client,
    wtype: str,
    schema: str,
    table: str,
    limit: int,
    page: int = 1,
    order_by: str | None = None,
    order: int = 1,
) -> list[dict]:
    """Fetch paginated rows from a table using centralized warehouse-specific SQL."""
    page_num = max(int(page or 1), 1)
    row_limit = max(int(limit or 10), 1)
    offset = (page_num - 1) * row_limit

    if wtype == WarehouseType.POSTGRES.value:
        schema_quoted = schema.replace('"', '""')
        table_quoted = table.replace('"', '""')
        query = f'SELECT * FROM "{schema_quoted}"."{table_quoted}"'
        params = {"offset": offset, "limit": row_limit}

        if order_by:
            order_by_quoted = order_by.replace('"', '""')
            sort_order = "ASC" if int(order) == 1 else "DESC"
            query += f' ORDER BY "{order_by_quoted}" {sort_order}'

        query += " OFFSET :offset LIMIT :limit"
        return client.execute(query, params)

    if wtype == WarehouseType.BIGQUERY.value:
        schema_quoted = schema.replace("`", "")
        table_quoted = table.replace("`", "")
        query = f"SELECT * FROM `{schema_quoted}.{table_quoted}`"

        if order_by:
            order_by_quoted = order_by.replace("`", "")
            sort_order = "ASC" if int(order) == 1 else "DESC"
            query += f" ORDER BY `{order_by_quoted}` {sort_order}"

        query += f" LIMIT {row_limit} OFFSET {offset}"
        return client.execute(query)

    raise ValueError(f"Unsupported warehouse type: {wtype}")
