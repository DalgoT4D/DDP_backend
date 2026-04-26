import re

from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


_BQ_DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BQ_PROJECT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]*$")


def _validate_bq_dataset(dataset: str) -> str:
    if not _BQ_DATASET_RE.fullmatch(dataset):
        raise ValueError(f"Invalid bigquery dataset name: {dataset}")
    return dataset


def _validate_bq_project(project: str) -> str:
    if not _BQ_PROJECT_RE.fullmatch(project):
        raise ValueError(f"Invalid bigquery project name: {project}")
    return project


def _split_bq_schema(schema: str) -> tuple[str | None, str]:
    if "." in schema:
        project, dataset = schema.split(".", 1)
        return _validate_bq_project(project), _validate_bq_dataset(dataset)
    return None, _validate_bq_dataset(schema)


def _get_bigquery_project(client) -> str:
    project = getattr(getattr(getattr(client, "engine", None), "url", None), "database", None)
    if not project:
        raise ValueError("Unable to determine BigQuery project from warehouse client")
    return _validate_bq_project(project)


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
        schema_project, dataset = _split_bq_schema(schema)
        project = schema_project or _get_bigquery_project(client)
        table_quoted = _validate_bq_dataset(table)
        query = f"SELECT * FROM `{project}.{dataset}.{table_quoted}`"

        if order_by:
            order_by_quoted = _validate_bq_dataset(order_by)
            sort_order = "ASC" if int(order) == 1 else "DESC"
            query += f" ORDER BY `{order_by_quoted}` {sort_order}"

        query += f" LIMIT {row_limit} OFFSET {offset}"
        return client.execute(query)

    raise ValueError(f"Unsupported warehouse type: {wtype}")
