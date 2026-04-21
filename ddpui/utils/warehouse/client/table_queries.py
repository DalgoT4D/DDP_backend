from typing import Any
import re

from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


TABLE_QUERIES: dict[str, str] = {
    WarehouseType.POSTGRES.value: """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        AND table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE')
    """,
    WarehouseType.BIGQUERY.value: """
        SELECT table_name
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW', 'EXTERNAL')
    """,
}

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


def get_table_query(wtype: str) -> str:
    query = TABLE_QUERIES.get(wtype)
    if query is None:
        raise ValueError(f"Unsupported warehouse type: {wtype}")
    return query


def _run_table_query(client, wtype: str, schema: str):
    query = get_table_query(wtype)
    if wtype == WarehouseType.POSTGRES.value:
        return client.execute(query, {"schema": schema})
    if wtype == WarehouseType.BIGQUERY.value:
        schema_project, dataset = _split_bq_schema(schema)
        project = schema_project or _get_bigquery_project(client)
        return client.execute(query.format(project=project, dataset=dataset))

    raise ValueError(f"Unsupported warehouse type: {wtype}")


def _extract_table_name(row: Any) -> str | None:
    if isinstance(row, dict):
        return row.get("table_name")

    if isinstance(row, (list, tuple)):
        return row[0] if row else None

    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping.get("table_name")

    return None


def list_table_names(client, wtype: str, schema: str) -> list[str]:
    rows = _run_table_query(client, wtype, schema)
    return [table_name for table_name in (_extract_table_name(row) for row in rows) if table_name]
