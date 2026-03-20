"""Warehouse access helpers used by dashboard chat runtime."""

import json
from typing import Any

from ddpui.core.dashboard_chat.runtime_types import DashboardChatSchemaSnippet
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory


class DashboardChatWarehouseToolsError(Exception):
    """Raised when a warehouse-backed dashboard chat action cannot complete."""


class DashboardChatWarehouseTools:
    """Read-only warehouse helpers for dashboard chat nodes."""

    def __init__(
        self,
        org: Org,
        org_warehouse: OrgWarehouse | None = None,
        warehouse_client: Any = None,
        max_rows: int = 200,
    ):
        self.org = org
        self.org_warehouse = org_warehouse or OrgWarehouse.objects.filter(org=org).first()
        if self.org_warehouse is None:
            raise DashboardChatWarehouseToolsError("Warehouse not configured for dashboard chat")

        self.max_rows = max_rows
        self.warehouse_client = warehouse_client or WarehouseFactory.get_warehouse_client(
            self.org_warehouse
        )

    def get_schema_snippets(self, tables: list[str]) -> dict[str, DashboardChatSchemaSnippet]:
        """Load table column metadata for prompt grounding."""
        snippets: dict[str, DashboardChatSchemaSnippet] = {}

        for table_name in list(dict.fromkeys(tables)):
            parsed_table = self._parse_table_name(table_name)
            if parsed_table is None:
                continue
            schema_name, bare_table_name = parsed_table
            columns = self.warehouse_client.get_table_columns(schema_name, bare_table_name)
            if not columns:
                continue
            snippets[table_name.lower()] = DashboardChatSchemaSnippet(
                table_name=table_name.lower(),
                columns=list(columns),
            )

        return snippets

    def get_distinct_values(
        self,
        table_name: str,
        column_name: str,
        limit: int = 50,
    ) -> list[str]:
        """Return distinct, non-empty values for a text filter column."""
        parsed_table = self._parse_table_name(table_name)
        if parsed_table is None:
            raise DashboardChatWarehouseToolsError(
                f"Table '{table_name}' must be schema-qualified for distinct lookups"
            )
        schema_name, bare_table_name = parsed_table

        if not self.warehouse_client.column_exists(schema_name, bare_table_name, column_name):
            return []

        query = self._build_distinct_values_query(
            schema_name=schema_name,
            table_name=bare_table_name,
            column_name=column_name,
            limit=limit,
        )
        rows = self.warehouse_client.execute(query)
        return [
            str(row.get("value"))
            for row in rows
            if row.get("value") is not None and str(row.get("value")).strip()
        ]

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute a validated read-only SQL statement."""
        rows = self.warehouse_client.execute(sql)
        return list(rows[: self.max_rows])

    def _build_distinct_values_query(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        limit: int,
    ) -> str:
        """Build a warehouse-specific query for distinct values."""
        if self.org_warehouse.wtype == "postgres":
            quoted_column = self._quote_postgres_identifier(column_name)
            return f"""
                SELECT DISTINCT {quoted_column} AS value
                FROM {self._quote_table_ref(schema_name, table_name)}
                WHERE {quoted_column} IS NOT NULL
                  AND TRIM(CAST({quoted_column} AS TEXT)) != ''
                ORDER BY value
                LIMIT {int(limit)}
            """

        if self.org_warehouse.wtype == "bigquery":
            quoted_column = self._quote_bigquery_identifier(column_name)
            return f"""
                SELECT DISTINCT {quoted_column} AS value
                FROM {self._quote_bigquery_table_ref(schema_name, table_name)}
                WHERE {quoted_column} IS NOT NULL
                  AND TRIM(CAST({quoted_column} AS STRING)) != ''
                ORDER BY value
                LIMIT {int(limit)}
            """

        raise DashboardChatWarehouseToolsError(
            f"Unsupported warehouse type for dashboard chat: {self.org_warehouse.wtype}"
        )

    def _quote_table_ref(self, schema_name: str, table_name: str) -> str:
        """Quote a Postgres schema.table reference."""
        return (
            f"{self._quote_postgres_identifier(schema_name)}."
            f"{self._quote_postgres_identifier(table_name)}"
        )

    def _quote_bigquery_table_ref(self, schema_name: str, table_name: str) -> str:
        """Quote a BigQuery fully-qualified table reference."""
        project_name = self._get_bigquery_project_id()
        if not project_name:
            raise DashboardChatWarehouseToolsError("BigQuery project id not configured")
        return f"`{project_name}.{schema_name}.{table_name}`"

    def _get_bigquery_project_id(self) -> str | None:
        """Read the BigQuery project id from stored warehouse credentials."""
        credentials = secretsmanager.retrieve_warehouse_credentials(self.org_warehouse) or {}
        project_id = credentials.get("project_id")
        if project_id:
            return str(project_id)

        credentials_json = credentials.get("credentials_json")
        if isinstance(credentials_json, str):
            try:
                parsed_credentials = json.loads(credentials_json)
            except json.JSONDecodeError:
                return None
            project_id = parsed_credentials.get("project_id")
            if project_id:
                return str(project_id)

        return None

    @staticmethod
    def _quote_postgres_identifier(identifier: str) -> str:
        """Quote a Postgres identifier while preserving its literal value."""
        escaped_identifier = identifier.replace('"', '""')
        return f'"{escaped_identifier}"'

    @staticmethod
    def _quote_bigquery_identifier(identifier: str) -> str:
        """Quote a BigQuery identifier while preserving its literal value."""
        escaped_identifier = identifier.replace("`", "")
        return f"`{escaped_identifier}`"

    @staticmethod
    def _parse_table_name(table_name: str | None) -> tuple[str, str] | None:
        """Parse schema.table into separate pieces."""
        if not table_name or "." not in table_name:
            return None
        schema_name, bare_table_name = table_name.split(".", 1)
        return schema_name.strip().strip('"').strip("`"), bare_table_name.strip().strip('"').strip(
            "`"
        )
