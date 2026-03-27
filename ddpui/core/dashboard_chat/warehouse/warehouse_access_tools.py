"""Warehouse access helpers used by dashboard chat runtime."""

import json
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatSchemaSnippet
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse

logger = CustomLogger("dashboard_chat")

SAFE_WAREHOUSE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class DashboardChatWarehouseToolsError(Exception):
    """Raised when a warehouse-backed dashboard chat action cannot complete."""


class DashboardChatWarehouseTools:
    """Read-only warehouse helpers for dashboard chat nodes."""

    def __init__(
        self,
        org: Org,
        org_warehouse: OrgWarehouse | None = None,
        warehouse_client: Warehouse | None = None,
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
            try:
                parsed_table = self._parse_table_name(table_name)
            except DashboardChatWarehouseToolsError as error:
                logger.warning(
                    "dashboard chat schema lookup skipped invalid table %s: %s",
                    table_name,
                    error,
                )
                continue
            if parsed_table is None:
                continue
            schema_name, bare_table_name = parsed_table
            try:
                columns = self.warehouse_client.get_table_columns(schema_name, bare_table_name)
            except Exception as error:
                logger.warning(
                    "dashboard chat schema lookup failed for %s.%s: %s",
                    schema_name,
                    bare_table_name,
                    error,
                )
                continue
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
        return self.warehouse_client.get_distinct_values(
            schema_name, bare_table_name, column_name, limit
        )

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute a validated read-only SQL statement."""
        rows = self.warehouse_client.execute(sql)
        return list(rows[: self.max_rows])

    def _quote_bigquery_table_ref(self, schema_name: str, table_name: str) -> str:
        """Return a quoted BigQuery table ref using the configured project id."""
        project_id = self._resolve_bigquery_project_id()
        normalized_project_id = self._normalize_identifier_component(project_id, "project id")
        normalized_schema_name = self._normalize_identifier_component(schema_name, "schema name")
        normalized_table_name = self._normalize_identifier_component(table_name, "table name")
        return (
            f"`{normalized_project_id}.{normalized_schema_name}.{normalized_table_name}`"
        )

    def _resolve_bigquery_project_id(self) -> str:
        """Resolve the BigQuery project id from stored warehouse credentials."""
        if self.org_warehouse is None:
            raise DashboardChatWarehouseToolsError("Warehouse not configured for dashboard chat")
        credentials = secretsmanager.retrieve_warehouse_credentials(self.org_warehouse) or {}
        project_id = credentials.get("project_id")
        if not project_id:
            credentials_json = credentials.get("credentials_json")
            if credentials_json:
                try:
                    parsed_credentials = (
                        json.loads(credentials_json)
                        if isinstance(credentials_json, str)
                        else dict(credentials_json)
                    )
                except Exception as error:
                    raise DashboardChatWarehouseToolsError(
                        "Failed to parse BigQuery credentials JSON"
                    ) from error
                project_id = parsed_credentials.get("project_id")
        if not project_id:
            raise DashboardChatWarehouseToolsError("BigQuery project id not configured")
        return str(project_id)

    @staticmethod
    def _parse_table_name(table_name: str | None) -> tuple[str, str] | None:
        """Parse schema.table into separate pieces."""
        if not table_name or "." not in table_name:
            return None
        schema_name, bare_table_name = table_name.split(".", 1)
        return DashboardChatWarehouseTools._normalize_identifier_component(
            schema_name,
            "schema name",
        ), DashboardChatWarehouseTools._normalize_identifier_component(
            bare_table_name,
            "table name",
        )

    @staticmethod
    def _normalize_identifier_component(component: str, component_name: str) -> str:
        """Normalize a schema/table component and reject unsafe identifier text."""
        normalized_component = component.strip().strip('"').strip("`")
        if not normalized_component:
            raise DashboardChatWarehouseToolsError(f"{component_name} is required")
        if not SAFE_WAREHOUSE_IDENTIFIER_PATTERN.fullmatch(normalized_component):
            raise DashboardChatWarehouseToolsError(
                f"Invalid {component_name} for dashboard chat warehouse access"
            )
        return normalized_component
