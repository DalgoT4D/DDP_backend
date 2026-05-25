"""Warehouse access helpers used by dashboard chat runtime."""

import json
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatSchemaSnippet
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse

logger = CustomLogger("dashboard_chat")

SAFE_WAREHOUSE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
POSTGRES_TABLE_REF_PATTERN = re.compile(
    r"(?P<prefix>\b(?:FROM|JOIN)\b\s+)"
    r"(?P<schema>(?:\"[^\"]+\"|`[^`]+`|[A-Za-z0-9_-]+))"
    r"\."
    r"(?P<table>(?:\"[^\"]+\"|`[^`]+`|[A-Za-z0-9_-]+))",
    flags=re.IGNORECASE,
)
SINGLE_QUOTED_SQL_SEGMENT_PATTERN = re.compile(r"('(?:''|[^'])*')")


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
            snippets[table_name] = DashboardChatSchemaSnippet(
                table_name=table_name,
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
        rows = self.warehouse_client.execute(self._prepare_sql_for_execution(sql))
        return list(rows[: self.max_rows])

    def _prepare_sql_for_execution(self, sql: str) -> str:
        """Normalize generated SQL for the target warehouse before execution."""
        if getattr(self.org_warehouse, "wtype", None) != "postgres":
            return sql
        return self._prepare_postgres_sql(sql)

    def _prepare_postgres_sql(self, sql: str) -> str:
        """Quote physical Postgres identifiers that require case preservation."""
        normalized_sql = sql
        mixed_case_columns_by_table: dict[tuple[str, str], list[str]] = {}

        def replace_table_ref(match: re.Match[str]) -> str:
            schema_name = self._normalize_identifier_component(match.group("schema"), "schema name")
            table_name = self._normalize_identifier_component(match.group("table"), "table name")
            mixed_case_columns_by_table[(schema_name, table_name)] = (
                self._get_postgres_mixed_case_columns(schema_name, table_name)
            )
            return (
                f'{match.group("prefix")}'
                f'{self._quote_postgres_identifier(schema_name)}.'
                f'{self._quote_postgres_identifier(table_name)}'
            )

        normalized_sql = POSTGRES_TABLE_REF_PATTERN.sub(replace_table_ref, normalized_sql)

        mixed_case_columns = sorted(
            {
                column_name
                for column_names in mixed_case_columns_by_table.values()
                for column_name in column_names
            },
            key=len,
            reverse=True,
        )
        if not mixed_case_columns:
            return normalized_sql
        return self._quote_postgres_columns_outside_strings(normalized_sql, mixed_case_columns)

    def _quote_bigquery_table_ref(self, schema_name: str, table_name: str) -> str:
        """Return a quoted BigQuery table ref using the configured project id."""
        project_id = self._resolve_bigquery_project_id()
        normalized_project_id = self._normalize_identifier_component(project_id, "project id")
        normalized_schema_name = self._normalize_identifier_component(schema_name, "schema name")
        normalized_table_name = self._normalize_identifier_component(table_name, "table name")
        return (
            f"`{normalized_project_id}.{normalized_schema_name}.{normalized_table_name}`"
        )

    def _get_postgres_mixed_case_columns(self, schema_name: str, table_name: str) -> list[str]:
        """Return physical Postgres column names that require quoting."""
        try:
            columns = self.warehouse_client.get_table_columns(schema_name, table_name)
        except Exception as error:
            logger.warning(
                "dashboard chat postgres column lookup failed for %s.%s: %s",
                schema_name,
                table_name,
                error,
            )
            return []

        return [
            str(column.get("name"))
            for column in columns
            if column.get("name")
            and str(column.get("name")) != str(column.get("name")).lower()
        ]

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

    @staticmethod
    def _quote_postgres_identifier(identifier: str) -> str:
        """Quote a Postgres identifier while preserving physical case."""
        return '"' + identifier.replace('"', '""') + '"'

    def _quote_postgres_columns_outside_strings(
        self,
        sql: str,
        column_names: list[str],
    ) -> str:
        """Quote mixed-case Postgres column identifiers outside string literals."""
        segments = SINGLE_QUOTED_SQL_SEGMENT_PATTERN.split(sql)
        quoted_sql_segments: list[str] = []

        for index, segment in enumerate(segments):
            if index % 2 == 1:
                quoted_sql_segments.append(segment)
                continue

            updated_segment = segment
            for column_name in column_names:
                pattern = re.compile(
                    rf'(?<!["`])\b{re.escape(column_name)}\b(?!["`])'
                )
                updated_segment = pattern.sub(
                    self._quote_postgres_identifier(column_name),
                    updated_segment,
                )
            quoted_sql_segments.append(updated_segment)

        return "".join(quoted_sql_segments)
