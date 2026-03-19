"""Dashboard table allowlist derived from dashboard exports and dbt lineage."""

import json
from dataclasses import dataclass, field
from pathlib import Path

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.models.org import OrgDbt


def normalize_dashboard_chat_table_name(table_name: str | None) -> str | None:
    """Normalize schema-qualified table names for matching and policy checks."""
    if not table_name:
        return None
    normalized = table_name.strip().strip('"').strip("`").strip()
    if not normalized:
        return None
    normalized = normalized.replace('"."', ".").replace("`.", ".").replace(".`", ".")
    return normalized.lower()


def build_dashboard_chat_table_name(schema_name: str | None, table_name: str | None) -> str | None:
    """Build a normalized schema.table identifier from separate pieces."""
    if not schema_name or not table_name:
        return None
    return normalize_dashboard_chat_table_name(f"{schema_name}.{table_name}")


@dataclass
class DashboardChatAllowlist:
    """Allowed tables and dbt nodes for the current dashboard context."""

    chart_tables: set[str] = field(default_factory=set)
    upstream_tables: set[str] = field(default_factory=set)
    allowed_tables: set[str] = field(default_factory=set)
    allowed_unique_ids: set[str] = field(default_factory=set)
    unique_id_to_table: dict[str, str] = field(default_factory=dict)
    table_to_unique_ids: dict[str, set[str]] = field(default_factory=dict)

    def is_allowed(self, table_name: str | None) -> bool:
        """Return whether the table is inside the dashboard allowlist."""
        normalized = normalize_dashboard_chat_table_name(table_name)
        if not normalized or not self.allowed_tables:
            return False
        if normalized in self.allowed_tables:
            return True
        table_only = normalized.split(".")[-1]
        return any(
            allowed_table == table_only or allowed_table.endswith(f".{table_only}")
            for allowed_table in self.allowed_tables
        )

    def is_unique_id_allowed(self, unique_id: str | None) -> bool:
        """Return whether the dbt node belongs to the current dashboard lineage."""
        return bool(unique_id and unique_id in self.allowed_unique_ids)

    def prioritized_tables(self, limit: int | None = None) -> list[str]:
        """Return chart tables first, then lineage tables."""
        ordered_tables = sorted(self.chart_tables) + sorted(self.upstream_tables - self.chart_tables)
        deduped_tables = list(dict.fromkeys(ordered_tables))
        if limit is None:
            return deduped_tables
        return deduped_tables[:limit]


class DashboardChatAllowlistBuilder:
    """Build the allowlist for a dashboard using chart metadata and dbt lineage."""

    @classmethod
    def build(
        cls,
        export_payload: dict,
        manifest_json: dict | None = None,
    ) -> DashboardChatAllowlist:
        """Build the allowlist from the dashboard export contract and manifest lineage."""
        allowlist = DashboardChatAllowlist()

        for chart in export_payload.get("charts") or []:
            table_name = build_dashboard_chat_table_name(
                chart.get("schema_name"),
                chart.get("table_name"),
            )
            if not table_name:
                continue
            allowlist.chart_tables.add(table_name)
            allowlist.allowed_tables.add(table_name)

        if not manifest_json:
            return allowlist

        nodes_by_unique_id = cls._manifest_nodes_by_unique_id(manifest_json)
        table_to_unique_ids = cls._table_to_unique_ids(nodes_by_unique_id)
        allowlist.table_to_unique_ids = table_to_unique_ids
        allowlist.unique_id_to_table = {
            unique_id: table_name
            for table_name, unique_ids in table_to_unique_ids.items()
            for unique_id in unique_ids
        }

        for chart_table in list(allowlist.chart_tables):
            for unique_id in table_to_unique_ids.get(chart_table, set()):
                cls._add_unique_id_and_upstreams(
                    unique_id=unique_id,
                    allowlist=allowlist,
                    nodes_by_unique_id=nodes_by_unique_id,
                    visited=set(),
                )

        return allowlist

    @staticmethod
    def load_manifest_json(orgdbt: OrgDbt | None) -> dict | None:
        """Load the current manifest.json from the dbt target directory if it exists."""
        if orgdbt is None:
            return None

        target_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt)) / "target"
        manifest_path = target_dir / "manifest.json"
        if not manifest_path.exists():
            return None

        with open(manifest_path, "r", encoding="utf-8") as manifest_file:
            return json.load(manifest_file)

    @staticmethod
    def _manifest_nodes_by_unique_id(manifest_json: dict) -> dict[str, dict]:
        """Collect manifest nodes and sources that can participate in lineage traversal."""
        nodes_by_unique_id: dict[str, dict] = {}
        for unique_id, node in (manifest_json.get("nodes") or {}).items():
            if node.get("resource_type") not in {"model", "seed"}:
                continue
            nodes_by_unique_id[unique_id] = node
        for unique_id, source in (manifest_json.get("sources") or {}).items():
            nodes_by_unique_id[unique_id] = source
        return nodes_by_unique_id

    @staticmethod
    def _table_to_unique_ids(nodes_by_unique_id: dict[str, dict]) -> dict[str, set[str]]:
        """Build a schema.table lookup for manifest nodes and sources."""
        table_to_unique_ids: dict[str, set[str]] = {}
        for unique_id, node in nodes_by_unique_id.items():
            table_name = DashboardChatAllowlistBuilder._table_name_for_node(node)
            if not table_name:
                continue
            table_to_unique_ids.setdefault(table_name, set()).add(unique_id)
        return table_to_unique_ids

    @staticmethod
    def _table_name_for_node(node: dict) -> str | None:
        """Resolve a normalized schema.table identifier for a dbt node."""
        schema_name = node.get("schema")
        table_name = node.get("alias") or node.get("identifier") or node.get("name")
        if node.get("resource_type") == "source":
            table_name = node.get("identifier") or node.get("name")
        return build_dashboard_chat_table_name(schema_name, table_name)

    @classmethod
    def _add_unique_id_and_upstreams(
        cls,
        unique_id: str,
        allowlist: DashboardChatAllowlist,
        nodes_by_unique_id: dict[str, dict],
        visited: set[str],
    ) -> None:
        """Recursively add a dbt node and its upstream lineage into the allowlist."""
        if unique_id in visited:
            return
        visited.add(unique_id)

        node = nodes_by_unique_id.get(unique_id)
        if node is None:
            return

        allowlist.allowed_unique_ids.add(unique_id)
        table_name = cls._table_name_for_node(node)
        if table_name:
            allowlist.allowed_tables.add(table_name)
            if table_name not in allowlist.chart_tables:
                allowlist.upstream_tables.add(table_name)

        for dependency_unique_id in node.get("depends_on", {}).get("nodes") or []:
            cls._add_unique_id_and_upstreams(
                unique_id=dependency_unique_id,
                allowlist=allowlist,
                nodes_by_unique_id=nodes_by_unique_id,
                visited=visited,
            )
