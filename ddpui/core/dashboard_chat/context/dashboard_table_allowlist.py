"""Dashboard table allowlist derived from dashboard exports and dbt lineage."""

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatMetadataArtifactPayload
from ddpui.models.org import OrgDbt


class DashboardChatManifestDependencyError(RuntimeError):
    """Raised when the metadata build-time dbt manifest dependency is unavailable."""


def normalize_dashboard_chat_table_name(table_name: str | None) -> str | None:
    """Normalize schema-qualified table names for matching and policy checks."""
    if not table_name:
        return None
    normalized = table_name.strip().strip('"').strip("`").strip()
    if not normalized:
        return None
    normalized = normalized.replace('"."', ".").replace("`.", ".").replace(".`", ".")
    return normalized


def dashboard_chat_table_match_key(table_name: str | None) -> str | None:
    """Return the case-insensitive match key for one dashboard-chat table name."""
    normalized = normalize_dashboard_chat_table_name(table_name)
    if not normalized:
        return None
    return normalized.lower()


def find_matching_dashboard_chat_table_name(
    table_name: str | None,
    candidates: set[str] | list[str] | tuple[str, ...] | dict | Any,
) -> str | None:
    """Return the canonical candidate table name that matches one input table reference."""
    normalized = normalize_dashboard_chat_table_name(table_name)
    match_key = dashboard_chat_table_match_key(table_name)
    if not normalized or not match_key:
        return None

    candidate_names = list(candidates.keys()) if isinstance(candidates, dict) else list(candidates)
    for candidate_name in candidate_names:
        if normalize_dashboard_chat_table_name(str(candidate_name)) == normalized:
            return str(candidate_name)

    exact_matches = [
        str(candidate_name)
        for candidate_name in candidate_names
        if dashboard_chat_table_match_key(str(candidate_name)) == match_key
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]

    table_only = match_key.split(".")[-1]
    table_only_matches = [
        str(candidate_name)
        for candidate_name in candidate_names
        if dashboard_chat_table_match_key(str(candidate_name)) in {table_only}
        or str(dashboard_chat_table_match_key(str(candidate_name)) or "").endswith(
            f".{table_only}"
        )
    ]
    if len(table_only_matches) == 1:
        return table_only_matches[0]
    return None


def build_dashboard_chat_table_name(schema_name: str | None, table_name: str | None) -> str | None:
    """Build a normalized schema.table identifier from separate pieces."""
    if not schema_name or not table_name:
        return None
    return normalize_dashboard_chat_table_name(f"{schema_name}.{table_name}")


class DashboardChatAllowlist(BaseModel):
    """Allowed tables and dbt nodes for the current dashboard context."""

    chart_tables: set[str] = Field(default_factory=set)
    upstream_tables: set[str] = Field(default_factory=set)
    allowed_tables: set[str] = Field(default_factory=set)
    allowed_unique_ids: set[str] = Field(default_factory=set)
    unique_id_to_table: dict[str, str] = Field(default_factory=dict)
    table_to_unique_ids: dict[str, set[str]] = Field(default_factory=dict)

    def is_allowed(self, table_name: str | None) -> bool:
        """Return whether the table is inside the dashboard allowlist."""
        return self.resolve_allowed_table_name(table_name) is not None

    def resolve_allowed_table_name(self, table_name: str | None) -> str | None:
        """Resolve one table reference to the canonical allowlisted physical table name."""
        if not self.allowed_tables:
            return None
        return find_matching_dashboard_chat_table_name(table_name, self.allowed_tables)

    def is_unique_id_allowed(self, unique_id: str | None) -> bool:
        """Return whether the dbt node belongs to the current dashboard lineage."""
        return bool(unique_id and unique_id in self.allowed_unique_ids)

    def prioritized_tables(self, limit: int | None = None) -> list[str]:
        """Return chart tables first, then lineage tables."""
        ordered_tables = sorted(self.chart_tables) + sorted(
            self.upstream_tables - self.chart_tables
        )
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
        source_tables = cls._manifest_source_tables(manifest_json) if manifest_json else set()

        for chart in export_payload.get("charts") or []:
            table_name = build_dashboard_chat_table_name(
                chart.get("schema_name"),
                chart.get("table_name"),
            )
            if not table_name:
                continue
            if cls._should_exclude_physical_table(table_name):
                continue
            if table_name in source_tables:
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

    @classmethod
    def build_from_metadata_artifact(
        cls,
        payload: DashboardChatMetadataArtifactPayload,
    ) -> DashboardChatAllowlist:
        """Rebuild the runtime allowlist directly from the persisted metadata artifact."""
        allowlist = DashboardChatAllowlist()
        allowlist.allowed_tables = set(payload.allowlisted_tables or [])
        allowlist.chart_tables = set(payload.chart_table_map.keys())
        allowlist.upstream_tables = allowlist.allowed_tables - allowlist.chart_tables
        return allowlist

    @staticmethod
    def load_manifest_json(orgdbt: OrgDbt | None) -> dict | None:
        """Load the current manifest.json build dependency from the dbt target directory."""
        if orgdbt is None:
            return None

        target_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt)) / "target"
        manifest_path = target_dir / "manifest.json"
        if not manifest_path.exists():
            return None

        with open(manifest_path, "r", encoding="utf-8") as manifest_file:
            return json.load(manifest_file)

    @classmethod
    def load_required_manifest_json(cls, orgdbt: OrgDbt | None) -> dict:
        """Load manifest.json or raise a clear build-time dependency error."""
        manifest_json = cls.load_manifest_json(orgdbt)
        if manifest_json:
            return manifest_json
        raise DashboardChatManifestDependencyError(
            "Dashboard chat metadata build requires dbt target/manifest.json to be present. "
            "Run or compile the dbt project for this org before rebuilding dashboard chat metadata."
        )

    @staticmethod
    def _manifest_nodes_by_unique_id(manifest_json: dict) -> dict[str, dict]:
        """Collect manifest nodes and sources that can participate in lineage traversal."""
        nodes_by_unique_id: dict[str, dict] = {}
        for unique_id, node in (manifest_json.get("nodes") or {}).items():
            if node.get("resource_type") not in {"model", "seed"}:
                continue
            if DashboardChatAllowlistBuilder._should_exclude_lineage_node(node):
                continue
            nodes_by_unique_id[unique_id] = node
        return nodes_by_unique_id

    @classmethod
    def _manifest_source_tables(cls, manifest_json: dict | None) -> set[str]:
        """Return physical source tables from the manifest that should never be queryable."""
        if not manifest_json:
            return set()

        source_tables: set[str] = set()
        for source in (manifest_json.get("sources") or {}).values():
            table_name = cls._table_name_for_node(source)
            if table_name:
                source_tables.add(table_name)
        return source_tables

    @staticmethod
    def _should_exclude_lineage_node(node: dict) -> bool:
        """Return whether one manifest node should be excluded from the chat allowlist."""
        resource_type = str(node.get("resource_type") or "")
        if resource_type == "source":
            return True
        schema_name = str(node.get("schema") or "")
        if DashboardChatAllowlistBuilder._should_exclude_physical_table(
            build_dashboard_chat_table_name(
                schema_name,
                node.get("alias") or node.get("identifier") or node.get("name"),
            )
        ):
            return True
        node_path = " ".join(
            str(value or "")
            for value in [
                node.get("path"),
                node.get("original_file_path"),
                " ".join(node.get("fqn") or []),
            ]
        ).lower()
        if "/source/" in node_path or " source " in node_path:
            return True
        return False

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
        resolved = build_dashboard_chat_table_name(schema_name, table_name)
        if DashboardChatAllowlistBuilder._should_exclude_physical_table(resolved):
            return None
        return resolved

    @staticmethod
    def _should_exclude_physical_table(table_name: str | None) -> bool:
        """Return whether one physical table belongs to a disallowed source/raw layer."""
        normalized = normalize_dashboard_chat_table_name(table_name)
        if not normalized:
            return True
        schema_name = normalized.split(".", 1)[0].lower()
        return (
            schema_name.endswith("_source")
            or ".source" in normalized.lower()
            or "_source." in normalized.lower()
        )

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

    @classmethod
    def build_dbt_index(
        cls,
        manifest_json: dict | None,
        allowlist: DashboardChatAllowlist,
    ) -> dict[str, Any]:
        """Build a compact allowlisted dbt index for deterministic model-search tools."""
        if not manifest_json:
            return {"resources_by_unique_id": {}}

        nodes_by_unique_id = cls._manifest_nodes_by_unique_id(manifest_json)
        parent_map = manifest_json.get("parent_map") or {}
        child_map = manifest_json.get("child_map") or {}
        resources_by_unique_id: dict[str, dict[str, Any]] = {}

        for unique_id in sorted(allowlist.allowed_unique_ids):
            node = nodes_by_unique_id.get(unique_id)
            if node is None:
                continue

            table_name = cls._table_name_for_node(node)
            resources_by_unique_id[unique_id] = {
                "unique_id": unique_id,
                "resource_type": str(node.get("resource_type") or ""),
                "name": str(node.get("name") or ""),
                "schema": str(node.get("schema") or ""),
                "database": str(node.get("database") or ""),
                "description": str(node.get("description") or ""),
                "table": table_name,
                "columns": [
                    {
                        "name": str(column.get("name") or column_name),
                        "type": str(column.get("data_type") or column.get("type") or ""),
                        "description": str(column.get("description") or ""),
                    }
                    for column_name, column in (node.get("columns") or {}).items()
                ],
                "upstream": cls._lineage_entries(
                    unique_ids=parent_map.get(unique_id) or [],
                    nodes_by_unique_id=nodes_by_unique_id,
                    allowlist=allowlist,
                ),
                "downstream": cls._lineage_entries(
                    unique_ids=child_map.get(unique_id) or [],
                    nodes_by_unique_id=nodes_by_unique_id,
                    allowlist=allowlist,
                ),
            }

        return {"resources_by_unique_id": resources_by_unique_id}

    @classmethod
    def _lineage_entries(
        cls,
        *,
        unique_ids: list[str],
        nodes_by_unique_id: dict[str, dict],
        allowlist: DashboardChatAllowlist,
    ) -> list[str]:
        """Return compact allowlisted lineage labels for one dbt resource."""
        lineage_entries: list[str] = []
        for unique_id in unique_ids:
            if not allowlist.is_unique_id_allowed(unique_id):
                continue
            node = nodes_by_unique_id.get(unique_id)
            if node is None:
                lineage_entries.append(unique_id)
                continue
            table_name = cls._table_name_for_node(node)
            lineage_entries.append(table_name or str(node.get("name") or unique_id))
        return lineage_entries
