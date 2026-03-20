"""Context-build pipeline for dashboard chat retrieval."""

from collections import defaultdict
from dataclasses import dataclass
import json
from typing import Callable

from django.utils import timezone

from ddpui.core.dashboard_chat.dbt_docs import (
    DashboardChatDbtDocsArtifacts,
    generate_dashboard_chat_dbt_docs_artifacts,
)
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.vector_documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
)
from ddpui.core.dashboard_chat.vector_store import ChromaDashboardChatVectorStore
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardAIContext, OrgAIContext
from ddpui.models.org import Org
from ddpui.models.visualization import Chart
from ddpui.services.dashboard_service import DashboardService

MARKDOWN_CHUNK_MAX_CHARS = 1200
DBT_IGNORE_PACKAGES = {"dbt", "dbt_bigquery", "elementary"}
INGEST_SOURCE_ORDER = [
    DashboardChatSourceType.ORG_CONTEXT,
    DashboardChatSourceType.DASHBOARD_CONTEXT,
    DashboardChatSourceType.DASHBOARD_EXPORT,
    DashboardChatSourceType.DBT_MANIFEST,
    DashboardChatSourceType.DBT_CATALOG,
]


class DashboardChatIngestionError(Exception):
    """Raised when the dashboard chat context build cannot complete."""


@dataclass(frozen=True)
class DashboardChatIngestionResult:
    """Summary of one completed org context build."""

    org_id: int
    docs_generated_at: timezone.datetime | None
    vector_ingested_at: timezone.datetime
    source_document_counts: dict[str, int]
    upserted_document_ids: list[str]
    deleted_document_ids: list[str]


def _normalize_text(value: str) -> str:
    """Normalize text before chunking so document IDs stay deterministic."""
    return "\n".join(
        line.rstrip() for line in (value or "").replace("\r\n", "\n").split("\n")
    ).strip()


def chunk_dashboard_chat_text(text: str, max_chars: int = MARKDOWN_CHUNK_MAX_CHARS) -> list[str]:
    """Chunk text by paragraph blocks and hard-wrap only when needed."""
    normalized = _normalize_text(text)
    if not normalized:
        return []

    blocks = [block.strip() for block in normalized.split("\n\n") if block.strip()]
    if not blocks:
        return []

    chunks: list[str] = []
    current_blocks: list[str] = []
    current_length = 0

    for block in blocks:
        block_length = len(block)
        if block_length > max_chars:
            if current_blocks:
                chunks.append("\n\n".join(current_blocks))
                current_blocks = []
                current_length = 0
            for start_index in range(0, block_length, max_chars):
                chunks.append(block[start_index : start_index + max_chars].strip())
            continue

        separator_length = 2 if current_blocks else 0
        if current_length + separator_length + block_length > max_chars:
            chunks.append("\n\n".join(current_blocks))
            current_blocks = [block]
            current_length = block_length
            continue

        current_blocks.append(block)
        current_length += separator_length + block_length

    if current_blocks:
        chunks.append("\n\n".join(current_blocks))
    return chunks


class DashboardChatIngestionService:
    """Build and ingest org-scoped retrieval documents for dashboard chat."""

    def __init__(
        self,
        vector_store: ChromaDashboardChatVectorStore | None = None,
        dbt_docs_generator: Callable[[Org, object], DashboardChatDbtDocsArtifacts] | None = None,
        source_config: DashboardChatSourceConfig | None = None,
    ):
        self.vector_store = vector_store or ChromaDashboardChatVectorStore()
        self.dbt_docs_generator = dbt_docs_generator or generate_dashboard_chat_dbt_docs_artifacts
        self.source_config = source_config or DashboardChatSourceConfig.from_env()

    def ingest_org(self, org: Org) -> DashboardChatIngestionResult:
        """Run dbt docs generation and rebuild the desired vector documents for an org."""
        if org.dbt is None:
            raise DashboardChatIngestionError("dbt workspace not configured")

        dbt_docs = None
        if self.source_config.is_enabled(
            DashboardChatSourceType.DBT_MANIFEST
        ) or self.source_config.is_enabled(DashboardChatSourceType.DBT_CATALOG):
            dbt_docs = self.dbt_docs_generator(org, org.dbt)
        documents_by_source = self._build_documents(org, dbt_docs)
        desired_documents = [
            document
            for source_type in INGEST_SOURCE_ORDER
            if self.source_config.is_enabled(source_type)
            for document in documents_by_source[source_type.value]
        ]

        existing_documents = self.vector_store.get_documents(org.id)
        existing_document_ids = {document.document_id for document in existing_documents}
        desired_document_ids = {document.document_id for document in desired_documents}

        new_documents = [
            document
            for document in desired_documents
            if document.document_id not in existing_document_ids
        ]
        upserted_document_ids: list[str] = []
        if new_documents:
            upserted_document_ids = sorted(
                self.vector_store.upsert_documents(org.id, new_documents)
            )

        stale_document_ids = sorted(existing_document_ids - desired_document_ids)
        if stale_document_ids:
            self.vector_store.delete_documents(org.id, ids=stale_document_ids)

        vector_ingested_at = timezone.now()
        org.dbt.vector_last_ingested_at = vector_ingested_at
        org.dbt.save(update_fields=["vector_last_ingested_at", "updated_at"])

        return DashboardChatIngestionResult(
            org_id=org.id,
            docs_generated_at=dbt_docs.generated_at if dbt_docs else org.dbt.docs_generated_at,
            vector_ingested_at=vector_ingested_at,
            source_document_counts={
                source_type.value: (
                    len(documents_by_source[source_type.value])
                    if self.source_config.is_enabled(source_type)
                    else 0
                )
                for source_type in INGEST_SOURCE_ORDER
            },
            upserted_document_ids=upserted_document_ids,
            deleted_document_ids=stale_document_ids,
        )

    def _build_documents(
        self,
        org: Org,
        dbt_docs: DashboardChatDbtDocsArtifacts | None,
    ) -> dict[str, list[DashboardChatVectorDocument]]:
        """Build the full desired vector document set for an org."""
        documents_by_source: dict[str, list[DashboardChatVectorDocument]] = defaultdict(list)

        org_context = OrgAIContext.objects.filter(org=org).first()
        if org_context and org_context.markdown:
            documents_by_source[DashboardChatSourceType.ORG_CONTEXT.value].extend(
                self._build_markdown_documents(
                    org_id=org.id,
                    source_type=DashboardChatSourceType.ORG_CONTEXT,
                    source_identifier=f"org:{org.id}:context",
                    markdown=org_context.markdown,
                    title=f"{org.name} organization context",
                    updated_at=org_context.updated_at,
                )
            )

        dashboard_contexts = {
            context.dashboard_id: context
            for context in DashboardAIContext.objects.filter(
                dashboard__org=org,
            ).select_related("dashboard")
        }
        dashboards = list(
            Dashboard.objects.filter(org=org).prefetch_related("filters").order_by("id")
        )
        chart_ids = {
            chart_id
            for dashboard in dashboards
            for chart_id in DashboardService.extract_chart_ids_from_components(
                dashboard.components,
            )
        }
        charts_by_id = {
            chart.id: chart for chart in Chart.objects.filter(org=org, id__in=chart_ids)
        }

        for dashboard in dashboards:
            dashboard_context = dashboard_contexts.get(dashboard.id)
            if dashboard_context and dashboard_context.markdown:
                documents_by_source[DashboardChatSourceType.DASHBOARD_CONTEXT.value].extend(
                    self._build_markdown_documents(
                        org_id=org.id,
                        source_type=DashboardChatSourceType.DASHBOARD_CONTEXT,
                        source_identifier=f"dashboard:{dashboard.id}:context",
                        markdown=dashboard_context.markdown,
                        dashboard_id=dashboard.id,
                        title=f"{dashboard.title} dashboard context",
                        updated_at=dashboard_context.updated_at,
                    )
                )

            export_payload = DashboardService.export_dashboard_context_for_dashboard(
                dashboard,
                org,
                charts_by_id=charts_by_id,
            )
            documents_by_source[DashboardChatSourceType.DASHBOARD_EXPORT.value].extend(
                self._build_dashboard_export_documents(org.id, dashboard.id, export_payload)
            )

        if dbt_docs is not None and self.source_config.is_enabled(
            DashboardChatSourceType.DBT_MANIFEST
        ):
            documents_by_source[DashboardChatSourceType.DBT_MANIFEST.value].extend(
                self._build_manifest_documents(org.id, dbt_docs)
            )
        if dbt_docs is not None and self.source_config.is_enabled(
            DashboardChatSourceType.DBT_CATALOG
        ):
            documents_by_source[DashboardChatSourceType.DBT_CATALOG.value].extend(
                self._build_catalog_documents(org.id, dbt_docs)
            )

        return {
            source_type.value: documents_by_source.get(source_type.value, [])
            for source_type in INGEST_SOURCE_ORDER
        }

    def _build_markdown_documents(
        self,
        org_id: int,
        source_type: DashboardChatSourceType,
        source_identifier: str,
        markdown: str,
        title: str,
        dashboard_id: int | None = None,
        updated_at: timezone.datetime | None = None,
    ) -> list[DashboardChatVectorDocument]:
        """Chunk a markdown source into deterministic vector documents."""
        return [
            DashboardChatVectorDocument(
                org_id=org_id,
                source_type=source_type,
                source_identifier=source_identifier,
                content=chunk,
                dashboard_id=dashboard_id,
                title=title,
                chunk_index=chunk_index,
                updated_at=updated_at,
            )
            for chunk_index, chunk in enumerate(chunk_dashboard_chat_text(markdown))
        ]

    def _build_dashboard_export_documents(
        self,
        org_id: int,
        dashboard_id: int,
        export_payload: dict,
    ) -> list[DashboardChatVectorDocument]:
        """Build dashboard summary and chart documents from the export contract."""
        documents: list[DashboardChatVectorDocument] = []
        dashboard_payload = export_payload["dashboard"]
        dashboard_title = dashboard_payload.get("title") or f"Dashboard {dashboard_id}"

        summary_lines = [
            f"Dashboard title: {dashboard_title}",
            f"Dashboard id: {dashboard_payload.get('id')}",
            f"Dashboard type: {dashboard_payload.get('dashboard_type')}",
        ]
        if dashboard_payload.get("description"):
            summary_lines.append(f"Description: {dashboard_payload['description']}")

        filters = dashboard_payload.get("filters") or []
        if filters:
            summary_lines.append("Filters:")
            for dashboard_filter in filters:
                summary_lines.append(
                    "- {name} ({filter_type}) from {schema}.{table}.{column}".format(
                        name=dashboard_filter.get("name") or dashboard_filter.get("column_name"),
                        filter_type=dashboard_filter.get("filter_type"),
                        schema=dashboard_filter.get("schema_name"),
                        table=dashboard_filter.get("table_name"),
                        column=dashboard_filter.get("column_name"),
                    )
                )

        charts = export_payload.get("charts") or []
        if charts:
            summary_lines.append("Charts:")
            for chart in charts:
                summary_lines.append(
                    "- {title} [{chart_type}] from {schema}.{table}".format(
                        title=chart.get("title"),
                        chart_type=chart.get("chart_type"),
                        schema=chart.get("schema_name"),
                        table=chart.get("table_name"),
                    )
                )

        documents.extend(
            self._build_markdown_documents(
                org_id=org_id,
                source_type=DashboardChatSourceType.DASHBOARD_EXPORT,
                source_identifier=f"dashboard:{dashboard_id}:summary",
                markdown="\n\n".join(summary_lines),
                dashboard_id=dashboard_id,
                title=dashboard_title,
            )
        )

        for chart in charts:
            chart_title = chart.get("title") or f"Chart {chart.get('id')}"
            chart_lines = [
                f"Dashboard id: {dashboard_id}",
                f"Chart id: {chart.get('id')}",
                f"Chart title: {chart_title}",
                f"Chart type: {chart.get('chart_type')}",
                "Data source: {schema}.{table}".format(
                    schema=chart.get("schema_name"),
                    table=chart.get("table_name"),
                ),
            ]
            if chart.get("description"):
                chart_lines.append(f"Description: {chart['description']}")
            if chart.get("extra_config"):
                chart_lines.append(
                    "Extra config: "
                    + json.dumps(chart["extra_config"], sort_keys=True, separators=(",", ":"))
                )

            documents.extend(
                self._build_markdown_documents(
                    org_id=org_id,
                    source_type=DashboardChatSourceType.DASHBOARD_EXPORT,
                    source_identifier=f"dashboard:{dashboard_id}:chart:{chart['id']}",
                    markdown="\n\n".join(chart_lines),
                    dashboard_id=dashboard_id,
                    title=chart_title,
                )
            )

        return documents

    def _build_manifest_documents(
        self,
        org_id: int,
        dbt_docs: DashboardChatDbtDocsArtifacts,
    ) -> list[DashboardChatVectorDocument]:
        """Build vector documents from manifest.json models and sources."""
        manifest_json = dbt_docs.manifest_json
        project_name = manifest_json.get("metadata", {}).get("project_name")
        documents: list[DashboardChatVectorDocument] = []

        for unique_id, source in sorted((manifest_json.get("sources") or {}).items()):
            if not self._include_dbt_unique_id(unique_id, project_name):
                continue
            source_name = source.get("name") or unique_id
            documents.extend(
                self._build_markdown_documents(
                    org_id=org_id,
                    source_type=DashboardChatSourceType.DBT_MANIFEST,
                    source_identifier=f"manifest:{unique_id}",
                    markdown=self._format_manifest_source(unique_id, source),
                    title=source_name,
                    updated_at=dbt_docs.generated_at,
                )
            )

        for unique_id, node in sorted((manifest_json.get("nodes") or {}).items()):
            if node.get("resource_type") != "model":
                continue
            if not self._include_dbt_unique_id(unique_id, project_name):
                continue
            model_name = node.get("name") or unique_id
            documents.extend(
                self._build_markdown_documents(
                    org_id=org_id,
                    source_type=DashboardChatSourceType.DBT_MANIFEST,
                    source_identifier=f"manifest:{unique_id}",
                    markdown=self._format_manifest_model(unique_id, node),
                    title=model_name,
                    updated_at=dbt_docs.generated_at,
                )
            )

        return documents

    def _build_catalog_documents(
        self,
        org_id: int,
        dbt_docs: DashboardChatDbtDocsArtifacts,
    ) -> list[DashboardChatVectorDocument]:
        """Build vector documents from catalog.json models and sources."""
        catalog_json = dbt_docs.catalog_json
        project_name = dbt_docs.manifest_json.get("metadata", {}).get("project_name")
        documents: list[DashboardChatVectorDocument] = []

        for unique_id, source in sorted((catalog_json.get("sources") or {}).items()):
            if not self._include_dbt_unique_id(unique_id, project_name):
                continue
            source_name = ((source.get("metadata") or {}).get("name")) or unique_id
            documents.extend(
                self._build_markdown_documents(
                    org_id=org_id,
                    source_type=DashboardChatSourceType.DBT_CATALOG,
                    source_identifier=f"catalog:{unique_id}",
                    markdown=self._format_catalog_entry(unique_id, source, entry_type="source"),
                    title=source_name,
                    updated_at=dbt_docs.generated_at,
                )
            )

        for unique_id, node in sorted((catalog_json.get("nodes") or {}).items()):
            if not self._include_dbt_unique_id(unique_id, project_name):
                continue
            model_name = ((node.get("metadata") or {}).get("name")) or unique_id
            documents.extend(
                self._build_markdown_documents(
                    org_id=org_id,
                    source_type=DashboardChatSourceType.DBT_CATALOG,
                    source_identifier=f"catalog:{unique_id}",
                    markdown=self._format_catalog_entry(unique_id, node, entry_type="model"),
                    title=model_name,
                    updated_at=dbt_docs.generated_at,
                )
            )

        return documents

    @staticmethod
    def _include_dbt_unique_id(unique_id: str, project_name: str | None) -> bool:
        """Exclude package docs that do not belong to the org project."""
        parts = unique_id.split(".")
        if len(parts) < 2:
            return True
        package_name = parts[1]
        if package_name in DBT_IGNORE_PACKAGES:
            return False
        if project_name and package_name != project_name:
            return False
        return True

    @staticmethod
    def _format_manifest_source(unique_id: str, source: dict) -> str:
        """Format a manifest source entry into stable text."""
        column_lines = DashboardChatIngestionService._format_columns(source.get("columns") or {})
        blocks = [
            f"dbt manifest source: {source.get('schema')}.{source.get('name')}",
            f"Unique id: {unique_id}",
            f"Source name: {source.get('source_name')}",
            f"Database: {source.get('database')}",
        ]
        if column_lines:
            blocks.append("Columns:\n" + "\n".join(column_lines))
        return "\n\n".join(block for block in blocks if block and block != "Database: None")

    @staticmethod
    def _format_manifest_model(unique_id: str, node: dict) -> str:
        """Format a manifest model entry into stable text."""
        blocks = [
            f"dbt manifest model: {node.get('schema')}.{node.get('name')}",
            f"Unique id: {unique_id}",
            f"Path: {node.get('original_file_path') or node.get('path')}",
            f"Description: {node.get('description')}",
            f"Database: {node.get('database')}",
        ]
        depends_on_nodes = sorted(node.get("depends_on", {}).get("nodes") or [])
        if depends_on_nodes:
            blocks.append(
                "Depends on:\n" + "\n".join(f"- {dependency}" for dependency in depends_on_nodes)
            )
        column_lines = DashboardChatIngestionService._format_columns(node.get("columns") or {})
        if column_lines:
            blocks.append("Columns:\n" + "\n".join(column_lines))
        return "\n\n".join(
            block
            for block in blocks
            if block and block not in {"Database: None", "Description: None"}
        )

    @staticmethod
    def _format_catalog_entry(unique_id: str, entry: dict, entry_type: str) -> str:
        """Format a catalog source/model entry into stable text."""
        metadata = entry.get("metadata") or {}
        blocks = [
            f"dbt catalog {entry_type}: {metadata.get('schema')}.{metadata.get('name')}",
            f"Unique id: {unique_id}",
            f"Database: {metadata.get('database')}",
            f"Type: {metadata.get('type')}",
        ]
        column_lines = DashboardChatIngestionService._format_catalog_columns(
            entry.get("columns") or {}
        )
        if column_lines:
            blocks.append("Columns:\n" + "\n".join(column_lines))
        return "\n\n".join(block for block in blocks if block and block != "Database: None")

    @staticmethod
    def _format_columns(columns: dict) -> list[str]:
        """Format manifest column metadata into stable bullet lines."""
        formatted_columns: list[str] = []
        for column_key, column in sorted(columns.items()):
            column_name = column.get("name") or column_key
            line = f"- {column_name}"
            if column.get("data_type"):
                line += f" ({column['data_type']})"
            if column.get("description"):
                line += f": {column['description']}"
            formatted_columns.append(line)
        return formatted_columns

    @staticmethod
    def _format_catalog_columns(columns: dict) -> list[str]:
        """Format catalog column metadata into stable bullet lines."""
        formatted_columns: list[str] = []
        for column_name, column in sorted(columns.items()):
            line = f"- {column_name}"
            if column.get("type"):
                line += f" ({column['type']})"
            if column.get("comment"):
                line += f": {column['comment']}"
            formatted_columns.append(line)
        return formatted_columns
