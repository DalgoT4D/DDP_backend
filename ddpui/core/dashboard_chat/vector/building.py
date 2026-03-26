"""Vector build pipeline for dashboard chat retrieval."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from django.utils import timezone

from ddpui.core.dashboard_chat.context.dbt_docs import (
    DashboardChatDbtDocsArtifacts,
    generate_dashboard_chat_dbt_docs_artifacts,
)
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.vector.builder import DashboardChatVectorDocumentBuilder
from ddpui.core.dashboard_chat.vector.documents import DashboardChatSourceType
from ddpui.core.dashboard_chat.vector.store import ChromaDashboardChatVectorStore
from ddpui.models.dashboard_chat import DashboardChatSession
from ddpui.models.org import Org

INGEST_SOURCE_ORDER = [
    DashboardChatSourceType.ORG_CONTEXT,
    DashboardChatSourceType.DASHBOARD_CONTEXT,
    DashboardChatSourceType.DASHBOARD_EXPORT,
    DashboardChatSourceType.DBT_MANIFEST,
    DashboardChatSourceType.DBT_CATALOG,
]


class DashboardChatVectorBuildError(Exception):
    """Raised when the dashboard chat vector build cannot complete."""


@dataclass(frozen=True)
class DashboardChatVectorBuildResult:
    """Summary of one completed org vector build."""

    org_id: int
    docs_generated_at: timezone.datetime | None
    vector_ingested_at: timezone.datetime
    source_document_counts: dict[str, int]
    upserted_document_ids: list[str]
    deleted_document_ids: list[str]


class DashboardChatVectorBuildService:
    """Build org-scoped dashboard-chat vector context and sync it into Chroma."""

    def __init__(
        self,
        vector_store: ChromaDashboardChatVectorStore | None = None,
        dbt_docs_generator: Callable[[Org, object], DashboardChatDbtDocsArtifacts] | None = None,
        source_config: DashboardChatSourceConfig | None = None,
        document_builder: DashboardChatVectorDocumentBuilder | None = None,
    ):
        self.vector_store = vector_store or ChromaDashboardChatVectorStore()
        self.dbt_docs_generator = dbt_docs_generator or generate_dashboard_chat_dbt_docs_artifacts
        self.source_config = source_config or DashboardChatSourceConfig.from_env()
        self.document_builder = document_builder or DashboardChatVectorDocumentBuilder(
            source_config=self.source_config
        )

    def build_org_vector_context(self, org: Org) -> DashboardChatVectorBuildResult:
        """Run dbt docs generation and rebuild the desired vector documents for an org."""
        if org.dbt is None:
            raise DashboardChatVectorBuildError("dbt workspace not configured")

        collection_versioned_at = timezone.now()
        target_collection_name = self.vector_store.collection_name(
            org.id,
            version=collection_versioned_at,
        )
        dbt_docs = None
        if self.source_config.is_enabled(
            DashboardChatSourceType.DBT_MANIFEST
        ) or self.source_config.is_enabled(DashboardChatSourceType.DBT_CATALOG):
            dbt_docs = self.dbt_docs_generator(org, org.dbt)
        documents_by_source = self.document_builder.build_documents_by_source(org, dbt_docs)
        desired_documents = [
            document
            for source_type in INGEST_SOURCE_ORDER
            if self.source_config.is_enabled(source_type)
            for document in documents_by_source[source_type.value]
        ]
        if self.vector_store.load_collection(
            org.id,
            collection_name=target_collection_name,
            allow_legacy_fallback=False,
        ) is not None:
            self.vector_store.delete_collection(
                org.id,
                collection_name=target_collection_name,
            )

        upserted_document_ids = sorted(
            self.vector_store.upsert_documents(
                org.id,
                desired_documents,
                collection_name=target_collection_name,
            )
        )

        vector_ingested_at = collection_versioned_at
        org.dbt.vector_last_ingested_at = collection_versioned_at
        org.dbt.save(update_fields=["vector_last_ingested_at", "updated_at"])
        self._garbage_collect_inactive_collections(
            org=org,
            active_collection_name=target_collection_name,
        )

        return DashboardChatVectorBuildResult(
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
            deleted_document_ids=[],
        )

    def _garbage_collect_inactive_collections(
        self,
        *,
        org: Org,
        active_collection_name: str,
    ) -> None:
        """Delete old versioned collections that are not pinned by recent chat sessions."""
        retention_cutoff = timezone.now() - timedelta(hours=24)
        recent_sessions = DashboardChatSession.objects.filter(
            org=org,
            updated_at__gte=retention_cutoff,
        )
        pinned_collection_names = {
            collection_name
            for collection_name in recent_sessions.values_list("vector_collection_name", flat=True)
            if collection_name
        }
        if recent_sessions.filter(vector_collection_name__isnull=True).exists():
            pinned_collection_names.add(self.vector_store.collection_name(org.id))
        pinned_collection_names.add(active_collection_name)

        for collection_name in self.vector_store.list_org_collection_names(org.id):
            if collection_name in pinned_collection_names:
                continue
            self.vector_store.delete_collection(
                org.id,
                collection_name=collection_name,
            )
