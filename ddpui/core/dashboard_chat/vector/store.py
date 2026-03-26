"""Dashboard-chat vector retrieval built on top of the shared Chroma transport."""

from collections.abc import Sequence
from typing import Any

from chromadb import ClientAPI

from ddpui.core.dashboard_chat.vector.embeddings import (
    DashboardChatEmbeddingProvider,
    OpenAIEmbeddingProvider,
)
from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector.documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
    build_dashboard_chat_collection_base_name,
    build_dashboard_chat_collection_name,
)
from ddpui.utils.vector.chroma import (
    ChromaHttpVectorStore,
    ChromaQueryResult,
    ChromaStoredDocument,
)

DashboardChatVectorQueryResult = ChromaQueryResult
DashboardChatStoredDocument = ChromaStoredDocument


class ChromaDashboardChatVectorStore:
    """Dashboard-chat-specific adapter on top of the generic Chroma wrapper."""

    def __init__(
        self,
        config: DashboardChatVectorStoreConfig | None = None,
        embedding_provider: DashboardChatEmbeddingProvider | None = None,
        client: ClientAPI | None = None,
        chroma_store: ChromaHttpVectorStore | None = None,
    ):
        self.config = config or DashboardChatVectorStoreConfig.from_env()
        self.embedding_provider = embedding_provider or OpenAIEmbeddingProvider(
            model=self.config.embedding_model
        )
        self.chroma_store = chroma_store or ChromaHttpVectorStore(
            host=self.config.chroma_host,
            port=self.config.chroma_port,
            ssl=self.config.chroma_ssl,
            client=client,
        )

    def collection_name(
        self,
        org_id: int,
        *,
        version: Any = None,
    ) -> str:
        """Return the Chroma collection name for an org."""
        return build_dashboard_chat_collection_name(
            org_id,
            self.config.collection_prefix,
            version=version,
        )

    def create_collection(
        self,
        org_id: int,
        *,
        collection_name: str | None = None,
    ) -> Any:
        """Create or load the Chroma collection for an org."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.create_collection(
            name=resolved_collection_name,
            metadata={"org_id": str(org_id)},
        )

    def load_collection(
        self,
        org_id: int,
        *,
        collection_name: str | None = None,
        allow_legacy_fallback: bool = True,
    ) -> Any | None:
        """Load an existing Chroma collection for an org."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        collection = self.chroma_store.load_collection(resolved_collection_name)
        if collection is not None or collection_name is None or not allow_legacy_fallback:
            return collection
        return self.chroma_store.load_collection(
            build_dashboard_chat_collection_base_name(
                org_id,
                self.config.collection_prefix,
            )
        )

    def delete_collection(
        self,
        org_id: int,
        *,
        collection_name: str | None = None,
    ) -> bool:
        """Delete the Chroma collection for an org if it exists."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.delete_collection(resolved_collection_name)

    def list_collection_names(self) -> list[str]:
        """Return all Chroma collection names for the current client."""
        return self.chroma_store.list_collection_names()

    def list_org_collection_names(self, org_id: int) -> list[str]:
        """Return all collection names that belong to one org."""
        base_name = build_dashboard_chat_collection_base_name(org_id, self.config.collection_prefix)
        return [
            collection_name
            for collection_name in self.list_collection_names()
            if collection_name == base_name or collection_name.startswith(f"{base_name}__")
        ]

    def get_documents(
        self,
        org_id: int,
        source_types: Sequence[DashboardChatSourceType] | None = None,
        dashboard_id: int | None = None,
        include_documents: bool = False,
        collection_name: str | None = None,
    ) -> list[DashboardChatStoredDocument]:
        """Load stored documents for an org using metadata filters."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.get_documents(
            resolved_collection_name,
            where=self._build_vector_metadata_filter(
                source_types=source_types,
                dashboard_id=dashboard_id,
            ),
            include_documents=include_documents,
        )

    def delete_documents(
        self,
        org_id: int,
        ids: list[str] | None = None,
        source_types: Sequence[DashboardChatSourceType] | None = None,
        dashboard_id: int | None = None,
        collection_name: str | None = None,
    ) -> int:
        """Delete matching documents from an org collection."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.delete_documents(
            resolved_collection_name,
            ids=ids,
            where=self._build_vector_metadata_filter(
                source_types=source_types,
                dashboard_id=dashboard_id,
            ),
        )

    def upsert_documents(
        self,
        org_id: int,
        documents: list[DashboardChatVectorDocument],
        collection_name: str | None = None,
    ) -> list[str]:
        """Upsert documents into the org-specific Chroma collection."""
        if not documents:
            return []

        contents = [document.content for document in documents]
        document_ids = [document.document_id for document in documents]
        metadatas = [document.metadata() for document in documents]
        embeddings = self.embedding_provider.embed_documents(contents)
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.upsert_documents(
            resolved_collection_name,
            ids=document_ids,
            documents=contents,
            metadatas=metadatas,
            embeddings=embeddings,
            collection_metadata={"org_id": str(org_id)},
        )

    def embed_query(self, query_text: str) -> list[float]:
        """Build one query embedding that can be reused across filtered retrieval calls."""
        return self.embedding_provider.embed_query(query_text)

    def reset_usage(self) -> None:
        """Reset embedding usage counters before one new runtime invocation."""
        if hasattr(self.embedding_provider, "reset_usage"):
            self.embedding_provider.reset_usage()

    def usage_summary(self) -> dict[str, Any]:
        """Return embedding usage from the configured provider when supported."""
        if hasattr(self.embedding_provider, "usage_summary"):
            return self.embedding_provider.usage_summary()
        return {}

    def query(
        self,
        org_id: int,
        query_text: str,
        n_results: int = 5,
        source_types: Sequence[DashboardChatSourceType] | None = None,
        dashboard_id: int | None = None,
        query_embedding: list[float] | None = None,
        collection_name: str | None = None,
    ) -> list[DashboardChatVectorQueryResult]:
        """Query the org-specific Chroma collection."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        return self.chroma_store.query(
            resolved_collection_name,
            query_embedding=query_embedding or self.embed_query(query_text),
            n_results=n_results,
            where=self._build_vector_metadata_filter(
                source_types=source_types,
                dashboard_id=dashboard_id,
            ),
        )

    @staticmethod
    def _build_vector_metadata_filter(
        source_types: Sequence[DashboardChatSourceType] | None = None,
        dashboard_id: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the metadata filter used for Chroma queries."""
        filters: list[dict[str, Any]] = []

        if source_types:
            normalized_types = [source_type.value for source_type in source_types]
            if len(normalized_types) == 1:
                filters.append({"source_type": normalized_types[0]})
            else:
                filters.append({"source_type": {"$in": normalized_types}})

        if dashboard_id is not None:
            filters.append({"dashboard_id": dashboard_id})

        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"$and": filters}
