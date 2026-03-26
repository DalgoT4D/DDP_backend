"""Org-scoped vector store for dashboard chat retrieval."""

from collections.abc import Sequence
from typing import Any, Union

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector.documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
    build_dashboard_chat_collection_base_name,
    build_dashboard_chat_collection_name,
)
from ddpui.core.dashboard_chat.vector.embeddings import (
    DashboardChatEmbeddingProvider,
    OpenAIEmbeddingProvider,
)
from ddpui.utils.vector.interface import VectorStore, VectorQueryResult, VectorStoredDocument


def _default_backend(config: DashboardChatVectorStoreConfig) -> VectorStore:
    """Build the default vector store backend from config."""
    from ddpui.utils.vector.backends.chroma import ChromaVectorStore

    return ChromaVectorStore(
        host=config.vector_store_host,
        port=config.vector_store_port,
        ssl=config.vector_store_ssl,
    )


class OrgVectorStore:
    """Org-scoped vector retrieval layer for dashboard chat."""

    def __init__(
        self,
        config: Union[DashboardChatVectorStoreConfig, None] = None,
        embedding_provider: Union[DashboardChatEmbeddingProvider, None] = None,
        backend: Union[VectorStore, None] = None,
    ):
        self.config = config or DashboardChatVectorStoreConfig.from_env()
        self.embedding_provider = embedding_provider or OpenAIEmbeddingProvider(
            model=self.config.embedding_model
        )
        self.backend = backend or _default_backend(self.config)

    def collection_name(self, org_id: int, *, version: Any = None) -> str:
        """Return the collection name for an org, optionally versioned."""
        return build_dashboard_chat_collection_name(
            org_id,
            self.config.collection_prefix,
            version=version,
        )

    def create_collection(
        self,
        org_id: int,
        *,
        collection_name: Union[str, None] = None,
    ) -> Any:
        """Create or load the collection for an org."""
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.create_collection(resolved, metadata={"org_id": str(org_id)})

    def load_collection(
        self,
        org_id: int,
        *,
        collection_name: Union[str, None] = None,
        allow_legacy_fallback: bool = True,
    ) -> Union[Any, None]:
        """Load an existing collection for an org."""
        resolved = collection_name or self.collection_name(org_id)
        collection = self.backend.load_collection(resolved)
        if collection is not None or collection_name is None or not allow_legacy_fallback:
            return collection
        return self.backend.load_collection(
            build_dashboard_chat_collection_base_name(org_id, self.config.collection_prefix)
        )

    def delete_collection(
        self,
        org_id: int,
        *,
        collection_name: Union[str, None] = None,
    ) -> bool:
        """Delete the collection for an org if it exists."""
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.delete_collection(resolved)

    def list_collection_names(self) -> list[str]:
        """Return all collection names in the backend."""
        return self.backend.list_collection_names()

    def list_org_collection_names(self, org_id: int) -> list[str]:
        """Return all collection names that belong to one org."""
        base_name = build_dashboard_chat_collection_base_name(org_id, self.config.collection_prefix)
        return [
            name
            for name in self.list_collection_names()
            if name == base_name or name.startswith(f"{base_name}__")
        ]

    def get_documents(
        self,
        org_id: int,
        source_types: Union[Sequence[DashboardChatSourceType], None] = None,
        dashboard_id: Union[int, None] = None,
        include_documents: bool = False,
        collection_name: Union[str, None] = None,
    ) -> list[VectorStoredDocument]:
        """Load stored documents for an org using metadata filters."""
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.get_documents(
            resolved,
            where=self._build_filter(source_types=source_types, dashboard_id=dashboard_id),
            include_documents=include_documents,
        )

    def delete_documents(
        self,
        org_id: int,
        ids: Union[list[str], None] = None,
        source_types: Union[Sequence[DashboardChatSourceType], None] = None,
        dashboard_id: Union[int, None] = None,
        collection_name: Union[str, None] = None,
    ) -> int:
        """Delete matching documents from an org collection."""
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.delete_documents(
            resolved,
            ids=ids,
            where=self._build_filter(source_types=source_types, dashboard_id=dashboard_id),
        )

    def upsert_documents(
        self,
        org_id: int,
        documents: list[DashboardChatVectorDocument],
        collection_name: Union[str, None] = None,
    ) -> list[str]:
        """Upsert documents into the org-specific collection."""
        if not documents:
            return []
        contents = [doc.content for doc in documents]
        embeddings = self.embedding_provider.embed_documents(contents)
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.upsert(
            resolved,
            ids=[doc.document_id for doc in documents],
            documents=contents,
            metadatas=[doc.metadata() for doc in documents],
            embeddings=embeddings,
            collection_metadata={"org_id": str(org_id)},
        )

    def embed_query(self, query_text: str) -> list[float]:
        """Embed one query string."""
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
        source_types: Union[Sequence[DashboardChatSourceType], None] = None,
        dashboard_id: Union[int, None] = None,
        query_embedding: Union[list[float], None] = None,
        collection_name: Union[str, None] = None,
    ) -> list[VectorQueryResult]:
        """Query the org-specific collection."""
        resolved = collection_name or self.collection_name(org_id)
        return self.backend.query(
            resolved,
            query_embedding=query_embedding or self.embed_query(query_text),
            n_results=n_results,
            where=self._build_filter(source_types=source_types, dashboard_id=dashboard_id),
        )

    @staticmethod
    def _build_filter(
        source_types: Union[Sequence[DashboardChatSourceType], None] = None,
        dashboard_id: Union[int, None] = None,
    ) -> Union[dict[str, Any], None]:
        """Build the metadata filter for collection queries."""
        filters: list[dict[str, Any]] = []
        if source_types:
            normalized = [st.value for st in source_types]
            if len(normalized) == 1:
                filters.append({"source_type": normalized[0]})
            else:
                filters.append({"source_type": {"$in": normalized}})
        if dashboard_id is not None:
            filters.append({"dashboard_id": dashboard_id})
        if not filters:
            return None
        if len(filters) == 1:
            return filters[0]
        return {"$and": filters}
