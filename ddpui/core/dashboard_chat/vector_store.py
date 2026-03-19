"""Chroma-backed vector store wrapper for dashboard chat."""

from dataclasses import dataclass
import os
from typing import Any, Protocol

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector_documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
    build_dashboard_chat_collection_name,
)


class DashboardChatEmbeddingProvider(Protocol):
    """Embedding provider interface used by the vector store wrapper."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query."""


class OpenAIEmbeddingProvider:
    """OpenAI embeddings adapter for dashboard chat retrieval."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "text-embedding-3-small",
        client: Any = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        if client is None:
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY must be set for dashboard chat embeddings")
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key)
        self.client = client

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents using OpenAI."""
        if not texts:
            return []
        response = self.client.embeddings.create(model=self.model, input=texts)
        return [item.embedding for item in response.data]

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query using the document embedding path."""
        return self.embed_documents([text])[0]


@dataclass(frozen=True)
class DashboardChatVectorQueryResult:
    """Single query result returned from Chroma."""

    document_id: str
    content: str
    metadata: dict[str, Any]
    distance: float | None = None


@dataclass(frozen=True)
class DashboardChatStoredDocument:
    """Stored document metadata returned from Chroma collection reads."""

    document_id: str
    metadata: dict[str, Any]
    content: str | None = None


class ChromaDashboardChatVectorStore:
    """Thin wrapper around the Chroma HTTP client with Dalgo-specific conventions."""

    def __init__(
        self,
        config: DashboardChatVectorStoreConfig | None = None,
        embedding_provider: DashboardChatEmbeddingProvider | None = None,
        client: Any = None,
    ):
        self.config = config or DashboardChatVectorStoreConfig.from_env()
        self.embedding_provider = embedding_provider or OpenAIEmbeddingProvider(
            model=self.config.embedding_model
        )
        self.client = client or self._build_client()

    def _build_client(self) -> Any:
        """Build the real Chroma HTTP client lazily."""
        from chromadb import HttpClient

        return HttpClient(
            host=self.config.chroma_host,
            port=self.config.chroma_port,
            ssl=self.config.chroma_ssl,
        )

    def collection_name(self, org_id: int) -> str:
        """Return the Chroma collection name for an org."""
        return build_dashboard_chat_collection_name(org_id, self.config.collection_prefix)

    def create_collection(self, org_id: int) -> Any:
        """Create or load the Chroma collection for an org."""
        return self.client.get_or_create_collection(
            name=self.collection_name(org_id),
            metadata={"org_id": str(org_id)},
        )

    def load_collection(self, org_id: int) -> Any | None:
        """Load an existing Chroma collection for an org."""
        from chromadb.errors import InvalidCollectionException

        try:
            return self.client.get_collection(name=self.collection_name(org_id))
        except (InvalidCollectionException, ValueError):
            return None

    def delete_collection(self, org_id: int) -> bool:
        """Delete the Chroma collection for an org if it exists."""
        if self.load_collection(org_id) is None:
            return False
        self.client.delete_collection(name=self.collection_name(org_id))
        return True

    def get_documents(
        self,
        org_id: int,
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
        include_documents: bool = False,
    ) -> list[DashboardChatStoredDocument]:
        """Load stored documents for an org using metadata filters."""
        collection = self.load_collection(org_id)
        if collection is None:
            return []

        include = ["metadatas"]
        if include_documents:
            include.append("documents")

        result = collection.get(
            where=self._build_where_clause(source_types=source_types, dashboard_id=dashboard_id),
            include=include,
        )
        return self._parse_get_result(result, include_documents=include_documents)

    def delete_documents(
        self,
        org_id: int,
        ids: list[str] | None = None,
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
    ) -> int:
        """Delete matching documents from an org collection."""
        collection = self.load_collection(org_id)
        if collection is None:
            return 0

        where = self._build_where_clause(source_types=source_types, dashboard_id=dashboard_id)
        if ids is None and where is None:
            return 0

        deleted_count = (
            len(ids)
            if ids is not None
            else len(
                self.get_documents(
                    org_id,
                    source_types=source_types,
                    dashboard_id=dashboard_id,
                    include_documents=False,
                )
            )
        )
        collection.delete(ids=ids, where=where)
        return deleted_count

    def upsert_documents(
        self,
        org_id: int,
        documents: list[DashboardChatVectorDocument],
    ) -> list[str]:
        """Upsert documents into the org-specific Chroma collection."""
        if not documents:
            return []

        collection = self.create_collection(org_id)
        contents = [document.content for document in documents]
        document_ids = [document.document_id for document in documents]
        metadatas = [document.metadata() for document in documents]
        embeddings = self.embedding_provider.embed_documents(contents)

        collection.upsert(
            ids=document_ids,
            documents=contents,
            metadatas=metadatas,
            embeddings=embeddings,
        )
        return document_ids

    def query(
        self,
        org_id: int,
        query_text: str,
        n_results: int = 5,
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
    ) -> list[DashboardChatVectorQueryResult]:
        """Query the org-specific Chroma collection."""
        collection = self.load_collection(org_id)
        if collection is None:
            return []

        where = self._build_where_clause(source_types=source_types, dashboard_id=dashboard_id)
        result = collection.query(
            query_embeddings=[self.embedding_provider.embed_query(query_text)],
            n_results=n_results,
            where=where,
            include=["documents", "metadatas", "distances"],
        )
        return self._parse_query_result(result)

    @staticmethod
    def _build_where_clause(
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the metadata filter used for Chroma queries."""
        filters: list[dict[str, Any]] = []

        if source_types:
            normalized_types = [
                source_type.value
                if isinstance(source_type, DashboardChatSourceType)
                else source_type
                for source_type in source_types
            ]
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

    @staticmethod
    def _parse_query_result(result: dict[str, Any]) -> list[DashboardChatVectorQueryResult]:
        """Parse Chroma's nested result shape into flat typed rows."""
        ids = result.get("ids", [[]])
        documents = result.get("documents", [[]])
        metadatas = result.get("metadatas", [[]])
        distances = result.get("distances", [[]])

        parsed_results: list[DashboardChatVectorQueryResult] = []
        for document_id, content, metadata, distance in zip(
            ids[0] if ids else [],
            documents[0] if documents else [],
            metadatas[0] if metadatas else [],
            distances[0] if distances else [],
        ):
            parsed_results.append(
                DashboardChatVectorQueryResult(
                    document_id=document_id,
                    content=content,
                    metadata=metadata,
                    distance=distance,
                )
            )
        return parsed_results

    @staticmethod
    def _parse_get_result(
        result: dict[str, Any],
        include_documents: bool = False,
    ) -> list[DashboardChatStoredDocument]:
        """Parse Chroma's get result into typed stored-document rows."""
        ids = result.get("ids", [])
        metadatas = result.get("metadatas", [])
        documents = result.get("documents", []) if include_documents else []

        parsed_results: list[DashboardChatStoredDocument] = []
        for index, document_id in enumerate(ids):
            parsed_results.append(
                DashboardChatStoredDocument(
                    document_id=document_id,
                    metadata=metadatas[index] if index < len(metadatas) else {},
                    content=documents[index]
                    if include_documents and index < len(documents)
                    else None,
                )
            )
        return parsed_results
