"""Chroma-backed vector store wrapper for dashboard chat."""

from dataclasses import dataclass
import os
from typing import Any, Protocol

from ddpui.core.dashboard_chat.config import DashboardChatVectorStoreConfig
from ddpui.core.dashboard_chat.vector_documents import (
    DashboardChatSourceType,
    DashboardChatVectorDocument,
    build_dashboard_chat_collection_base_name,
    build_dashboard_chat_collection_name,
)


class DashboardChatEmbeddingProvider(Protocol):
    """Embedding provider interface used by the vector store wrapper."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query."""

    def reset_usage(self) -> None:
        """Reset per-turn embedding usage before a new runtime invocation."""


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
        self.usage_events: list[dict[str, Any]] = []
        if client is None:
            if not self.api_key:
                raise ValueError("OPENAI_API_KEY must be set for dashboard chat embeddings")
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key, max_retries=2)
        self.client = client

    def reset_usage(self) -> None:
        """Reset aggregated embedding usage before one new chat turn."""
        self.usage_events = []

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents using OpenAI."""
        if not texts:
            return []
        response = self.client.embeddings.create(model=self.model, input=texts)
        self._record_usage("embed_documents", response, len(texts))
        return [item.embedding for item in response.data]

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query using the document embedding path."""
        return self.embed_documents([text])[0]

    def usage_summary(self) -> dict[str, Any]:
        """Return aggregated embedding usage for the current turn."""
        totals = {
            "prompt_tokens": 0,
            "total_tokens": 0,
        }
        for event in self.usage_events:
            totals["prompt_tokens"] += event.get("prompt_tokens", 0)
            totals["total_tokens"] += event.get("total_tokens", 0)
        return {
            "model": self.model,
            "calls": list(self.usage_events),
            "totals": totals,
        }

    def _record_usage(self, operation: str, response: Any, input_count: int) -> None:
        """Capture embedding usage from one OpenAI embeddings response."""
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        self.usage_events.append(
            {
                "operation": operation,
                "model": self.model,
                "input_count": input_count,
                "prompt_tokens": getattr(usage, "prompt_tokens", 0) or 0,
                "total_tokens": getattr(usage, "total_tokens", 0) or 0,
            }
        )


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
        return self.client.get_or_create_collection(
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
        from chromadb.errors import InvalidCollectionException

        try:
            resolved_collection_name = collection_name or self.collection_name(org_id)
            return self.client.get_collection(name=resolved_collection_name)
        except (InvalidCollectionException, ValueError):
            if collection_name is None or not allow_legacy_fallback:
                return None
            try:
                return self.client.get_collection(
                    name=build_dashboard_chat_collection_base_name(
                        org_id,
                        self.config.collection_prefix,
                    )
                )
            except (InvalidCollectionException, ValueError):
                return None

    def delete_collection(
        self,
        org_id: int,
        *,
        collection_name: str | None = None,
    ) -> bool:
        """Delete the Chroma collection for an org if it exists."""
        resolved_collection_name = collection_name or self.collection_name(org_id)
        if self.load_collection(
            org_id,
            collection_name=resolved_collection_name,
            allow_legacy_fallback=False,
        ) is None:
            return False
        self.client.delete_collection(name=resolved_collection_name)
        return True

    def list_collection_names(self) -> list[str]:
        """Return all Chroma collection names for the current client."""
        raw_collections = self.client.list_collections()
        collection_names: list[str] = []
        for collection in raw_collections:
            if isinstance(collection, str):
                collection_names.append(collection)
                continue
            name = getattr(collection, "name", None)
            if name:
                collection_names.append(str(name))
        return collection_names

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
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
        include_documents: bool = False,
        collection_name: str | None = None,
    ) -> list[DashboardChatStoredDocument]:
        """Load stored documents for an org using metadata filters."""
        collection = self.load_collection(org_id, collection_name=collection_name)
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
        collection_name: str | None = None,
    ) -> int:
        """Delete matching documents from an org collection."""
        collection = self.load_collection(
            org_id,
            collection_name=collection_name,
            allow_legacy_fallback=False,
        )
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
                    collection_name=collection_name,
                )
            )
        )
        collection.delete(ids=ids, where=where)
        return deleted_count

    def upsert_documents(
        self,
        org_id: int,
        documents: list[DashboardChatVectorDocument],
        collection_name: str | None = None,
    ) -> list[str]:
        """Upsert documents into the org-specific Chroma collection."""
        if not documents:
            return []

        collection = self.create_collection(org_id, collection_name=collection_name)
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
        source_types: list[DashboardChatSourceType | str] | None = None,
        dashboard_id: int | None = None,
        query_embedding: list[float] | None = None,
        collection_name: str | None = None,
    ) -> list[DashboardChatVectorQueryResult]:
        """Query the org-specific Chroma collection."""
        collection = self.load_collection(org_id, collection_name=collection_name)
        if collection is None:
            return []

        where = self._build_where_clause(source_types=source_types, dashboard_id=dashboard_id)
        result = collection.query(
            query_embeddings=[query_embedding or self.embed_query(query_text)],
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
