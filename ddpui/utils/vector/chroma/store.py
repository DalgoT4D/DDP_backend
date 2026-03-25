"""Generic Chroma wrapper that only knows how to talk to the Chroma server."""

from typing import Any

from chromadb import ClientAPI
from chromadb.errors import InvalidCollectionException

from ddpui.utils.vector.chroma.client import get_shared_chroma_http_client
from ddpui.utils.vector.chroma.types import ChromaQueryResult, ChromaStoredDocument


class ChromaHttpVectorStore:
    """Thin generic wrapper around the Chroma HTTP client."""

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 8000,
        ssl: bool = False,
        client: ClientAPI | None = None,
    ):
        self.client = client or get_shared_chroma_http_client(host, port, ssl)

    def create_collection(
        self,
        name: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Create or load a Chroma collection."""
        return self.client.get_or_create_collection(name=name, metadata=metadata)

    def load_collection(self, name: str) -> Any | None:
        """Load one existing Chroma collection by name."""
        try:
            return self.client.get_collection(name=name)
        except (InvalidCollectionException, ValueError):
            return None

    def delete_collection(self, name: str) -> bool:
        """Delete one Chroma collection if it exists."""
        if self.load_collection(name) is None:
            return False
        self.client.delete_collection(name=name)
        return True

    def list_collection_names(self) -> list[str]:
        """Return all collection names available to this Chroma client."""
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

    def get_documents(
        self,
        collection_name: str,
        *,
        where: dict[str, Any] | None = None,
        include_documents: bool = False,
    ) -> list[ChromaStoredDocument]:
        """Read documents from one collection using an optional metadata filter."""
        collection = self.load_collection(collection_name)
        if collection is None:
            return []

        include = ["metadatas"]
        if include_documents:
            include.append("documents")
        result = collection.get(where=where, include=include)
        return self._parse_chroma_get_response(
            result,
            include_documents=include_documents,
        )

    def delete_documents(
        self,
        collection_name: str,
        *,
        ids: list[str] | None = None,
        where: dict[str, Any] | None = None,
    ) -> int:
        """Delete documents from one collection by ids and/or metadata filter."""
        collection = self.load_collection(collection_name)
        if collection is None:
            return 0

        if ids is None and where is None:
            return 0

        deleted_count = (
            len(ids)
            if ids is not None
            else len(
                self.get_documents(
                    collection_name,
                    where=where,
                    include_documents=False,
                )
            )
        )
        collection.delete(ids=ids, where=where)
        return deleted_count

    def upsert_documents(
        self,
        collection_name: str,
        *,
        ids: list[str],
        documents: list[str],
        metadatas: list[dict[str, Any]],
        embeddings: list[list[float]],
        collection_metadata: dict[str, Any] | None = None,
    ) -> list[str]:
        """Upsert documents into one collection."""
        if not ids:
            return []

        collection = self.create_collection(
            collection_name,
            metadata=collection_metadata,
        )
        collection.upsert(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
            embeddings=embeddings,
        )
        return ids

    def query(
        self,
        collection_name: str,
        *,
        query_embedding: list[float],
        n_results: int = 5,
        where: dict[str, Any] | None = None,
    ) -> list[ChromaQueryResult]:
        """Query one collection using a precomputed embedding and optional metadata filter."""
        collection = self.load_collection(collection_name)
        if collection is None:
            return []

        result = collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=where,
            include=["documents", "metadatas", "distances"],
        )
        return self._parse_chroma_query_response(result)

    @staticmethod
    def _parse_chroma_query_response(
        result: dict[str, Any],
    ) -> list[ChromaQueryResult]:
        """Parse Chroma's nested query result shape into flat typed rows."""
        ids = result.get("ids", [[]])
        documents = result.get("documents", [[]])
        metadatas = result.get("metadatas", [[]])
        distances = result.get("distances", [[]])

        parsed_results: list[ChromaQueryResult] = []
        for document_id, content, metadata, distance in zip(
            ids[0] if ids else [],
            documents[0] if documents else [],
            metadatas[0] if metadatas else [],
            distances[0] if distances else [],
        ):
            parsed_results.append(
                ChromaQueryResult(
                    document_id=document_id,
                    content=content,
                    metadata=metadata,
                    distance=distance,
                )
            )
        return parsed_results

    @staticmethod
    def _parse_chroma_get_response(
        result: dict[str, Any],
        *,
        include_documents: bool = False,
    ) -> list[ChromaStoredDocument]:
        """Parse Chroma's get result into typed stored-document rows."""
        ids = result.get("ids", [])
        metadatas = result.get("metadatas", [])
        documents = result.get("documents", []) if include_documents else []

        parsed_results: list[ChromaStoredDocument] = []
        for index, document_id in enumerate(ids):
            parsed_results.append(
                ChromaStoredDocument(
                    document_id=document_id,
                    metadata=metadatas[index] if index < len(metadatas) else {},
                    content=documents[index]
                    if include_documents and index < len(documents)
                    else None,
                )
            )
        return parsed_results
