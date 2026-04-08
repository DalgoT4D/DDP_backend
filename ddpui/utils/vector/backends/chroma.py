"""Chroma vector store backend implementing the VectorStore interface."""

from functools import lru_cache
from typing import Any, Union

from chromadb import ClientAPI, Collection, HttpClient
from chromadb.errors import NotFoundError

from ddpui.utils.vector.interface import VectorStore, VectorQueryResult, VectorStoredDocument


@lru_cache(maxsize=8)
def _get_chroma_client(host: str, port: int, ssl: bool) -> ClientAPI:
    """Return a shared Chroma HTTP client for one host/port/ssl tuple."""
    return HttpClient(host=host, port=port, ssl=ssl)


class ChromaVectorStore(VectorStore):
    """Chroma HTTP backend implementing the VectorStore interface."""

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 8000,
        ssl: bool = False,
        client: Union[ClientAPI, None] = None,
    ):
        self.client = client or _get_chroma_client(host, port, ssl)

    def create_collection(
        self,
        name: str,
        *,
        metadata: Union[dict[str, Any], None] = None,
    ) -> Collection:
        return self.client.get_or_create_collection(name=name, metadata=metadata)

    def load_collection(self, name: str) -> Union[Collection, None]:
        try:
            return self.client.get_collection(name=name)
        # In recent Chroma releases, a missing collection resolves as NotFoundError.
        # For our store interface, that means "this collection is absent".
        except NotFoundError:
            return None

    def delete_collection(self, name: str) -> bool:
        if self.load_collection(name) is None:
            return False
        self.client.delete_collection(name=name)
        return True

    def list_collection_names(self) -> list[str]:
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
        where: Union[dict[str, Any], None] = None,
        include_documents: bool = False,
    ) -> list[VectorStoredDocument]:
        collection = self.load_collection(collection_name)
        if collection is None:
            return []
        include = ["metadatas"]
        if include_documents:
            include.append("documents")
        result = collection.get(where=where, include=include)
        return self._parse_get_response(result, include_documents=include_documents)

    def delete_documents(
        self,
        collection_name: str,
        *,
        ids: Union[list[str], None] = None,
        where: Union[dict[str, Any], None] = None,
    ) -> int:
        collection = self.load_collection(collection_name)
        if collection is None:
            return 0
        if ids is None and where is None:
            return 0
        deleted_count = (
            len(ids) if ids is not None else len(self.get_documents(collection_name, where=where))
        )
        collection.delete(ids=ids, where=where)
        return deleted_count

    def upsert(
        self,
        collection_name: str,
        *,
        ids: list[str],
        documents: list[str],
        metadatas: list[dict[str, Any]],
        embeddings: list[list[float]],
        collection_metadata: Union[dict[str, Any], None] = None,
    ) -> list[str]:
        if not ids:
            return []
        collection = self.create_collection(collection_name, metadata=collection_metadata)
        collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)
        return ids

    def query(
        self,
        collection_name: str,
        *,
        query_embedding: list[float],
        n_results: int = 5,
        where: Union[dict[str, Any], None] = None,
    ) -> list[VectorQueryResult]:
        collection = self.load_collection(collection_name)
        if collection is None:
            return []
        result = collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=where,
            include=["documents", "metadatas", "distances"],
        )
        return self._parse_query_response(result)

    @staticmethod
    def _parse_query_response(result: dict[str, Any]) -> list[VectorQueryResult]:
        ids = result.get("ids", [[]])
        documents = result.get("documents", [[]])
        metadatas = result.get("metadatas", [[]])
        distances = result.get("distances", [[]])
        return [
            VectorQueryResult(
                document_id=document_id,
                content=content,
                metadata=metadata,
                distance=distance,
            )
            for document_id, content, metadata, distance in zip(
                ids[0] if ids else [],
                documents[0] if documents else [],
                metadatas[0] if metadatas else [],
                distances[0] if distances else [],
            )
        ]

    @staticmethod
    def _parse_get_response(
        result: dict[str, Any],
        *,
        include_documents: bool = False,
    ) -> list[VectorStoredDocument]:
        ids = result.get("ids", [])
        metadatas = result.get("metadatas", [])
        documents = result.get("documents", []) if include_documents else []
        return [
            VectorStoredDocument(
                document_id=document_id,
                metadata=metadatas[index] if index < len(metadatas) else {},
                content=(
                    documents[index] if include_documents and index < len(documents) else None
                ),
            )
            for index, document_id in enumerate(ids)
        ]
