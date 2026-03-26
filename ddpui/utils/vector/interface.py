"""Abstract vector store interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union


@dataclass(frozen=True)
class VectorQueryResult:
    """Single result from a vector similarity query."""

    document_id: str
    content: str
    metadata: dict[str, Any]
    distance: Union[float, None] = None


@dataclass(frozen=True)
class VectorStoredDocument:
    """Stored document returned from a vector collection read."""

    document_id: str
    metadata: dict[str, Any]
    content: Union[str, None] = None


class VectorStore(ABC):
    """Abstract interface for a vector store backend."""

    @abstractmethod
    def create_collection(
        self,
        name: str,
        *,
        metadata: Union[dict[str, Any], None] = None,
    ) -> Any:
        """Create or load a collection by name."""

    @abstractmethod
    def load_collection(self, name: str) -> Union[Any, None]:
        """Load an existing collection by name, or return None if not found."""

    @abstractmethod
    def delete_collection(self, name: str) -> bool:
        """Delete a collection if it exists. Returns True if deleted."""

    @abstractmethod
    def list_collection_names(self) -> list[str]:
        """Return all collection names available in this backend."""

    @abstractmethod
    def get_documents(
        self,
        collection_name: str,
        *,
        where: Union[dict[str, Any], None] = None,
        include_documents: bool = False,
    ) -> list[VectorStoredDocument]:
        """Read documents from a collection using an optional metadata filter."""

    @abstractmethod
    def delete_documents(
        self,
        collection_name: str,
        *,
        ids: Union[list[str], None] = None,
        where: Union[dict[str, Any], None] = None,
    ) -> int:
        """Delete documents by ids and/or metadata filter. Returns count deleted."""

    @abstractmethod
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
        """Upsert documents into a collection. Returns the upserted ids."""

    @abstractmethod
    def query(
        self,
        collection_name: str,
        *,
        query_embedding: list[float],
        n_results: int = 5,
        where: Union[dict[str, Any], None] = None,
    ) -> list[VectorQueryResult]:
        """Query a collection using a precomputed embedding."""
