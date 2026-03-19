"""Deterministic vector document models for dashboard chat retrieval."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any


class DashboardChatSourceType(str, Enum):
    """Context source types supported by dashboard chat retrieval."""

    ORG_CONTEXT = "org_context"
    DASHBOARD_CONTEXT = "dashboard_context"
    DASHBOARD_EXPORT = "dashboard_export"
    DBT_MANIFEST = "dbt_manifest"
    DBT_CATALOG = "dbt_catalog"


def build_dashboard_chat_collection_name(org_id: int, prefix: str = "org_") -> str:
    """Build the per-org Chroma collection name."""
    return f"{prefix}{org_id}"


def compute_dashboard_chat_document_hash(content: str) -> str:
    """Compute a stable content hash used in vector metadata and IDs."""
    return sha256(content.encode("utf-8")).hexdigest()


def build_dashboard_chat_document_id(
    org_id: int,
    source_type: str,
    source_identifier: str,
    chunk_index: int,
    content_hash: str,
) -> str:
    """Build a deterministic document ID for Chroma upserts."""
    raw_identifier = ":".join(
        [
            str(org_id),
            source_type,
            source_identifier,
            str(chunk_index),
            content_hash,
        ]
    )
    return sha256(raw_identifier.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DashboardChatVectorDocument:
    """Single retrieval chunk stored in the dashboard chat vector store."""

    org_id: int
    source_type: DashboardChatSourceType | str
    source_identifier: str
    content: str
    dashboard_id: int | None = None
    chart_id: int | None = None
    title: str | None = None
    chunk_index: int = 0
    updated_at: datetime | None = None

    @property
    def source_type_value(self) -> str:
        """Return the string form of the source type."""
        if isinstance(self.source_type, DashboardChatSourceType):
            return self.source_type.value
        return self.source_type

    @property
    def document_hash(self) -> str:
        """Return a stable content hash for this document."""
        return compute_dashboard_chat_document_hash(self.content)

    @property
    def document_id(self) -> str:
        """Return the deterministic Chroma ID for this document."""
        return build_dashboard_chat_document_id(
            org_id=self.org_id,
            source_type=self.source_type_value,
            source_identifier=self.source_identifier,
            chunk_index=self.chunk_index,
            content_hash=self.document_hash,
        )

    def metadata(self) -> dict[str, Any]:
        """Return Chroma-safe metadata for this document."""
        metadata: dict[str, Any] = {
            "org_id": self.org_id,
            "source_type": self.source_type_value,
            "source_identifier": self.source_identifier,
            "chunk_index": self.chunk_index,
            "document_hash": self.document_hash,
        }
        if self.dashboard_id is not None:
            metadata["dashboard_id"] = self.dashboard_id
        if self.chart_id is not None:
            metadata["chart_id"] = self.chart_id
        if self.title is not None:
            metadata["title"] = self.title
        if self.updated_at is not None:
            metadata["updated_at"] = self.updated_at.isoformat()
        return metadata
