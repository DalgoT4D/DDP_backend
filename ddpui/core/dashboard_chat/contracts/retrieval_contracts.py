"""Retrieval-related dashboard chat contracts."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DashboardChatRetrievedDocument:
    """Retrieved document returned from the vector store."""

    document_id: str
    source_type: str
    source_identifier: str
    content: str
    dashboard_id: int | None = None
    distance: float | None = None


@dataclass(frozen=True)
class DashboardChatSchemaSnippet:
    """Schema description for a warehouse table."""

    table_name: str
    columns: list[dict[str, Any]]
