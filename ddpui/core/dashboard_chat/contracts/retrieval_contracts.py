"""Retrieval-related dashboard chat contracts."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class DashboardChatRetrievedDocument(BaseModel):
    """Retrieved document returned from the vector store."""

    model_config = ConfigDict(frozen=True)

    document_id: str
    source_type: str
    source_identifier: str
    content: str
    dashboard_id: int | None = None
    distance: float | None = None


class DashboardChatSchemaSnippet(BaseModel):
    """Schema description for a warehouse table."""

    model_config = ConfigDict(frozen=True)

    table_name: str
    columns: list[dict[str, Any]]
