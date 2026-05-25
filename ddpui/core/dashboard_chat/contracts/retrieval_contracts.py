"""Retrieval-related dashboard chat contracts."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict


class DashboardChatSourceType(str, Enum):
    """Supported source types for citations and stored tool payloads."""

    ORG_CONTEXT = "org_context"
    DASHBOARD_CONTEXT = "dashboard_context"
    DASHBOARD_EXPORT = "dashboard_export"


class DashboardChatRetrievedDocument(BaseModel):
    """Structured source payload used for citations and answer composition."""

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
