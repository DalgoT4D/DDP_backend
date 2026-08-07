"""Pydantic schemas for Chat with Data REST endpoints."""

from datetime import datetime
from typing import Optional

from ninja import Schema


class StatusResponse(Schema):
    """Whether the chat surface is usable for this org, and why not if not."""

    enabled: bool
    # feature_disabled | llm_consent_required | no_warehouse | ok
    reason: str


class SessionCreate(Schema):
    """Optional create payload. Omitted (legacy clients) means an org-wide chat;
    scope_type="dashboard" + scope_id restricts the session to that dashboard's
    tables."""

    scope_type: str = "org"  # org | dashboard
    scope_id: Optional[int] = None


class SessionOut(Schema):
    id: int
    title: str
    scope_type: str
    scope_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, session) -> "SessionOut":
        return cls(
            id=session.id,
            title=session.title,
            scope_type=session.scope_type,
            scope_id=session.scope_id,
            created_at=session.created_at,
            updated_at=session.updated_at,
        )


class SessionRename(Schema):
    title: str


class SqlAttachment(Schema):
    """A query the agent ran within a turn, replayed for the UI."""

    sql: str
    status: str
    row_count: Optional[int] = None
    columns: Optional[list[str]] = None
    rows: Optional[list[list[str]]] = None


class MessageOut(Schema):
    """One chat bubble: user question or assistant answer (+ its queries)."""

    role: str  # "user" | "assistant"
    content: str
    sql_attachments: list[SqlAttachment] = []
    # charts the agent created in this turn: {chart_id, title, url_path}
    charts: list[dict] = []
