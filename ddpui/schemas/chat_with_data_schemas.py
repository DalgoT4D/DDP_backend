"""Pydantic schemas for Chat with Data: REST endpoints, WebSocket client
messages, the JSON replies of the one-shot LLM calls, and eval items."""

from datetime import datetime
from typing import Annotated, Any, Literal, Optional, Union

from ninja import Schema
from pydantic import ConfigDict, Field, StrictBool, field_validator


class ModelOption(Schema):
    """A model the user may pick in the chat UI."""

    id: str
    label: str


class StatusResponse(Schema):
    """Whether the chat surface is usable for this org, and why not if not."""

    enabled: bool
    # feature_disabled | no_warehouse | ok
    reason: str
    # models the user may choose from (empty when disabled); the default is
    # what runs when they never touch the selector
    models: list[ModelOption] = []
    default_model: str | None = None


class CopilotSettingsOut(Schema):
    """Copilot settings for the org: the enable flag + the org memory text."""

    enabled: bool
    text: str
    updated_at: Optional[str] = None
    updated_by_email: Optional[str] = None
    # the memory char cap — the frontend drives its counter from this
    max_chars: int


class CopilotSettingsUpdate(Schema):
    """Partial update: omitted fields stay unchanged."""

    enabled: Optional[bool] = None
    text: Optional[str] = None


class SessionOut(Schema):
    id: int
    title: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, session) -> "SessionOut":
        return cls(
            id=session.id,
            title=session.title,
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


ArtifactType = Literal["chart", "dashboard", "metric", "kpi", "report"]


class CreatedArtifact(Schema):
    """A Dalgo object a creation tool saved — stored as the ToolMessage
    artifact and sent to the UI as a link chip."""

    type: ArtifactType
    object_id: int
    title: str
    url_path: str


class RejectedArtifact(Schema):
    """A creation tool's refusal — never shown as a chip."""

    type: ArtifactType
    status: Literal["rejected"] = "rejected"
    error: str


class MessageOut(Schema):
    """One chat bubble: user question or assistant answer (+ its queries)."""

    role: str  # "user" | "assistant"
    content: str
    sql_attachments: list[SqlAttachment] = []
    # Dalgo objects the agent created in this turn
    artifacts: list[CreatedArtifact] = []


# ---------------------------------------------------------------------------
# WebSocket client messages (websockets/chat_with_data_consumer.py)
# ---------------------------------------------------------------------------


class SendMessageAction(Schema):
    """A new question — or, while an ask_user card is pending, its answer."""

    action: Literal["send_message"]
    message: str = ""
    # the user's model pick; validated against the allowlist by the consumer
    model: Optional[str] = None

    @field_validator("message", mode="before")
    @classmethod
    def _message_as_text(cls, value: Any) -> str:
        return "" if value is None else str(value)

    @field_validator("model", mode="before")
    @classmethod
    def _drop_non_text_model(cls, value: Any) -> Optional[str]:
        # anything but a string falls back to the default model, never errors
        return value if isinstance(value, str) else None


class ResumeApprovalAction(Schema):
    """The user's approve/cancel on a pending approval card."""

    action: Literal["resume_approval"]
    approve: bool = False
    # columns the user ticked as PII; narrowed to what the card offered
    pii_columns: list[str] = []

    @field_validator("pii_columns", mode="before")
    @classmethod
    def _columns_as_text(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(entry) for entry in value]


ChatClientMessage = Annotated[
    Union[SendMessageAction, ResumeApprovalAction], Field(discriminator="action")
]


# ---------------------------------------------------------------------------
# One-shot LLM replies (llm_calls/)
#
# Lenient on purpose: a small model's JSON is coerced where the meaning is
# clear and rejected (ValidationError) where it is not — every caller treats
# a rejection as "no result" and fails open.
# ---------------------------------------------------------------------------

Intent = Literal["data_question", "platform_help", "small_talk", "needs_clarification"]
Complexity = Literal["simple", "complex"]


def _text_or_none(value: Any) -> Optional[str]:
    return str(value) if value else None


class RouteResult(Schema):
    """The router's classification of one question (llm_calls/router.py)."""

    model_config = ConfigDict(frozen=True)

    intent: Intent
    complexity: Complexity = "simple"
    # metrics, filter values, time ranges the question mentions
    entities: list[str] = []
    # the question to ask back when intent is needs_clarification
    clarification: Optional[str] = None

    @field_validator("complexity", mode="before")
    @classmethod
    def _unknown_complexity_is_simple(cls, value: Any) -> str:
        return value if value in ("simple", "complex") else "simple"

    @field_validator("entities", mode="before")
    @classmethod
    def _keep_scalar_entities(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(entity) for entity in value if isinstance(entity, (str, int))]

    @field_validator("clarification", mode="before")
    @classmethod
    def _clarification_as_text(cls, value: Any) -> Optional[str]:
        return _text_or_none(value)


class TurnAuditReply(Schema):
    """The turn audit's verdict on one answer (llm_calls/turn_audit.py)."""

    verdict: Literal["ok", "warn"]
    # what the SQL assumed, in short phrases
    assumptions: list[str] = []
    # one plain-language sentence for the user; None when verdict is ok
    caveat: Optional[str] = None

    @field_validator("assumptions", mode="before")
    @classmethod
    def _assumptions_as_text(cls, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [str(assumption) for assumption in value]
        return [str(value)]

    @field_validator("caveat", mode="before")
    @classmethod
    def _caveat_as_text(cls, value: Any) -> Optional[str]:
        return _text_or_none(value)


class SqlReflectionReply(Schema):
    """The pre-execution SQL review (llm_calls/sql_reflection.py). Only a JSON
    `false` for `ok` flags the SQL — strict so "false" or 0 can't send sound
    SQL back for revision."""

    ok: Optional[StrictBool] = None
    issue: Optional[str] = None

    @field_validator("issue", mode="before")
    @classmethod
    def _issue_as_text(cls, value: Any) -> Optional[str]:
        return _text_or_none(value)


# ---------------------------------------------------------------------------
# Evals (evals/runner.py; the JSONL format is documented in evals/README.md)
# ---------------------------------------------------------------------------


class EvalItem(Schema):
    """One golden question and whatever truth it is scored against."""

    question: str
    expected_intent: Optional[Intent] = None
    # hand-written correct SQL, executed and compared to the agent's result
    gold_sql: Optional[str] = None
    # lighter alternative to gold_sql: must appear in the answer text
    expected_value: Optional[Union[str, int, float]] = None
    # one sentence describing a correct answer, scored by an LLM judge
    answer_expectations: Optional[str] = None
    # metadata for now (future table-selection score)
    expected_tables: list[str] = []
