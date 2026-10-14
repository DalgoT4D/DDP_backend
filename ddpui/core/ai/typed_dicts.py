"""The dict shapes that travel through a chat turn, as TypedDicts.

These are data OUR code builds, which must stay plain dicts at runtime: tool
artifacts and graph state are written into LangGraph's Postgres checkpoints
(a Pydantic object there would pin its import path into every saved thread),
and WebSocket events go straight to json.dumps. A TypedDict documents the shape
at zero runtime cost. Data arriving from OUTSIDE (the browser, an LLM reply,
Redis, eval files) is parsed with the Pydantic schemas in
ddpui/schemas/chat_with_data_schemas.py instead.

typing_extensions.TypedDict, not typing's: on Python 3.10 Pydantic can only
validate the former, and PendingInput validates InputRequiredEvent.
"""

from typing import Any, Literal, Optional, Union

from typing_extensions import NotRequired, TypedDict

from ddpui.schemas.chat_with_data_schemas import ArtifactType, Complexity, Intent

# ---------------------------------------------------------------------------
# Tool artifacts (ToolMessage.artifact — checkpointed)
# ---------------------------------------------------------------------------

SqlStatus = Literal["success", "rejected", "error"]


class SqlArtifact(TypedDict):
    """execute_sql's artifact. A success carries the result; a rejection
    (guard, PII rewrite, reflection) or a warehouse error carries `error`."""

    sql: str
    status: SqlStatus
    row_count: NotRequired[int]
    columns: NotRequired[list[str]]
    # truncated cell strings, at most max_result_rows rows
    rows: NotRequired[list[list[str]]]
    error: NotRequired[str]


class CreationArtifact(TypedDict):
    """A creation tool's artifact: a dumped CreatedArtifact, or a dumped
    RejectedArtifact (status "rejected" + error). Read only through
    messages/artifacts.py."""

    type: ArtifactType
    object_id: NotRequired[int]
    title: NotRequired[str]
    url_path: NotRequired[str]
    status: NotRequired[Literal["rejected"]]
    error: NotRequired[str]
    # checkpoints written before CreatedArtifact carried object_id
    chart_id: NotRequired[int]
    dashboard_id: NotRequired[int]


ToolArtifact = Union[SqlArtifact, CreationArtifact]


class CreatedArtifactChip(TypedDict):
    """A created object's link chip in the UI (a dumped CreatedArtifact)."""

    type: ArtifactType
    object_id: int
    title: str
    url_path: str


# ---------------------------------------------------------------------------
# One turn's results, derived from the artifacts (messages/artifacts.py)
# ---------------------------------------------------------------------------


class SqlQueryEntry(TypedDict):
    """One execute_sql call as the audit row and the turn-audit prompt see it."""

    sql: Optional[str]
    status: Optional[SqlStatus]
    row_count: Optional[int]
    error: Optional[str]


class ResultTable(TypedDict):
    """The result table the UI renders under an answer."""

    columns: list[str]
    rows: list[list[str]]
    row_count: int


# ---------------------------------------------------------------------------
# Graph state values (chat/turn_graph.py — checkpointed)
# ---------------------------------------------------------------------------


class RouteState(TypedDict):
    """A dumped RouteResult: the router's classification of the question."""

    intent: Intent
    complexity: Complexity
    entities: list[str]
    clarification: Optional[str]


class TurnValidation(TypedDict):
    """A dumped TurnAuditReply: the post-answer audit's verdict."""

    verdict: Literal["ok", "warn"]
    assumptions: list[str]
    caveat: Optional[str]


# ---------------------------------------------------------------------------
# Human-in-the-loop cards (agent/hitl.py)
# ---------------------------------------------------------------------------


class PiiColumn(TypedDict):
    """One checkbox on an approval card: a column the query returns."""

    schema: str
    table: str
    column: str
    # the projection wraps the column in a literal (e.g. CONCAT(name, '!'))
    has_literal: bool


class CardRequest(TypedDict):
    """One paused tool call as the UI renders it."""

    tool: str
    args: dict[str, Any]
    description: str
    sql: Optional[str]
    # PII review tools only. None = the list could not be built, and the card
    # must not be approvable (fail-closed); absent = not a PII review tool.
    columns: NotRequired[Optional[list[PiiColumn]]]
    columns_error: NotRequired[str]


# ---------------------------------------------------------------------------
# WebSocket events, server → browser (protocol in chat/turn_runner.py)
# ---------------------------------------------------------------------------


class TokenUsage(TypedDict):
    input_tokens: int
    output_tokens: int


class TokenEvent(TypedDict):
    type: Literal["token"]
    text: str


class ToolStartEvent(TypedDict):
    type: Literal["tool_start"]
    tool: str
    label: str
    sql: Optional[str]


class ToolEndEvent(TypedDict):
    type: Literal["tool_end"]
    tool: str
    status: Literal["success", "error"]


class MessageCompleteEvent(TypedDict):
    type: Literal["message_complete"]
    message: str
    result_table: Optional[ResultTable]
    artifacts: list[CreatedArtifactChip]
    usage: TokenUsage


class ValidationEvent(TypedDict):
    """Arrives after message_complete; carries TurnValidation's fields."""

    type: Literal["validation"]
    verdict: Literal["ok", "warn"]
    assumptions: list[str]
    caveat: Optional[str]


class InputRequiredEvent(TypedDict):
    """The turn paused for the user: approve/cancel cards, or one question."""

    type: Literal["input_required"]
    kind: Literal["approval", "question"]
    requests: list[CardRequest]
    # kind "question" only: the agent's ask_user question
    question: NotRequired[str]
    # stamped by the runner so a resume continues the same Langfuse trace
    trace_id: NotRequired[str]


class ErrorEvent(TypedDict):
    type: Literal["error"]
    message: str


class TitleUpdatedEvent(TypedDict):
    """Sent by the consumer once the session's title is generated."""

    type: Literal["title_updated"]
    title: str


# What run_turn yields
TurnEvent = Union[
    TokenEvent,
    ToolStartEvent,
    ToolEndEvent,
    MessageCompleteEvent,
    ValidationEvent,
    InputRequiredEvent,
    ErrorEvent,
]
# What the consumer sends
ChatEvent = Union[TurnEvent, TitleUpdatedEvent]
