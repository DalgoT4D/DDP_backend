"""Rendering tool replies for the LLM: result rows, creations and refusals."""

from pydantic import ValidationError

from ddpui.schemas.chat_with_data_schemas import ArtifactType, CreatedArtifact, RejectedArtifact
from ddpui.core.ai.typed_dicts import CreationArtifact

# Cap on characters per cell when rendering results/samples for the LLM
MAX_CELL_CHARS = 120


def truncate_cell(value) -> str:
    text = "" if value is None else str(value)
    if len(text) > MAX_CELL_CHARS:
        return text[: MAX_CELL_CHARS - 1] + "…"
    return text


def render_rows(rows: list[dict], max_rows: int) -> str:
    """Compact pipe-separated rendering of query rows for the LLM."""
    if not rows:
        return "(no rows)"
    shown = rows[:max_rows]
    columns = list(shown[0].keys())
    lines = [" | ".join(columns)]
    for row in shown:
        lines.append(" | ".join(truncate_cell(row.get(col)) for col in columns))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows not shown)")
    return "\n".join(lines)


def rejection(
    artifact_type: ArtifactType, message: str, reason: str
) -> tuple[str, CreationArtifact]:
    """A creation tool's refusal: LLM-readable text + the rejected artifact."""
    return f"{message}: {reason}", RejectedArtifact(type=artifact_type, error=reason).model_dump()


def created(artifact: CreatedArtifact, content: str) -> tuple[str, CreationArtifact]:
    """A creation tool's success: LLM-readable text + the created artifact."""
    return content, artifact.model_dump()


def error_reason(err: Exception) -> str:
    """First line of a service/validation error, short enough for the LLM."""
    if isinstance(err, ValidationError):
        return "; ".join(
            f"{'.'.join(str(part) for part in e['loc'])}: {e['msg']}" if e["loc"] else e["msg"]
            for e in err.errors()
        )[:300]
    return str(getattr(err, "message", None) or err).split("\n", maxsplit=1)[0][:300]
