"""Rendering tool replies for the LLM: result rows and refusals."""

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


def rejection(artifact_type: str, message: str, reason: str) -> tuple[str, dict]:
    """A creation tool's refusal: LLM-readable text + the rejected artifact
    (same shape for charts and dashboards, so the artifact contract holds)."""
    return f"{message}: {reason}", {
        "type": artifact_type,
        "status": "rejected",
        "error": reason,
    }
