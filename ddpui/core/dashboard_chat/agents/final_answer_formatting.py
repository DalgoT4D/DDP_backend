"""Answer-formatting helpers for dashboard chat LLM responses."""

import json
from typing import Any

from ddpui.core.dashboard_chat.contracts import DashboardChatRetrievedDocument

TABLE_SUMMARY_JSON_INSTRUCTIONS = """
For table-like responses, return valid JSON only with this shape:
{
  "title": "short heading or null",
  "summary": "1-2 sentence narrative summary",
  "key_points": ["short point", "short point"]
}

Rules:
- Do not include markdown tables.
- Do not include pipe characters or ASCII table formatting.
- Do not repeat every row from the result set.
- The UI will render the structured table separately from sql_results.
- Keep key_points to at most 3 concise bullets.
""".strip()


def build_final_answer_context_payload(
    *,
    user_query: str,
    intent: str,
    response_format: str,
    draft_answer: str | None,
    retrieved_documents: list[DashboardChatRetrievedDocument],
    sql: str | None,
    sql_results: list[dict[str, Any]] | None,
    warnings: list[str],
) -> dict[str, Any]:
    """Build the prompt payload used for final answer composition."""
    return {
        "user_query": user_query,
        "intent": intent,
        "response_format": response_format,
        "draft_answer": draft_answer or None,
        "warnings": warnings[:5],
        "sql": sql,
        "sql_results": (sql_results or [])[:8],
        "row_count": len(sql_results or []),
        "retrieved_context": [
            {
                "source_type": document.source_type,
                "source_identifier": document.source_identifier,
                "content": compact_answer_snippet(document.content),
            }
            for document in retrieved_documents[:6]
        ],
    }


def compact_answer_snippet(content: str, max_length: int = 320) -> str:
    """Trim retrieved context before feeding it into the final answer prompt."""
    normalized_content = " ".join(content.split())
    if len(normalized_content) <= max_length:
        return normalized_content
    return normalized_content[: max_length - 1].rstrip() + "…"


def format_table_summary_markdown(result: dict[str, Any]) -> str:
    """Render a structured table summary into short markdown without any table body."""
    title = str(result.get("title") or "").strip()
    summary = str(result.get("summary") or "").strip()
    raw_key_points = result.get("key_points") or []
    key_points = [
        str(point).strip() for point in raw_key_points if isinstance(point, str) and point.strip()
    ][:3]

    sections: list[str] = []
    if title:
        sections.append(f"### {title}")
    if summary:
        sections.append(summary)
    if key_points:
        sections.append("\n".join(f"- {point}" for point in key_points))
    return "\n\n".join(section for section in sections if section).strip()
