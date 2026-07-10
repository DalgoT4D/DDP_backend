"""AI draft of a report's executive summary (one model call, no agent loop).

Reads each frozen component's data through the same ReportService paths the
report page renders with, compacts it, and asks the model for a short summary
in the markdown subset the summary area renders. The result is a DRAFT — the
API returns it to the client for the user to edit and save; nothing is written
to the snapshot here. Unlike the chat's fail-open helpers, errors RAISE: the
user explicitly clicked "Generate summary" and deserves a real answer or a
real error.
"""

import json
import os

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.chat_with_data.messages.content import extract_text
from ddpui.core.reports.report_service import ReportService
from ddpui.models.report import ReportSnapshot
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_SUMMARY_MODEL = "claude-sonnet-5"
SUMMARY_MAX_TOKENS = 1000
# Cap per-component data in the prompt — a table chart can hold hundreds of rows
MAX_COMPONENT_CHARS = 2000


class SummaryGenerationError(Exception):
    """Summary could not be drafted. Message is user-facing (HTTP 400)."""


def get_summary_model() -> BaseChatModel:
    return ChatAnthropic(
        model=os.getenv("REPORT_SUMMARY_MODEL", DEFAULT_SUMMARY_MODEL),
        max_tokens=SUMMARY_MAX_TOKENS,
    )


_PROMPT = """You are writing the executive summary for an NGO's data report. \
The audience is NGO leadership and funders — plain language, no jargon.

Report: "{title}"
Reporting period: {period_start} to {period_end}

The report contains these charts and KPIs, with their data:

{component_blocks}

Write a short executive summary (3-8 sentences, or a few bullets if the report \
covers several distinct topics):
- Lead with the single most important finding, its key number in **bold**.
- Only state numbers that appear in the data above — never estimate or invent.
- If a component says "(data unavailable)", simply don't mention it.
- Formatting allowed: **bold**, "- " bullets, "### " headings. Nothing else — \
no code, no links, no tables.

Executive summary:"""


def generate_report_summary(snapshot: ReportSnapshot, model: BaseChatModel | None = None) -> str:
    """Draft an executive summary from the snapshot's frozen components."""
    components = snapshot.frozen_chart_configs or {}
    if not components:
        raise SummaryGenerationError("This report has no charts to summarize yet.")

    blocks = []
    failed = 0
    for component_id, config in components.items():
        rendered = _component_data(snapshot, component_id, config)
        if rendered is None:
            rendered = "(data unavailable)"
            failed += 1
        title = config.get("title", f"Component {component_id}")
        kind = config.get("chart_type") or config.get("component_type") or "chart"
        blocks.append(f'### "{title}" ({kind})\n{rendered}')

    if failed == len(components):
        raise SummaryGenerationError(
            "We couldn't read any of this report's data right now — try again in a bit."
        )

    prompt = _PROMPT.format(
        title=snapshot.title,
        period_start=snapshot.period_start or "unbounded",
        period_end=snapshot.period_end or "unbounded",
        component_blocks="\n\n".join(blocks),
    )
    model = model or get_summary_model()
    response = model.invoke(prompt)
    # thinking-enabled models return block lists; only the text block is the draft
    return extract_text(response.content).strip()


def _component_data(snapshot, component_id, config) -> str | None:
    """One component's data as compact JSON, or None if it can't be read."""
    try:
        if config.get("component_type") == "kpi":
            data = ReportService.get_report_kpi_data(snapshot.id, int(component_id), snapshot.org)
        else:
            data = ReportService.get_report_chart_data(
                snapshot.id, int(component_id), snapshot.org
            ).get("data")
        return json.dumps(data, default=str)[:MAX_COMPONENT_CHARS]
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            "report summary: component %s of snapshot %s unreadable", component_id, snapshot.id
        )
        return None
