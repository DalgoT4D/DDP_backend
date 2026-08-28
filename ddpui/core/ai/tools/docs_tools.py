"""Dalgo docs tool: fetch a how-to page from docs.dalgo.org for the guide agent.

TOPIC_MAP is a curated topic → page map over the NGO-facing docs (the
developer section /self-serve-documentation and /release-notes are excluded
on purpose — the first would confuse program managers, the second describes
UI changes and goes stale). Pages are short (200–1,200 word) step-by-step
walkthroughs, so a whole page fits in context — no chunking or retrieval.

Fetched pages are cached in Redis for 24h; cache or network failures degrade
to a friendly message with the URL, never an exception.
"""

import json
import os

import requests
from bs4 import BeautifulSoup
from langchain.tools import tool

from ddpui.core.ai.tools.registry import register_tool
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("ddpui")

DOCS_BASE_URL = os.getenv("DALGO_DOCS_BASE_URL", "https://docs.dalgo.org")
FETCH_TIMEOUT_S = 10
CACHE_TTL_S = 24 * 60 * 60
MAX_PAGE_CHARS = 6000

# topic slug → docs path (NGO-facing pages only)
TOPIC_MAP = {
    "charts_overview": "/charts/",
    "chart_types": "/charts/chart-types",
    "creating_a_chart": "/charts/creating-a-chart",
    "dashboards_overview": "/dashboards/",
    "creating_a_dashboard": "/dashboards/creating",
    "viewing_dashboards": "/dashboards/viewing",
    "kpis_overview": "/kpis/",
    "creating_a_kpi": "/kpis/creating-a-kpi",
    "metrics": "/data/metrics",
    "reports_overview": "/reports/",
    "creating_a_report": "/reports/creating",
    "sharing_reports": "/reports/sharing",
    "exporting_reports": "/reports/exporting",
    "alerts": "/alerts/creating-an-alert",
    "data_ingestion": "/data/ingest/sources",
    "connections": "/data/ingest/connections",
    "pipelines": "/data/orchestrate",
    "transformations": "/data/transform/ui-transform",
    "data_quality": "/data/quality",
    "explore_data": "/data/explore",
    "user_management": "/settings/user-management",
    "glossary": "/concepts/glossary",
    "quickstart": "/quickstart/",
    "getting_help": "/support/getting-help",
}

_DESCRIPTION = (
    "Read one page of the Dalgo user guide (docs.dalgo.org) to answer how-to "
    "questions accurately. Returns the page text and its URL — always share "
    "that URL with the user as the place to read more. Valid topics: "
    + ", ".join(sorted(TOPIC_MAP))
)


def _page_text(html: str) -> str:
    """The readable text of a docs page: main content only, nav/footer stripped."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup.find("article") or soup
    text = main.get_text("\n", strip=True)
    if len(text) > MAX_PAGE_CHARS:
        text = text[:MAX_PAGE_CHARS] + "\n… (page truncated)"
    return text


def _cache_get(key: str) -> str | None:
    try:
        cached = RedisClient.get_instance().get(key)
        return json.loads(cached)["text"] if cached else None
    except Exception:  # pylint: disable=broad-except
        return None  # cache trouble must never block a docs answer


def _cache_set(key: str, text: str) -> None:
    try:
        RedisClient.get_instance().set(key, json.dumps({"text": text}), ex=CACHE_TTL_S)
    except Exception:  # pylint: disable=broad-except
        logger.warning(f"docs cache write failed for {key}")


@register_tool
@tool(description=_DESCRIPTION)
def get_dalgo_help(topic: str) -> str:
    """Fetch one Dalgo docs page as text. See tool description for topics."""
    slug = topic.strip().lower().replace("-", "_").replace(" ", "_")
    if slug not in TOPIC_MAP:
        return (
            f"Unknown topic '{topic}'. Valid topics: {', '.join(sorted(TOPIC_MAP))}. "
            "Pick the closest one."
        )
    url = DOCS_BASE_URL + TOPIC_MAP[slug]

    cache_key = f"dalgo_docs:{slug}"
    text = _cache_get(cache_key)
    if text is None:
        try:
            response = requests.get(url, timeout=FETCH_TIMEOUT_S)
            response.raise_for_status()
            text = _page_text(response.text)
            _cache_set(cache_key, text)
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"docs fetch failed for {url}")
            return (
                "Couldn't reach the Dalgo guide right now — answer from what you "
                f"know and point the user to {url} for the full steps."
            )
    return f"{text}\n\nRead more: {url}"
