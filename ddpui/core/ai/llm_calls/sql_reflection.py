"""Pre-execution SQL reflection — complex lane only.

One cheap checklist call reviewing the agent's SQL against the question BEFORE
it runs. Only fires when the router classified the question as complex (joins,
comparisons, top-N) — the AST guard already covers safety on every lane, and
taxing the 80% simple questions with an extra call isn't worth it.

Sync on purpose: it runs inside the execute_sql tool, which LangGraph executes
in a worker thread. FAIL-OPEN: any error means "no issue found".
"""

import json
import os

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.ai.messages.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_REFLECTION_MODEL = "claude-haiku-4-5"
REFLECTION_MAX_TOKENS = 300

_PROMPT = """Review this SQL against the question BEFORE it runs. Only flag \
problems that would make the ANSWER WRONG — not style.

Question: {question}
Dialect: {dialect}
SQL:
{sql}

Check: (1) do the joins duplicate or drop rows relative to what the question
asks, (2) does the grouping/aggregation match the entities being counted or
compared, (3) is any condition from the question missing?

Return ONLY JSON: {{"ok": true}} if the SQL is sound, or
{{"ok": false, "issue": "one short sentence naming the problem"}}"""


def get_reflection_model() -> BaseChatModel:
    return ChatAnthropic(
        model=os.getenv("CHAT_WITH_DATA_REFLECTION_MODEL", DEFAULT_REFLECTION_MODEL),
        max_tokens=REFLECTION_MAX_TOKENS,
    )


def check_sql(
    question: str, sql: str, dialect: str, model: BaseChatModel | None = None
) -> str | None:
    """The problem found, or None (clean SQL / reflection unavailable)."""
    try:
        model = model or get_reflection_model()
        response = model.invoke(
            _PROMPT.format(question=question[:1000], sql=sql[:4000], dialect=dialect)
        )
        raw = extract_text(response.content).strip()
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        data = json.loads(raw)
        if data.get("ok") is False and data.get("issue"):
            return str(data["issue"])
        return None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: reflection failed (failing open)")
        return None
