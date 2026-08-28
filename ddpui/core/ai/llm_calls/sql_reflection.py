"""Pre-execution SQL reflection — complex lane only.

One cheap checklist call reviewing the agent's SQL against the question BEFORE
it runs. Only fires when the router classified the question as complex (joins,
comparisons, top-N) — the AST guard already covers safety on every lane, and
taxing the 80% simple questions with an extra call isn't worth it.

Sync on purpose: it runs inside the execute_sql tool, which LangGraph executes
in a worker thread. FAIL-OPEN: any error means "no issue found".
"""

from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.ai.agent.base import build_model
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
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
    return build_model(
        "CHAT_WITH_DATA_REFLECTION_MODEL", DEFAULT_REFLECTION_MODEL, REFLECTION_MAX_TOKENS
    )


def find_sql_issue(
    question: str, sql: str, dialect: str, model: BaseChatModel | None = None
) -> str | None:
    """The problem found, or None (clean SQL / reflection unavailable)."""
    try:
        model = model or get_reflection_model()
        response = model.invoke(
            _PROMPT.format(question=question[:1000], sql=sql[:4000], dialect=dialect)
        )
        data = parse_json_reply(extract_text(response.content))
        if data.get("ok") is False and data.get("issue"):
            return str(data["issue"])
        return None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: reflection failed (failing open)")
        return None
