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
from ddpui.core.ai.constants import FAST_MODEL, REFLECTION_MAX_TOKENS, REFLECTION_MODEL_ENV_VAR
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.prompts import SQL_REFLECTION_PROMPT
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def get_reflection_model() -> BaseChatModel:
    return build_model(REFLECTION_MODEL_ENV_VAR, FAST_MODEL, REFLECTION_MAX_TOKENS)


def find_sql_issue(
    question: str, sql: str, dialect: str, model: BaseChatModel | None = None
) -> str | None:
    """The problem found, or None (clean SQL / reflection unavailable)."""
    try:
        model = model or get_reflection_model()
        response = model.invoke(
            SQL_REFLECTION_PROMPT.format(question=question[:1000], sql=sql[:4000], dialect=dialect)
        )
        data = parse_json_reply(extract_text(response.content))
        if data.get("ok") is False and data.get("issue"):
            return str(data["issue"])
        return None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: reflection failed (failing open)")
        return None
