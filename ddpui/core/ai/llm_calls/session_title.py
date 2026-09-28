"""Auto-generate a session title after the first exchange (one cheap Haiku call).

Failure is always non-fatal: a session keeps its default title rather than
blocking or erroring the chat.
"""

from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.ai.agent.base import build_model
from ddpui.core.ai.constants import FAST_MODEL, TITLE_MAX_TOKENS, TITLE_MODEL_ENV_VAR
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.prompts import SESSION_TITLE_PROMPT
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

TITLE_MAX_CHARS = 60


def get_title_model() -> BaseChatModel:
    return build_model(TITLE_MODEL_ENV_VAR, FAST_MODEL, TITLE_MAX_TOKENS)


async def generate_session_title(
    question: str, answer: str, model: BaseChatModel | None = None
) -> str | None:
    """A short human title for the session, or None if generation fails."""
    try:
        model = model or get_title_model()
        response = await model.ainvoke(SESSION_TITLE_PROMPT.format(question=question[:500]))
        title = extract_text(response.content).strip().strip('"').strip()
        return title[:TITLE_MAX_CHARS] or None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: title generation failed")
        return None
