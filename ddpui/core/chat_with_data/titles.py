"""Auto-generate a session title after the first exchange (one cheap Haiku call).

Failure is always non-fatal: a session keeps its default title rather than
blocking or erroring the chat.
"""

import os

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.chat_with_data.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_TITLE_MODEL = "claude-haiku-4-5"
TITLE_MAX_TOKENS = 50
TITLE_MAX_CHARS = 60

_PROMPT = (
    "Write a title (3-6 words, no quotes, no trailing punctuation) for a data "
    "chat that starts with this question:\n\n{question}\n\nTitle:"
)


def get_title_model() -> BaseChatModel:
    return ChatAnthropic(
        model=os.getenv("CHAT_WITH_DATA_TITLE_MODEL", DEFAULT_TITLE_MODEL),
        max_tokens=TITLE_MAX_TOKENS,
    )


async def generate_session_title(
    question: str, answer: str, model: BaseChatModel | None = None
) -> str | None:
    """A short human title for the session, or None if generation fails."""
    try:
        model = model or get_title_model()
        response = await model.ainvoke(_PROMPT.format(question=question[:500]))
        title = extract_text(response.content).strip().strip('"').strip()
        return title[:TITLE_MAX_CHARS] or None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: title generation failed")
        return None
