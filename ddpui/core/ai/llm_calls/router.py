"""Query-understanding router — one cheap call before the agent runs.

Classifies the question so the runner can (a) answer small talk without the
SQL agent, (b) ask for clarification instead of guessing, and (c) tag the turn
with complexity for the reflection gate and for evaluation slicing.

FAIL-OPEN by design: any error, timeout, or unparseable output routes to
data_question/simple — the v1 behavior. The router may only ever divert
obviously-non-data turns; it must never block a real question.
"""

import dataclasses
import re
from dataclasses import dataclass, field

from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.ai.agent.base import build_model
from ddpui.core.ai.constants import FAST_MODEL, ROUTER_MAX_TOKENS, ROUTER_MODEL_ENV_VAR
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.prompts import ROUTER_PROMPT, SMALL_TALK_PROMPT
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

INTENTS = {"data_question", "platform_help", "small_talk", "needs_clarification"}
COMPLEXITIES = {"simple", "complex"}


@dataclass(frozen=True)
class RouteResult:
    intent: str = "data_question"
    complexity: str = "simple"
    entities: list[str] = field(default_factory=list)
    clarification: str | None = None


FAIL_OPEN = RouteResult()

# Deterministic backstop: an explicit creation request must reach the guide
# agent even when the model misroutes it (e.g. follow-up stickiness in a data
# conversation) or the router fails open. Two shapes: a creation verb followed
# by a platform object ("create a KPI for silt carted"), and "chart/plot this".
_CREATION_REQUEST = re.compile(
    r"\b(create|make|build|generate|set\s?up|add)\b"
    r".{0,60}?\b(chart|graph|dashboard|kpi|metric|report)s?\b",
    re.IGNORECASE | re.DOTALL,
)
_VISUALIZE_REFERENCE = re.compile(
    r"\b(chart|plot|graph|visuali[sz]e)\b\s+(this|that|it|these|them)\b", re.IGNORECASE
)


def _apply_platform_help_backstop(question: str, route: RouteResult) -> RouteResult:
    """Force platform_help for unmistakable creation requests. Never touches
    small_talk (the phrases can't be greetings) — only routes that would
    otherwise send a creation request into the SQL agent."""
    if route.intent == "platform_help":
        return route
    if _CREATION_REQUEST.search(question) or _VISUALIZE_REFERENCE.search(question):
        return dataclasses.replace(route, intent="platform_help", clarification=None)
    return route


def get_router_model() -> BaseChatModel:
    return build_model(ROUTER_MODEL_ENV_VAR, FAST_MODEL, ROUTER_MAX_TOKENS)


FALLBACK_REPLY = "Happy to help! Ask me anything about your organization's data."


async def casual_reply(question: str, model: BaseChatModel | None = None) -> str:
    """A short friendly reply for small talk. Falls back to a canned line."""
    try:
        model = model or get_router_model()
        response = await model.ainvoke(SMALL_TALK_PROMPT.format(question=question[:500]))
        return extract_text(response.content).strip() or FALLBACK_REPLY
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: casual reply failed; using fallback")
        return FALLBACK_REPLY


async def route_question(
    question: str,
    model: BaseChatModel | None = None,
    history: list[str] | None = None,
) -> RouteResult:
    """Classify the question; on ANY failure return the fail-open route.

    `history` is a compact tail of the conversation ("User: …"/"Assistant: …"
    lines) — without it, every follow-up that says "this"/"that" looks
    ambiguous in isolation and gets wrongly diverted from the agent."""
    try:
        model = model or get_router_model()
        if history:
            history_block = "\nRecent conversation (oldest first):\n" + "\n".join(history) + "\n"
        else:
            history_block = ""
        response = await model.ainvoke(
            ROUTER_PROMPT.format(question=question[:1000], history_block=history_block)
        )
        data = parse_json_reply(extract_text(response.content))

        intent = data.get("intent")
        if intent not in INTENTS:
            return _apply_platform_help_backstop(question, FAIL_OPEN)
        complexity = data.get("complexity")
        if complexity not in COMPLEXITIES:
            complexity = "simple"

        entities = [str(e) for e in data.get("entities") or [] if isinstance(e, (str, int))]
        clarification = data.get("clarification")
        return _apply_platform_help_backstop(
            question,
            RouteResult(
                intent=intent,
                complexity=complexity,
                entities=entities,
                clarification=str(clarification) if clarification else None,
            ),
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: router failed; failing open to data_question")
        return _apply_platform_help_backstop(question, FAIL_OPEN)
