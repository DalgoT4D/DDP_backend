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
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
from ddpui.core.ai.messages.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_ROUTER_MODEL = "claude-haiku-4-5"
ROUTER_MAX_TOKENS = 300

INTENTS = {"data_question", "platform_help", "small_talk", "needs_clarification"}
COMPLEXITIES = {"simple", "complex"}

_PROMPT = """Classify one message sent to a data-analysis chat for an NGO.
{history_block}
Message: {question}

Return ONLY JSON:
{{"intent": "data_question" | "platform_help" | "small_talk" | "needs_clarification",
 "complexity": "simple" | "complex",
 "entities": [strings — metrics, filter values, time ranges mentioned],
 "clarification": string or null}}

Rules:
- data_question: asks for numbers, facts, or analysis FROM the org's data
  ("how many surveys in MH?", "top districts by enrollment"). When unsure
  between data_question and platform_help, choose data_question.
- platform_help: asks to CREATE or set up a platform object — chart,
  dashboard, KPI, metric, report — or asks HOW to use a Dalgo feature.
  Examples: "make me a chart of surveys by state", "create a KPI for
  survey completion", "how do I share a report?", "what is a metric?".
- small_talk: greetings, thanks, chit-chat with no request at all.
- needs_clarification: ONLY when the question is so ambiguous no reasonable
  query exists (e.g. "compare them" with no referent). Set "clarification"
  to one short, friendly question to ask back.
- IMPORTANT: an explicit creation request ("create/make/build a chart,
  KPI, dashboard, metric, report") is ALWAYS platform_help — even in the
  middle of a data conversation. The guide sees the full conversation, so
  "create a chart of this" works.
- IMPORTANT: for OTHER follow-ups that refer to the recent conversation
  ("this", "that", "the above", a short answer to the assistant's last
  question), keep the SAME intent as that conversation: a follow-up in a
  creation/how-to exchange is platform_help; a follow-up in a data
  exchange is data_question. Never ask to re-state context that already
  appears in the conversation.
- complexity "complex": needs multiple tables, comparisons across groups or
  time periods, or "top N by X" ranking. Otherwise "simple"."""


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
    return build_model("CHAT_WITH_DATA_ROUTER_MODEL", DEFAULT_ROUTER_MODEL, ROUTER_MAX_TOKENS)


_SMALL_TALK_PROMPT = """You are Dalgo's data assistant. The user sent a casual \
message (a greeting, thanks, or chit-chat), not a data question. Reply in one or \
two friendly plain-text sentences. If natural, remind them they can ask about \
their data.

User message: {question}"""

FALLBACK_REPLY = "Happy to help! Ask me anything about your organization's data."


async def casual_reply(question: str, model: BaseChatModel | None = None) -> str:
    """A short friendly reply for small talk. Falls back to a canned line."""
    try:
        model = model or get_router_model()
        response = await model.ainvoke(_SMALL_TALK_PROMPT.format(question=question[:500]))
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
            _PROMPT.format(question=question[:1000], history_block=history_block)
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
