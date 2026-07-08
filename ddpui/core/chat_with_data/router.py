"""Query-understanding router — one cheap call before the agent runs.

Classifies the question so the runner can (a) answer small talk without the
SQL agent, (b) ask for clarification instead of guessing, and (c) tag the turn
with complexity for the reflection gate and for evaluation slicing.

FAIL-OPEN by design: any error, timeout, or unparseable output routes to
data_question/simple — the v1 behavior. The router may only ever divert
obviously-non-data turns; it must never block a real question.
"""

import json
import os
from dataclasses import dataclass, field

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.chat_with_data.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_ROUTER_MODEL = "claude-haiku-4-5"
ROUTER_MAX_TOKENS = 300

INTENTS = {"data_question", "small_talk", "needs_clarification"}
COMPLEXITIES = {"simple", "complex"}

_PROMPT = """Classify one message sent to a data-analysis chat for an NGO.
{history_block}
Message: {question}

Return ONLY JSON:
{{"intent": "data_question" | "small_talk" | "needs_clarification",
 "complexity": "simple" | "complex",
 "entities": [strings — metrics, filter values, time ranges mentioned],
 "clarification": string or null}}

Rules:
- data_question: anything that could be answered from data, even vaguely.
  When unsure, choose data_question.
- small_talk: greetings, thanks, chit-chat with no data request at all.
- needs_clarification: ONLY when the question is so ambiguous no reasonable
  query exists (e.g. "compare them" with no referent). Set "clarification"
  to one short, friendly question to ask back.
- IMPORTANT: if the message refers to the recent conversation ("this",
  "that", "the above", "chart it", a short answer to the assistant's last
  question), it is a data_question — the analyst agent holds the full
  history and will resolve the reference. Never ask to re-state context
  that already appears in the conversation.
- complexity "complex": needs multiple tables, comparisons across groups or
  time periods, or "top N by X" ranking. Otherwise "simple"."""


@dataclass(frozen=True)
class RouteResult:
    intent: str = "data_question"
    complexity: str = "simple"
    entities: list[str] = field(default_factory=list)
    clarification: str | None = None


FAIL_OPEN = RouteResult()


def get_router_model() -> BaseChatModel:
    return ChatAnthropic(
        model=os.getenv("CHAT_WITH_DATA_ROUTER_MODEL", DEFAULT_ROUTER_MODEL),
        max_tokens=ROUTER_MAX_TOKENS,
    )


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
        raw = extract_text(response.content).strip()
        # tolerate models that wrap JSON in a code fence
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        data = json.loads(raw)

        intent = data.get("intent")
        if intent not in INTENTS:
            return FAIL_OPEN
        complexity = data.get("complexity")
        if complexity not in COMPLEXITIES:
            complexity = "simple"

        entities = [str(e) for e in data.get("entities") or [] if isinstance(e, (str, int))]
        clarification = data.get("clarification")
        return RouteResult(
            intent=intent,
            complexity=complexity,
            entities=entities,
            clarification=str(clarification) if clarification else None,
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: router failed; failing open to data_question")
        return FAIL_OPEN
