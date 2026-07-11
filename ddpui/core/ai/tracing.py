"""Langfuse tracing for Chat with Data — one trace per turn, spans per tool call.

Why a hand-rolled handler: the repo's dbt 1.8 stack pins protobuf<5, which
rules out the Langfuse v3 SDK (OpenTelemetry needs protobuf 5), and the v2
SDK's bundled LangChain handler imports pre-1.x langchain modules. So we use
the v2 SDK's low-level client (plain HTTP, still accepted by current Langfuse
servers) behind a small langchain_core callback handler of our own.

Tracing is OFF unless LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set.
Every call here is wrapped so a tracing failure can never break a chat turn.
"""

import os
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler

from ddpui.core.ai.messages.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# Cap trace payload sizes — traces are for debugging, not archival
MAX_IO_CHARS = 4000

_client = None
_client_initialized = False


def get_langfuse():
    """Singleton Langfuse client, or None when tracing is not configured."""
    global _client, _client_initialized  # pylint: disable=global-statement
    if _client_initialized:
        return _client
    _client_initialized = True

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    if not (public_key and secret_key):
        return None

    try:
        from langfuse import Langfuse  # imported lazily; optional dependency path

        _client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=os.getenv("LANGFUSE_HOST", "http://localhost:3000"),
        )
        logger.info("chat_with_data: langfuse tracing enabled")
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: langfuse init failed; tracing disabled")
        _client = None
    return _client


def _clip(value: Any) -> str:
    text = extract_text(value) if not isinstance(value, str) else value
    if not text:
        text = str(value)
    return text[:MAX_IO_CHARS]


class LangfuseTurnHandler(BaseCallbackHandler):
    """Maps LangChain callbacks for ONE turn onto a Langfuse trace.

    Flat structure: the trace is the turn; each model call is a generation,
    each tool call a span. The langfuse client batches sends on a background
    thread, so nothing here blocks the event loop.
    """

    raise_error = False  # never let tracing errors propagate into the turn

    def __init__(self, trace, model_name: str):
        self._trace = trace
        self._model_name = model_name
        self._observations: dict[str, Any] = {}  # run_id -> generation/span

    # ── model calls ─────────────────────────────────────────────────────────

    def on_chat_model_start(self, serialized, messages, *, run_id, **kwargs):
        try:
            last = messages[0][-1] if messages and messages[0] else None
            self._observations[str(run_id)] = self._trace.generation(
                name="model_call",
                model=self._model_name,
                input=_clip(last.content) if last is not None else None,
                metadata={"message_count": len(messages[0]) if messages else 0},
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_chat_model_start failed")

    def on_llm_end(self, response, *, run_id, **kwargs):
        try:
            generation = self._observations.pop(str(run_id), None)
            if generation is None:
                return
            usage = None
            output = None
            generations = getattr(response, "generations", None)
            if generations and generations[0]:
                message = getattr(generations[0][0], "message", None)
                if message is not None:
                    output = _clip(message.content)
                    meta = getattr(message, "usage_metadata", None) or {}
                    if meta:
                        usage = {
                            "input": meta.get("input_tokens", 0),
                            "output": meta.get("output_tokens", 0),
                        }
            generation.end(output=output, usage=usage)
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_llm_end failed")

    def on_llm_error(self, error, *, run_id, **kwargs):
        try:
            generation = self._observations.pop(str(run_id), None)
            if generation is not None:
                generation.end(level="ERROR", status_message=str(error)[:500])
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_llm_error failed")

    # ── tool calls ──────────────────────────────────────────────────────────

    def on_tool_start(self, serialized, input_str, *, run_id, **kwargs):
        try:
            self._observations[str(run_id)] = self._trace.span(
                name=(serialized or {}).get("name", "tool"),
                input=_clip(input_str),
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_tool_start failed")

    def on_tool_end(self, output, *, run_id, **kwargs):
        try:
            span = self._observations.pop(str(run_id), None)
            if span is not None:
                span.end(output=_clip(output))
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_tool_end failed")

    def on_tool_error(self, error, *, run_id, **kwargs):
        try:
            span = self._observations.pop(str(run_id), None)
            if span is not None:
                span.end(level="ERROR", status_message=str(error)[:500])
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_tool_error failed")

    # ── turn lifecycle ──────────────────────────────────────────────────────

    def score(self, name: str, value: float, comment: str | None = None):
        """Attach an evaluation score to this turn's trace (e.g. the result
        validator's verdict) — the input to Langfuse's evaluation dashboards."""
        try:
            self._trace.score(name=name, value=value, comment=comment)
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse score failed")

    def finish(self, output: str, status: str):
        try:
            self._trace.update(output=_clip(output), metadata={"status": status})
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse trace finish failed")


def start_turn_trace(
    *, session, orguser, context, question: str, request_uuid, model_name: str
) -> LangfuseTurnHandler | None:
    """One trace per turn, or None when tracing is off. IDs are opaque internal
    ids — never emails or names (same rule as our analytics)."""
    client = get_langfuse()
    if client is None:
        return None
    try:
        trace = client.trace(
            # deterministic id: the feedback endpoint and the eval runner
            # address the trace by request_uuid without storing a second id
            id=str(request_uuid),
            name="chat_with_data_turn",
            session_id=str(session.id),
            user_id=str(orguser.id),
            tags=[context.org_slug, context.dialect],
            input=_clip(question),
            metadata={"request_uuid": str(request_uuid)},
        )
        return LangfuseTurnHandler(trace, model_name=model_name)
    except Exception:  # pylint: disable=broad-except
        logger.exception("langfuse trace start failed")
        return None
