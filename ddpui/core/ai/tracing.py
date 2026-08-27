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
from contextvars import ContextVar
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler

from ddpui.core.ai.messages.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# Cap trace payload sizes — traces are for debugging, not archival
MAX_IO_CHARS = 4000

# Stage nodes of the TurnGraph → span names (verb-first, stable: these are
# referenced by Langfuse dashboards and filters, so treat them as an API)
STAGE_SPANS = {
    "route_node": "route-question",
    "casual_reply_node": "reply-casually",
    "clarify_node": "ask-clarification",
    "retrieve_context_node": "retrieve-context",
    "sql_agent": "run-sql-agent",
    "validate_node": "validate-answer",
}

# LLM call inside a graph node → generation name; distinguishes the router,
# agent, and validator calls that would otherwise all look identical
GENERATION_NAMES = {
    "route_node": "classify-intent",
    "casual_reply_node": "generate-reply",
    "model": "generate-response",
    "validate_node": "judge-answer",
}
# Same mapping keyed by the enclosing stage span — used when the model call
# arrives without langgraph metadata (see TurnCallbackDispatcher below)
STAGE_GENERATION_NAMES = {
    "route-question": "classify-intent",
    "reply-casually": "generate-reply",
    "run-sql-agent": "generate-response",
    "validate-answer": "judge-answer",
}
FALLBACK_GENERATION_NAME = "llm-call"

# The turn currently being traced in this async context. Set by run_turn before
# streaming the graph; plain ContextVar reads propagate into every child task
# the graph creates, which works on Python 3.10.
_current_turn_handler: ContextVar[Any] = ContextVar("langfuse_turn_handler", default=None)


def set_current_turn_handler(handler) -> Any:
    """Bind a turn's handler to this async context. Returns the reset token."""
    return _current_turn_handler.set(handler)


def reset_current_turn_handler(token) -> None:
    try:
        _current_turn_handler.reset(token)
    except Exception:  # pylint: disable=broad-except
        pass


class TurnCallbackDispatcher(BaseCallbackHandler):
    """Model-attached bridge for Python 3.10.

    LangChain only auto-propagates config callbacks into `model.ainvoke()` on
    Python 3.11+; on 3.10 every model call inside a graph node is invisible to
    the handler passed via config (langchain's own agent factory calls
    `model_.ainvoke(messages)` with no config). Chain and tool callbacks are
    unaffected — LangGraph invokes those with explicit config.

    So the model carries this dispatcher from construction (see base.build_model),
    and it forwards LLM events to whichever turn handler is bound to the current
    async context. Outside a traced turn (REPL, evals, one-shot calls) it no-ops.
    """

    raise_error = False
    run_inline = True  # preserve the caller's contextvar context

    def on_chat_model_start(self, serialized, messages, *, run_id, parent_run_id=None, **kwargs):
        handler = _current_turn_handler.get()
        if handler is not None:
            handler.on_chat_model_start(
                serialized, messages, run_id=run_id, parent_run_id=parent_run_id, **kwargs
            )

    def on_llm_end(self, response, *, run_id, **kwargs):
        handler = _current_turn_handler.get()
        if handler is not None:
            handler.on_llm_end(response, run_id=run_id, **kwargs)

    def on_llm_error(self, error, *, run_id, **kwargs):
        handler = _current_turn_handler.get()
        if handler is not None:
            handler.on_llm_error(error, run_id=run_id, **kwargs)


def _mask_tool_results() -> bool:
    """When set, tool outputs (which can contain warehouse rows, i.e. real
    beneficiary data) are replaced with a length stub in traces. Recommended
    in production; SQL text and tool inputs are still recorded."""
    return os.getenv("LANGFUSE_MASK_TOOL_RESULTS", "").lower() in ("1", "true", "yes")


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
            # ties traces to a deploy (git sha / image tag) for regression hunting
            release=os.getenv("LANGFUSE_RELEASE") or None,
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

    Structure mirrors the TurnGraph: the trace is the turn; each graph stage
    (route / agent / validate) is a span; model calls are generations and tool
    calls are spans nested under their stage. Non-stage chain runs (LangGraph
    internals) get no observation — they'd be noise — but their run ids still
    link children to the right stage. The langfuse client batches sends on a
    background thread, so nothing here blocks the event loop.
    """

    raise_error = False  # never let tracing errors propagate into the turn

    def __init__(self, trace, model_name: str):
        self._trace = trace
        self._model_name = model_name
        self._observations: dict[str, Any] = {}  # run_id -> generation/span
        self._parents: dict[str, str] = {}  # run_id -> parent run_id
        # currently-open stage spans, innermost last — the nesting fallback for
        # model runs that arrive with no parent linkage (see TurnCallbackDispatcher)
        self._stage_stack: list[tuple[str, Any]] = []  # (stage span name, span)

    def _parent_of(self, run_id, parent_run_id):
        """Nearest ancestor run that has an observation; else the innermost open
        stage span (the graph's stages are sequential, so at most one is open);
        else the trace root."""
        self._parents[str(run_id)] = str(parent_run_id) if parent_run_id else ""
        ancestor = str(parent_run_id) if parent_run_id else ""
        while ancestor:
            if ancestor in self._observations:
                return self._observations[ancestor]
            ancestor = self._parents.get(ancestor, "")
        if self._stage_stack:
            return self._stage_stack[-1][1]
        return self._trace

    def _current_stage_name(self) -> str | None:
        return self._stage_stack[-1][0] if self._stage_stack else None

    # ── graph stages ────────────────────────────────────────────────────────

    def on_chain_start(self, serialized, inputs, *, run_id, parent_run_id=None, **kwargs):
        try:
            parent = self._parent_of(run_id, parent_run_id)
            node = (kwargs.get("metadata") or {}).get("langgraph_node")
            run_name = (serialized or {}).get("name") or kwargs.get("name")
            # one span per stage node run; inner runs of the same node carry the
            # same langgraph_node metadata but a different run name — skip them
            if node in STAGE_SPANS and run_name == node:
                span = parent.span(name=STAGE_SPANS[node])
                self._observations[str(run_id)] = span
                self._stage_stack.append((STAGE_SPANS[node], span))
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_chain_start failed")

    def _end_stage(self, run_id, **end_kwargs):
        span = self._observations.pop(str(run_id), None)
        if span is not None:
            span.end(**end_kwargs)
            self._stage_stack = [(name, s) for name, s in self._stage_stack if s is not span]

    def on_chain_end(self, outputs, *, run_id, **kwargs):
        try:
            self._end_stage(run_id)
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_chain_end failed")

    def on_chain_error(self, error, *, run_id, **kwargs):
        try:
            # a human-in-the-loop pause surfaces as a GraphInterrupt "error" in
            # chain callbacks — it is healthy control flow, not a failure, and
            # must not pollute error-rate dashboards and alerts
            if error.__class__.__name__ in ("GraphInterrupt", "NodeInterrupt", "Interrupt"):
                self._end_stage(run_id, status_message="paused: waiting for the user")
            else:
                self._end_stage(run_id, level="ERROR", status_message=str(error)[:500])
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_chain_error failed")

    # ── model calls ─────────────────────────────────────────────────────────

    def on_chat_model_start(self, serialized, messages, *, run_id, parent_run_id=None, **kwargs):
        try:
            if str(run_id) in self._observations:
                return  # already seen via the other delivery path (config vs dispatcher)
            parent = self._parent_of(run_id, parent_run_id)
            # name from langgraph metadata when config propagated; from the
            # enclosing stage span when the call arrived via the dispatcher
            node = (kwargs.get("metadata") or {}).get("langgraph_node")
            name = (
                GENERATION_NAMES.get(node)
                or STAGE_GENERATION_NAMES.get(self._current_stage_name())
                or FALLBACK_GENERATION_NAME
            )
            # each stage may use its own model (router/validator run cheaper
            # ones) — read it from the call, not the agent default, so
            # Langfuse's per-model cost accounting stays correct
            params = kwargs.get("invocation_params") or {}
            model = params.get("model") or params.get("model_name") or self._model_name
            last = messages[0][-1] if messages and messages[0] else None
            self._observations[str(run_id)] = parent.generation(
                name=name,
                model=model,
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

    def on_tool_start(self, serialized, input_str, *, run_id, parent_run_id=None, **kwargs):
        try:
            parent = self._parent_of(run_id, parent_run_id)
            self._observations[str(run_id)] = parent.span(
                name=(serialized or {}).get("name", "tool"),
                input=_clip(input_str),
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("langfuse on_tool_start failed")

    def on_tool_end(self, output, *, run_id, **kwargs):
        try:
            span = self._observations.pop(str(run_id), None)
            if span is not None:
                if _mask_tool_results():
                    text = _clip(output)
                    span.end(output=f"[masked: {len(text)} chars]")
                else:
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
    *,
    session,
    orguser,
    context,
    question: str,
    request_uuid,
    model_name: str,
    trace_id: str | None = None,
    is_resume: bool = False,
) -> LangfuseTurnHandler | None:
    """One trace per QUESTION, or None when tracing is off.

    A paused-then-resumed question stays ONE trace: the resume run passes the
    original run's trace_id and is_resume=True, and the v2 client's upsert
    semantics attach the new spans to the existing trace — without touching
    its name/input (which still show the user's original question).

    IDs are opaque internal ids — never emails or names (same rule as our
    analytics). user_id/session_id carry an org_slug/ prefix so Langfuse's
    Users and Sessions pages group by org at a glance."""
    client = get_langfuse()
    if client is None:
        return None
    try:
        if is_resume and trace_id:
            # attach to the original question's trace; set no fields that
            # would overwrite the original name/input/tags
            trace = client.trace(id=trace_id)
        else:
            trace = client.trace(
                # deterministic id: the feedback endpoint and the eval runner
                # address the trace by request_uuid without storing a second id
                id=trace_id or str(request_uuid),
                # verb-first per Langfuse naming guidance; stable — dashboards,
                # filters, and evaluators key on this name
                name="answer-data-question",
                session_id=f"{context.org_slug}/s{session.id}",
                user_id=f"{context.org_slug}/{orguser.id}",
                tags=[
                    context.org_slug,
                    context.dialect,
                    f"env:{os.getenv('LANGFUSE_ENVIRONMENT', 'dev')}",
                    f"model:{model_name}",
                ],
                input=_clip(question),
                metadata={
                    "request_uuid": str(request_uuid),
                    "org_id": context.org_id,
                    "session_title": session.title,
                },
            )
        return LangfuseTurnHandler(trace, model_name=model_name)
    except Exception:  # pylint: disable=broad-except
        logger.exception("langfuse trace start failed")
        return None


def record_generation(
    *,
    name: str,
    orguser,
    model_name: str,
    input_text: str,
    output_text: str | None,
    latency_ms: int,
    status: str,
    usage: dict | None = None,
    error: str | None = None,
    metadata: dict | None = None,
) -> None:
    """One trace + one generation for single-call AI features (e.g. the report
    summary), recorded after the call finished. Fire-and-forget: never raises,
    no-op when tracing is off. `usage` is Langfuse-shaped: {"input": n, "output": n}."""
    client = get_langfuse()
    if client is None:
        return
    try:
        trace = client.trace(
            name=name,
            user_id=str(orguser.id),
            tags=[orguser.org.slug],
            input=_clip(input_text),
            output=_clip(output_text) if output_text else None,
            metadata={"status": status, "latency_ms": latency_ms, **(metadata or {})},
        )
        generation = trace.generation(name="model_call", model=model_name, input=_clip(input_text))
        if status == "failed":
            generation.end(level="ERROR", status_message=(error or "failed")[:500])
        else:
            generation.end(output=_clip(output_text) if output_text else None, usage=usage)
    except Exception:  # pylint: disable=broad-except
        logger.exception("langfuse record_generation failed")
