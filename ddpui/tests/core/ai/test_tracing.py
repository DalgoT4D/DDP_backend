"""Langfuse tracing tests — stubbed client, no server or keys needed."""

import os
import uuid

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult

from ddpui.core.ai import tracing as observability
from ddpui.core.ai.tracing import LangfuseTurnHandler


class StubObservation:
    def __init__(self, kind, kwargs):
        self.kind = kind
        self.kwargs = kwargs
        self.ended_with = None

    def end(self, **kwargs):
        self.ended_with = kwargs


class StubTrace:
    def __init__(self):
        self.observations = []
        self.updated_with = None

    def generation(self, **kwargs):
        obs = StubObservation("generation", kwargs)
        self.observations.append(obs)
        return obs

    def span(self, **kwargs):
        obs = StubObservation("span", kwargs)
        self.observations.append(obs)
        return obs

    def update(self, **kwargs):
        self.updated_with = kwargs


def test_tracing_disabled_without_keys(monkeypatch):
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    monkeypatch.setattr(observability, "_client", None)
    monkeypatch.setattr(observability, "_client_initialized", False)
    assert observability.get_langfuse() is None


def test_handler_maps_model_and_tool_events_to_trace():
    trace = StubTrace()
    handler = LangfuseTurnHandler(trace, model_name="claude-sonnet-5")

    model_run = uuid.uuid4()
    handler.on_chat_model_start({}, [[HumanMessage("how many surveys?")]], run_id=model_run)
    message = AIMessage(content="1,284 surveys.")
    message.usage_metadata = {"input_tokens": 900, "output_tokens": 40, "total_tokens": 940}
    handler.on_llm_end(LLMResult(generations=[[ChatGeneration(message=message)]]), run_id=model_run)

    tool_run = uuid.uuid4()
    handler.on_tool_start({"name": "execute_sql"}, "SELECT COUNT(*)...", run_id=tool_run)
    handler.on_tool_end("Query returned 1 rows.", run_id=tool_run)

    handler.finish(output="1,284 surveys.", status="completed")

    generation, span = trace.observations
    assert generation.kind == "generation"
    assert generation.kwargs["model"] == "claude-sonnet-5"
    assert generation.kwargs["input"] == "how many surveys?"
    assert generation.ended_with["output"] == "1,284 surveys."
    assert generation.ended_with["usage"] == {"input": 900, "output": 40}

    assert span.kind == "span"
    assert span.kwargs["name"] == "execute_sql"
    assert span.ended_with["output"] == "Query returned 1 rows."

    assert trace.updated_with["output"] == "1,284 surveys."
    assert trace.updated_with["metadata"]["status"] == "completed"


def test_handler_never_raises_on_malformed_events():
    handler = LangfuseTurnHandler(StubTrace(), model_name="m")
    handler.on_llm_end(LLMResult(generations=[]), run_id=uuid.uuid4())  # unknown run
    handler.on_tool_end("out", run_id=uuid.uuid4())  # unknown run
    handler.on_tool_error(RuntimeError("x"), run_id=uuid.uuid4())  # unknown run


def test_trace_id_is_the_request_uuid(monkeypatch):
    """The trace id must be deterministic (= request_uuid) so later actors —
    the feedback endpoint, the eval runner's item.link() — can address the
    trace without storing a separate id."""
    from types import SimpleNamespace

    captured = {}

    class StubClient:
        def trace(self, **kwargs):
            captured.update(kwargs)
            return StubTrace()

    monkeypatch.setattr(observability, "_client", StubClient())
    monkeypatch.setattr(observability, "_client_initialized", True)

    request_uuid = uuid.uuid4()
    handler = observability.start_turn_trace(
        session=SimpleNamespace(id=7, scope_type="dashboard", scope_id=42),
        orguser=SimpleNamespace(id=3),
        context=SimpleNamespace(org_slug="ngo", dialect="postgres"),
        question="how many surveys?",
        request_uuid=request_uuid,
        model_name="claude-sonnet-5",
    )

    assert handler is not None
    assert captured["id"] == str(request_uuid)
    # dashboard-scoped chats must be separable from org-wide chats in Langfuse
    assert "scope:dashboard" in captured["tags"]
    assert captured["metadata"]["scope_type"] == "dashboard"
    assert captured["metadata"]["scope_id"] == 42


def test_record_generation_maps_one_shot_call_to_trace(monkeypatch):
    """Single-call features (report summary) record a trace + one generation."""
    from types import SimpleNamespace

    captured = {}

    class StubClient:
        def trace(self, **kwargs):
            captured.update(kwargs)
            captured["_trace"] = StubTrace()
            return captured["_trace"]

    monkeypatch.setattr(observability, "_client", StubClient())
    monkeypatch.setattr(observability, "_client_initialized", True)

    orguser = SimpleNamespace(id=3, org=SimpleNamespace(slug="ngo"))
    observability.record_generation(
        name="report_summary",
        orguser=orguser,
        model_name="claude-sonnet-5",
        input_text='Report "Q1 Field Report" (2026-01-01 to 2026-03-31)',
        output_text="**Great quarter.**",
        latency_ms=1234,
        status="completed",
        usage={"input": 900, "output": 40},
        metadata={"snapshot_id": 11},
    )

    assert captured["name"] == "report_summary"
    assert captured["user_id"] == "3"
    assert captured["tags"] == ["ngo"]
    assert captured["metadata"]["status"] == "completed"
    assert captured["metadata"]["snapshot_id"] == 11
    (generation,) = captured["_trace"].observations
    assert generation.kwargs["model"] == "claude-sonnet-5"
    assert generation.ended_with["output"] == "**Great quarter.**"
    assert generation.ended_with["usage"] == {"input": 900, "output": 40}


def test_record_generation_marks_failures_as_errors(monkeypatch):
    from types import SimpleNamespace

    captured = {}

    class StubClient:
        def trace(self, **kwargs):
            captured["_trace"] = StubTrace()
            return captured["_trace"]

    monkeypatch.setattr(observability, "_client", StubClient())
    monkeypatch.setattr(observability, "_client_initialized", True)

    observability.record_generation(
        name="report_summary",
        orguser=SimpleNamespace(id=3, org=SimpleNamespace(slug="ngo")),
        model_name="claude-sonnet-5",
        input_text="Report ...",
        output_text=None,
        latency_ms=88,
        status="failed",
        error="model overloaded",
    )

    (generation,) = captured["_trace"].observations
    assert generation.ended_with["level"] == "ERROR"
    assert generation.ended_with["status_message"] == "model overloaded"
