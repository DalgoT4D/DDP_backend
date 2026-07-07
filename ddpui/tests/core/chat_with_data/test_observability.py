"""Langfuse tracing tests — stubbed client, no server or keys needed."""

import os
import uuid

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult

from ddpui.core.chat_with_data import observability
from ddpui.core.chat_with_data.observability import LangfuseTurnHandler


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
