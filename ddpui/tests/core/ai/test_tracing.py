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
    """Like the SDK's stateful observation: children can be created on it."""

    def __init__(self, kind, kwargs, registry, parent=None):
        self.kind = kind
        self.kwargs = kwargs
        self.parent = parent
        self.ended_with = None
        self._registry = registry

    def end(self, **kwargs):
        self.ended_with = kwargs

    def generation(self, **kwargs):
        obs = StubObservation("generation", kwargs, self._registry, parent=self)
        self._registry.append(obs)
        return obs

    def span(self, **kwargs):
        obs = StubObservation("span", kwargs, self._registry, parent=self)
        self._registry.append(obs)
        return obs


class StubTrace:
    def __init__(self):
        self.observations = []
        self.updated_with = None

    def generation(self, **kwargs):
        obs = StubObservation("generation", kwargs, self.observations, parent=None)
        self.observations.append(obs)
        return obs

    def span(self, **kwargs):
        obs = StubObservation("span", kwargs, self.observations, parent=None)
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
    # no langgraph node metadata → fallback name, agent-default model
    assert generation.kwargs["name"] == "llm-call"
    assert generation.kwargs["model"] == "claude-sonnet-5"
    assert generation.kwargs["input"] == "how many surveys?"
    assert generation.ended_with["output"] == "1,284 surveys."
    assert generation.ended_with["usage"] == {"input": 900, "output": 40}

    assert span.kind == "span"
    assert span.kwargs["name"] == "execute_sql"
    assert span.ended_with["output"] == "Query returned 1 rows."

    assert trace.updated_with["output"] == "1,284 surveys."
    assert trace.updated_with["metadata"]["status"] == "completed"


def test_handler_nests_observations_under_stage_spans():
    """The trace must mirror the TurnGraph: stage spans with the stage's LLM
    calls and tool calls nested inside, each generation named for its node and
    attributed to the model that actually served it."""
    trace = StubTrace()
    handler = LangfuseTurnHandler(trace, model_name="claude-sonnet-5")

    # route stage: a chain run for the node, an LLM call inside it
    route_run, route_llm = uuid.uuid4(), uuid.uuid4()
    handler.on_chain_start(
        {"name": "route_node"}, {}, run_id=route_run,
        metadata={"langgraph_node": "route_node"},
    )
    handler.on_chat_model_start(
        {}, [[HumanMessage("how many surveys?")]],
        run_id=route_llm, parent_run_id=route_run,
        metadata={"langgraph_node": "route_node"},
        invocation_params={"model": "claude-haiku-4-5"},
    )
    handler.on_chain_end({}, run_id=route_run)

    # agent stage: LLM + tool nested below the sql_agent span, one level apart
    agent_run, inner_run, agent_llm, tool_run = (uuid.uuid4() for _ in range(4))
    handler.on_chain_start(
        {"name": "sql_agent"}, {}, run_id=agent_run,
        metadata={"langgraph_node": "sql_agent"},
    )
    # LangGraph internal wrapper run: same node metadata, different name — no span,
    # but children must still resolve through it to the stage span
    handler.on_chain_start(
        {"name": "LangGraph"}, {}, run_id=inner_run, parent_run_id=agent_run,
        metadata={"langgraph_node": "sql_agent"},
    )
    handler.on_chat_model_start(
        {}, [[HumanMessage("q")]], run_id=agent_llm, parent_run_id=inner_run,
        metadata={"langgraph_node": "model"},
        invocation_params={"model": "claude-sonnet-5"},
    )
    handler.on_tool_start({"name": "execute_sql"}, "SELECT 1", run_id=tool_run, parent_run_id=inner_run)

    route_span, route_gen, agent_span, agent_gen, tool_span = trace.observations
    assert (route_span.kind, route_span.kwargs["name"], route_span.parent) == ("span", "route-question", None)
    assert route_span.ended_with == {}  # ended by on_chain_end
    assert route_gen.kwargs["name"] == "classify-intent"
    assert route_gen.kwargs["model"] == "claude-haiku-4-5"  # not the agent default
    assert route_gen.parent is route_span

    assert agent_span.kwargs["name"] == "run-sql-agent"
    assert agent_gen.kwargs["name"] == "generate-response"
    assert agent_gen.parent is agent_span  # resolved through the wrapper run
    assert tool_span.kwargs["name"] == "execute_sql"
    assert tool_span.parent is agent_span


def test_dispatcher_routes_model_events_to_context_bound_handler():
    """On Python 3.10 model calls never see config callbacks; the model-attached
    dispatcher must forward LLM events to the turn handler bound to the context,
    and the generation must nest under the currently open stage span."""
    trace = StubTrace()
    handler = LangfuseTurnHandler(trace, model_name="claude-sonnet-5")
    dispatcher = observability.TurnCallbackDispatcher()

    # sql_agent stage opens (config-propagated chain event, as in production)
    agent_run = uuid.uuid4()
    handler.on_chain_start(
        {"name": "sql_agent"}, {}, run_id=agent_run, metadata={"langgraph_node": "sql_agent"}
    )

    token = observability.set_current_turn_handler(handler)
    try:
        # model call arrives via the dispatcher: no parent link, no metadata
        llm_run = uuid.uuid4()
        dispatcher.on_chat_model_start(
            {}, [[HumanMessage("q")]], run_id=llm_run,
            invocation_params={"model": "claude-sonnet-5"},
        )
        message = AIMessage(content="answer")
        message.usage_metadata = {"input_tokens": 10, "output_tokens": 5}
        dispatcher.on_llm_end(LLMResult(generations=[[ChatGeneration(message=message)]]), run_id=llm_run)
    finally:
        observability.reset_current_turn_handler(token)

    stage_span, generation = trace.observations
    assert generation.kwargs["name"] == "generate-response"  # named from the open stage
    assert generation.parent is stage_span
    assert generation.ended_with["usage"] == {"input": 10, "output": 5}

    # outside a bound context the dispatcher is inert
    dispatcher.on_chat_model_start({}, [[HumanMessage("q")]], run_id=uuid.uuid4())
    assert len(trace.observations) == 2


def test_stage_stack_pops_when_stage_ends():
    """After a stage closes, later unparented model calls must not nest under it."""
    trace = StubTrace()
    handler = LangfuseTurnHandler(trace, model_name="m")

    route_run = uuid.uuid4()
    handler.on_chain_start(
        {"name": "route_node"}, {}, run_id=route_run, metadata={"langgraph_node": "route_node"}
    )
    handler.on_chain_end({}, run_id=route_run)

    llm_run = uuid.uuid4()
    handler.on_chat_model_start({}, [[HumanMessage("q")]], run_id=llm_run)
    route_span, generation = trace.observations
    assert generation.parent is None  # trace root, not the closed route span
    assert generation.kwargs["name"] == "llm-call"


def test_tool_output_masked_when_env_set(monkeypatch):
    """Tool outputs can carry warehouse rows (beneficiary data); the masking
    flag must strip them from traces while keeping the span itself."""
    monkeypatch.setenv("LANGFUSE_MASK_TOOL_RESULTS", "true")
    trace = StubTrace()
    handler = LangfuseTurnHandler(trace, model_name="m")

    tool_run = uuid.uuid4()
    handler.on_tool_start({"name": "execute_sql"}, "SELECT name FROM beneficiaries", run_id=tool_run)
    handler.on_tool_end("Anjali,9876543210\nPriya,9123456789", run_id=tool_run)

    (span,) = trace.observations
    assert span.kwargs["input"] == "SELECT name FROM beneficiaries"  # SQL kept
    assert "Anjali" not in span.ended_with["output"]
    assert span.ended_with["output"].startswith("[masked:")


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
