"""Eval runner tests — scripted model, fake warehouse, fake Langfuse. No cost."""

import asyncio
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

import pytest
from langchain_core.messages import AIMessage

from ddpui.core.ai.evals import runner as eval_runner
from ddpui.core.ai.llm_calls.router import RouteResult
from ddpui.tests.core.ai.test_agent_loop import ScriptedChatModel, make_context, sql_call
from ddpui.tests.core.ai.test_tools import FakeWarehouse


@pytest.fixture(autouse=True)
def routed_as(monkeypatch):
    """Patch the router (network) with a controllable stub; default data_question."""
    state = {"intent": "data_question"}

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent=state["intent"])

    async def fake_casual(question, model=None):
        return "Happy to help!"

    monkeypatch.setattr(eval_runner, "route_question", fake_route)
    monkeypatch.setattr(eval_runner, "casual_reply", fake_casual)
    return state


def run(item, *, script, warehouse=None, intent=None, routed=None):
    if routed is not None and intent is not None:
        routed["intent"] = intent
    model = ScriptedChatModel(script=script)
    context = make_context(warehouse or FakeWarehouse(rows=[{"n": 171}]))
    return asyncio.run(eval_runner.run_item(item, context=context, model=model, judge=False))


def test_gold_sql_match_passes(routed_as):
    item = {
        "question": "how many beneficiaries enrolled?",
        "expected_intent": "data_question",
        "gold_sql": "SELECT COUNT(DISTINCT beneficiary_id) AS n FROM prod.enrollments",
    }
    result = run(
        item,
        script=[
            sql_call("SELECT COUNT(DISTINCT beneficiary_id) AS n FROM prod.enrollments", "c1"),
            AIMessage(content="**171** beneficiaries are enrolled."),
        ],
    )
    assert result.routing_ok is True
    assert result.sql_ok is True
    assert result.hard_pass


def test_wrong_result_set_fails_sql_metric(routed_as):
    class SplitWarehouse(FakeWarehouse):
        """Gold SQL sees the right answer; the agent's row-count query doesn't."""

        def execute(self, sql):
            if "DISTINCT" in sql:
                return [{"n": 171}]
            if "pg_catalog.pg_class" in sql:
                return self.catalog_rows
            return [{"n": 300}]  # the grain trap: counting enrollment rows

    item = {
        "question": "how many beneficiaries enrolled?",
        "expected_intent": "data_question",
        "gold_sql": "SELECT COUNT(DISTINCT beneficiary_id) AS n FROM prod.enrollments",
    }
    result = run(
        item,
        warehouse=SplitWarehouse(),
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.enrollments", "c1"),
            AIMessage(content="**300** beneficiaries are enrolled."),
        ],
    )
    assert result.sql_ok is False
    assert not result.hard_pass


def test_expected_value_fallback(routed_as):
    item = {"question": "total donations?", "expected_value": "14909222"}
    result = run(
        item,
        script=[
            sql_call("SELECT SUM(amount) AS total FROM prod.donations", "c1"),
            AIMessage(content="You have received **₹1,49,09,222** in donations."),
        ],
    )
    assert result.sql_ok is True


def test_routing_mismatch_fails(routed_as):
    item = {"question": "compare them", "expected_intent": "needs_clarification"}
    result = run(item, script=[AIMessage(content="ok")], intent="data_question", routed=routed_as)
    assert result.routing_ok is False
    assert not result.hard_pass


def test_small_talk_routes_and_passes(routed_as):
    item = {"question": "hello!", "expected_intent": "small_talk"}
    result = run(item, script=[AIMessage(content="unused")], intent="small_talk", routed=routed_as)
    assert result.routing_ok is True
    assert result.sql_ok is None  # nothing to compare — hard_pass still true
    assert result.hard_pass


def test_run_items_pushes_scores_and_links(routed_as):
    class FakeDatasetItem:
        def __init__(self, question):
            self.input = {"question": question}
            self.linked = None

        def link(self, trace, run_name):
            self.linked = run_name

    class FakeTrace:
        def __init__(self):
            self.scores = {}

        def score(self, name, value, **kwargs):
            self.scores[name] = value

    class FakeLangfuse:
        def __init__(self):
            self.traces = []

        def trace(self, **kwargs):
            trace = FakeTrace()
            self.traces.append((kwargs, trace))
            return trace

        def flush(self):
            pass

    client = FakeLangfuse()
    ds_item = FakeDatasetItem("how many beneficiaries enrolled?")
    dataset = type("DS", (), {"items": [ds_item]})()

    items = [
        {
            "question": "how many beneficiaries enrolled?",
            "expected_intent": "data_question",
            "gold_sql": "SELECT 1",
        }
    ]
    model = ScriptedChatModel(
        script=[sql_call("SELECT 1", "c1"), AIMessage(content="**171** enrolled.")]
    )
    context = make_context(FakeWarehouse(rows=[{"n": 171}]))

    summary = asyncio.run(
        eval_runner.run_items(
            items,
            context=context,
            run_name="test-run",
            model=model,
            judge=False,
            langfuse_client=client,
            dataset=dataset,
        )
    )

    assert summary.passed == 1
    kwargs, trace = client.traces[0]
    assert "eval" in kwargs["tags"]
    assert trace.scores == {"eval_routing": 1, "eval_sql_correct": 1}
    assert ds_item.linked == "test-run"
    assert "1/1 hard-metric pass" in summary.render()
