"""Reflection (pre-execution SQL critique, complex lane only) tests."""

import json
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage

from ddpui.core.ai.llm_calls import sql_reflection as reflection
from ddpui.core.ai.tools import sql_tools
from ddpui.tests.core.ai.test_agent_loop import make_context
from ddpui.tests.core.ai.test_tools import FakeWarehouse


class FakeModel:
    def __init__(self, content):
        self._content = content

    def invoke(self, prompt):
        return AIMessage(content=self._content)


def test_find_sql_issue_flags_an_issue():
    model = FakeModel(json.dumps({"ok": False, "issue": "the join duplicates farmers"}))
    issue = reflection.find_sql_issue("q", "SELECT ...", "postgres", model=model)
    assert issue == "the join duplicates farmers"


def test_find_sql_issue_passes_clean_sql_and_fails_open():
    assert (
        reflection.find_sql_issue("q", "SELECT 1", "postgres", model=FakeModel('{"ok": true}'))
        is None
    )
    assert (
        reflection.find_sql_issue("q", "SELECT 1", "postgres", model=FakeModel("garbage")) is None
    )


def run_execute_sql(ctx, sql):
    return sql_tools.execute_sql.func(sql=sql, runtime=type("R", (), {"context": ctx})())


def test_reflection_gates_complex_lane_only(monkeypatch):
    calls = []

    def fake_check(question, sql, dialect, model=None):
        calls.append(sql)
        return "the join duplicates farmers"

    monkeypatch.setattr(sql_tools, "find_sql_issue", fake_check)

    # simple lane: reflection never runs, query executes
    ctx = make_context(FakeWarehouse(rows=[{"n": 1}]))
    ctx.complexity = "simple"
    content, artifact = run_execute_sql(ctx, "SELECT COUNT(*) AS n FROM prod.surveys")
    assert artifact["status"] == "success"
    assert calls == []

    # complex lane: reflection blocks execution and feeds the issue back
    class MustNotExecute(FakeWarehouse):
        def execute(self, sql):
            raise AssertionError("flawed SQL must not reach the warehouse")

    ctx = make_context(MustNotExecute())
    ctx.complexity = "complex"
    content, artifact = run_execute_sql(ctx, "SELECT COUNT(*) AS n FROM prod.surveys")
    assert artifact["status"] == "rejected"
    assert content.startswith("SQL rejected:")  # counts toward the 3-attempt limiter
    assert "duplicates farmers" in content
    # reflection reviews the GUARDED sql — what will actually run (LIMIT injected)
    assert len(calls) == 1
    assert calls[0].startswith("SELECT COUNT(*) AS n FROM prod.surveys")
    assert "LIMIT 100" in calls[0]
