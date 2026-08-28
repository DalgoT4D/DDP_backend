"""Platform guide agent tests: routing, docs tool, inventory scoping, creation
tools, and the HITL gate on creation.

Same conventions as the sibling test files: scripted models drive real graphs;
ORM-touching tests use @pytest.mark.django_db; services are monkeypatched at
their import site inside the tool functions.
"""

import os
from unittest import mock

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from langchain_core.messages import AIMessage

from ddpui.core.ai.agent.platform_guide_agent import (
    GUIDE_AGENT_TOOLS,
    build_guide_agent,
    build_guide_system_prompt,
)
from ddpui.core.ai.agent.chat_data_agent import SQL_AGENT_TOOLS, build_agent
from ddpui.core.ai.tools import docs_tools
from ddpui.core.ai.tools.registry import get_tools
from ddpui.tests.core.ai.test_agent_loop import ScriptedChatModel, make_context


def run_tool(tool, ctx, **kwargs):
    return tool.func(runtime=type("R", (), {"context": ctx})(), **kwargs)


# ── tool split ───────────────────────────────────────────────────────────────


def test_sql_agent_no_longer_carries_creation_tools():
    creation = {"create_chart", "create_dashboard", "add_charts_to_dashboard"}
    assert creation.isdisjoint(SQL_AGENT_TOOLS)
    assert creation.issubset(GUIDE_AGENT_TOOLS)


def test_both_agents_build_with_their_subsets():
    sql_agent = build_agent(model=ScriptedChatModel(script=[]), human_in_the_loop=False)
    guide_agent = build_guide_agent(model=ScriptedChatModel(script=[]), human_in_the_loop=False)
    assert sql_agent is not None and guide_agent is not None
    # a typo'd name fails loudly at build, not silently at runtime
    with pytest.raises(KeyError):
        get_tools(names=("execute_sql", "not_a_tool"))


def test_guide_prompt_teaches_dependencies_and_docs():
    prompt = build_guide_system_prompt(make_context())
    assert "KPI ALWAYS needs a metric first" in prompt
    assert "get_dalgo_help" in prompt
    assert "Read more" in prompt


# ── docs tool ────────────────────────────────────────────────────────────────


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value


@pytest.fixture
def fake_redis(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(docs_tools.RedisClient, "get_instance", staticmethod(lambda: redis))
    return redis


def _fake_response(html: str):
    response = mock.Mock()
    response.text = html
    response.raise_for_status = lambda: None
    return response


def test_get_dalgo_help_fetches_page_and_appends_url(monkeypatch, fake_redis):
    html = "<html><nav>menu</nav><main><h1>Creating a chart</h1><p>Select Charts.</p></main></html>"
    monkeypatch.setattr(docs_tools.requests, "get", lambda url, timeout: _fake_response(html))

    out = docs_tools.get_dalgo_help.func(topic="creating_a_chart")

    assert "Select Charts." in out
    assert "menu" not in out  # nav stripped, main content only
    assert "Read more: https://docs.dalgo.org/charts/creating-a-chart" in out


def test_get_dalgo_help_serves_from_cache_without_fetching(monkeypatch, fake_redis):
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        return _fake_response("<main>fresh text</main>")

    monkeypatch.setattr(docs_tools.requests, "get", fetch)

    docs_tools.get_dalgo_help.func(topic="metrics")
    docs_tools.get_dalgo_help.func(topic="metrics")

    assert len(calls) == 1  # second call came from Redis


def test_get_dalgo_help_degrades_on_fetch_failure(monkeypatch, fake_redis):
    def explode(url, timeout):
        raise ConnectionError("docs site down")

    monkeypatch.setattr(docs_tools.requests, "get", explode)

    out = docs_tools.get_dalgo_help.func(topic="creating_a_kpi")

    assert "Couldn't reach the Dalgo guide" in out
    assert "https://docs.dalgo.org/kpis/creating-a-kpi" in out


def test_get_dalgo_help_rejects_unknown_topic_with_the_valid_list(fake_redis):
    out = docs_tools.get_dalgo_help.func(topic="quantum_flux")
    assert "Unknown topic" in out and "creating_a_chart" in out


# ── inventory tools: org scoping ─────────────────────────────────────────────


@pytest.mark.django_db
def test_inventory_tools_list_only_own_org(seed_org_with_objects):
    from ddpui.core.ai.tools import guide_tools

    ctx, other_org_metric_name = seed_org_with_objects

    out = run_tool(guide_tools.list_metrics, ctx)
    assert "Survey count" in out
    assert other_org_metric_name not in out

    kpis = run_tool(guide_tools.list_kpis, ctx)
    assert "Survey KPI" in kpis and "Survey count" in kpis

    charts = run_tool(guide_tools.list_charts, ctx)
    assert "none yet" in charts  # this org has no charts


@pytest.fixture
def seed_org_with_objects():
    from django.contrib.auth.models import User

    from ddpui.models.metric import KPI, Metric
    from ddpui.models.org import Org
    from ddpui.models.org_user import OrgUser

    org = Org.objects.create(name="Guide Org", slug="guide-org")
    other = Org.objects.create(name="Other Org", slug="other-org")
    user = User.objects.create(username="guideuser", email="g@t.co", password="x")
    orguser = OrgUser.objects.create(user=user, org=org)

    metric = Metric.objects.create(
        name="Survey count",
        schema_name="prod",
        table_name="surveys",
        column="id",
        aggregation="count",
        org=org,
        created_by=orguser,
    )
    KPI.objects.create(
        metric=metric,
        name="Survey KPI",
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    Metric.objects.create(
        name="Secret other-org metric",
        schema_name="prod",
        table_name="x",
        column="id",
        aggregation="count",
        org=other,
        created_by=None,
    )

    ctx = make_context()
    ctx.org_id = org.id
    ctx.orguser_id = orguser.id
    yield ctx, "Secret other-org metric"
    KPI.objects.all().delete()
    Metric.objects.all().delete()
    orguser.delete()
    user.delete()
    org.delete()
    other.delete()


# ── creation tools: permissions + service delegation ─────────────────────────


def test_create_metric_requires_permission():
    from ddpui.core.ai.tools import metric_tools

    ctx = make_context()
    ctx.can_create_metrics = False
    content, artifact = run_tool(
        metric_tools.create_metric,
        ctx,
        name="m",
        schema_name="prod",
        table_name="surveys",
        column="id",
        aggregation="count",
    )
    assert artifact["status"] == "rejected"
    assert "permission" in content


def test_create_kpi_validates_enums_before_touching_services():
    from ddpui.core.ai.tools import metric_tools

    ctx = make_context()
    ctx.can_create_kpis = True
    _, artifact = run_tool(
        metric_tools.create_kpi, ctx, metric_id=1, direction="sideways", time_grain="monthly"
    )
    assert artifact["status"] == "rejected"


def test_create_report_requires_permission_and_valid_dates():
    from ddpui.core.ai.tools import report_tools

    ctx = make_context()
    ctx.can_create_dashboards = False
    _, artifact = run_tool(report_tools.create_report, ctx, title="Q1", dashboard_id=1)
    assert artifact["status"] == "rejected"

    ctx.can_create_dashboards = True
    _, artifact = run_tool(
        report_tools.create_report, ctx, title="Q1", dashboard_id=1, period_start="not-a-date"
    )
    assert artifact["status"] == "rejected"


# ── HITL: creation on the guide agent pauses for approval ────────────────────


def test_guide_agent_pauses_on_create_metric_for_approval():
    from langgraph.checkpoint.memory import InMemorySaver

    model = ScriptedChatModel(
        script=[
            AIMessage(
                "",
                tool_calls=[
                    {
                        "name": "create_metric",
                        "args": {
                            "name": "Total surveys",
                            "schema_name": "prod",
                            "table_name": "surveys",
                            "column": "id",
                            "aggregation": "count",
                        },
                        "id": "m1",
                    }
                ],
            ),
            AIMessage(content="Created."),
        ]
    )
    agent = build_guide_agent(checkpointer=InMemorySaver(), model=model, human_in_the_loop=True)
    ctx = make_context()
    ctx.can_create_metrics = True

    result = agent.invoke(
        {"messages": [("user", "create a metric for total surveys")]},
        config={"configurable": {"thread_id": "t1"}},
        context=ctx,
    )

    interrupts = result.get("__interrupt__")
    assert interrupts, "creation must pause for approval"
    tools_pending = [r["name"] for r in interrupts[0].value["action_requests"]]
    assert tools_pending == ["create_metric"]
