"""Tests for dashboard scope resolution (scope.py).

resolve_scope turns (org, scope_type, scope_id) into the table allowlist and
prompt context block for a scoped chat session. Pure ORM — no LLM, no warehouse.
"""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from django.contrib.auth.models import User

from ddpui.core.ai.scopes.base import ScopeUnavailable
from ddpui.core.ai.scopes.resolver import resolve_scope
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(name="Scope Test Org", slug="scope-test")
    yield org
    org.delete()


@pytest.fixture
def orguser(org):
    user = User.objects.create(username="scopeuser", email="scopeuser@test.com", password="x")
    ou = OrgUser.objects.create(user=user, org=org)
    yield ou
    # KPI.metric is on_delete=PROTECT — clear KPIs before the cascades run
    KPI.objects.filter(org=org).delete()
    Metric.objects.filter(org=org).delete()
    ou.delete()
    user.delete()


def make_dashboard(org, orguser, components):
    return Dashboard.objects.create(
        title="Field Performance",
        org=org,
        created_by=orguser,
        tabs=[{"id": "t1", "title": "Main", "layout_config": {}, "components": components}],
    )


def make_chart(org, orguser, title, schema_name, table_name):
    return Chart.objects.create(
        title=title,
        chart_type="bar",
        computation_type="aggregated",
        schema_name=schema_name,
        table_name=table_name,
        org=org,
        created_by=orguser,
    )


def test_chart_tables_are_collected(org, orguser):
    surveys = make_chart(org, orguser, "Surveys by district", "prod", "surveys")
    visits = make_chart(org, orguser, "Visits over time", "prod", "field_visits")
    dashboard = make_dashboard(
        org,
        orguser,
        {
            "c1": {"type": "chart", "config": {"chartId": surveys.id}},
            "c2": {"type": "chart", "config": {"chartId": visits.id}},
        },
    )

    scope = resolve_scope(org, "dashboard", dashboard.id)

    assert scope.allowed_tables == ["prod.field_visits", "prod.surveys"]
    assert scope.scope_type == "dashboard"


def test_kpi_tables_come_via_the_metric(org, orguser):
    # KPI rows have no table fields — the table lives on the underlying Metric
    metric = Metric.objects.create(
        name="Total donations",
        schema_name="prod",
        table_name="donations",
        column="amount",
        aggregation="sum",
        org=org,
        created_by=orguser,
    )
    kpi = KPI.objects.create(
        name="Donations this year",
        metric=metric,
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    dashboard = make_dashboard(org, orguser, {"k1": {"type": "kpi", "config": {"kpiId": kpi.id}}})

    scope = resolve_scope(org, "dashboard", dashboard.id)

    assert scope.allowed_tables == ["prod.donations"]


def test_filter_tables_are_included_and_deduped(org, orguser):
    # a dashboard filter may point at a lookup table no chart uses
    surveys = make_chart(org, orguser, "Surveys by district", "prod", "surveys")
    dashboard = make_dashboard(
        org, orguser, {"c1": {"type": "chart", "config": {"chartId": surveys.id}}}
    )
    DashboardFilter.objects.create(
        dashboard=dashboard,
        name="District",
        filter_type="value",
        schema_name="prod",
        table_name="districts",
        column_name="name",
    )
    # same table as the chart — must not appear twice
    DashboardFilter.objects.create(
        dashboard=dashboard,
        name="Survey date",
        filter_type="datetime",
        schema_name="prod",
        table_name="surveys",
        column_name="surveyed_at",
    )

    scope = resolve_scope(org, "dashboard", dashboard.id)

    assert scope.allowed_tables == ["prod.districts", "prod.surveys"]


def test_org_scope_has_no_restriction(org):
    scope = resolve_scope(org, "org", None)
    assert scope.allowed_tables is None
    assert scope.scope_context == ""


def test_missing_dashboard_raises_friendly_error(org):
    with pytest.raises(ScopeUnavailable, match="no longer exists"):
        resolve_scope(org, "dashboard", 999999)


def test_empty_dashboard_raises_not_empty_allowlist(org, orguser):
    # [] downstream would block everything with a confusing guard message —
    # fail here with a clear one instead
    dashboard = make_dashboard(org, orguser, {})
    with pytest.raises(ScopeUnavailable, match="no charts yet"):
        resolve_scope(org, "dashboard", dashboard.id)


def test_cross_org_dashboard_is_invisible(org, orguser):
    other_org = Org.objects.create(name="Other Org", slug="scope-other")
    surveys = make_chart(org, orguser, "Surveys", "prod", "surveys")
    dashboard = make_dashboard(
        org, orguser, {"c1": {"type": "chart", "config": {"chartId": surveys.id}}}
    )
    try:
        with pytest.raises(ScopeUnavailable, match="no longer exists"):
            resolve_scope(other_org, "dashboard", dashboard.id)
    finally:
        other_org.delete()


def test_scope_context_describes_dashboard_charts_and_filters(org, orguser):
    surveys = make_chart(org, orguser, "Surveys by district", "prod", "surveys")
    dashboard = make_dashboard(
        org, orguser, {"c1": {"type": "chart", "config": {"chartId": surveys.id}}}
    )
    dashboard.description = "Monthly field data quality tracking"
    dashboard.save(update_fields=["description"])
    DashboardFilter.objects.create(
        dashboard=dashboard,
        name="District",
        filter_type="value",
        schema_name="prod",
        table_name="districts",
        column_name="name",
    )

    scope = resolve_scope(org, "dashboard", dashboard.id)

    assert '"Field Performance"' in scope.scope_context
    assert "Monthly field data quality tracking" in scope.scope_context
    assert '"Surveys by district" — bar chart on prod.surveys' in scope.scope_context
    assert "District" in scope.scope_context  # filters give question-interpretation hints
    assert "prod.districts.name" in scope.scope_context
