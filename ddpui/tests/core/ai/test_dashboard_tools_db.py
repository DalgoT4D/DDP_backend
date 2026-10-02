"""Dashboard tools against the real DB: they must go through DashboardService
so sharing cascade, org-default assignment and the audit log match the
Dashboards API."""

import os
from unittest.mock import patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.auth import ADMIN_ROLE, MEMBER_ROLE
from ddpui.core.ai.tools import dashboard_tools
from ddpui.models.dashboard import Dashboard
from ddpui.models.resource_share import ResourceShare, ResourceType
from ddpui.tests.api_tests.test_access_api import _chart, _make_user, _share_dashboard
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ddpui.tests.core.ai.test_agent_loop import make_context
from ddpui.models.org import Org

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(name="Chat Dash Org", slug="chat-dash-org")
    yield org
    org.delete()


@pytest.fixture
def admin(org, seed_db):
    orguser = _make_user("chatadmin@t.com", org, ADMIN_ROLE)
    yield orguser
    orguser.user.delete()


@pytest.fixture
def member(org, seed_db):
    orguser = _make_user("chatmember@t.com", org, MEMBER_ROLE)
    yield orguser
    orguser.user.delete()


def run_tool(tool, orguser, **kwargs):
    ctx = make_context()
    ctx.org_id = orguser.org.id
    ctx.orguser_id = orguser.id
    return tool.func(runtime=type("R", (), {"context": ctx})(), **kwargs)


def _chart_share_exists(chart, principal) -> bool:
    return ResourceShare.objects.filter(
        resource_type=ResourceType.CHART, resource_id=str(chart.id), principal_id=principal.id
    ).exists()


@patch("ddpui.services.dashboard_service.create_audit_log")
def test_create_dashboard_runs_the_api_side_effects(mock_audit_log, org, admin):
    chart = _chart(org, admin)

    _, artifact = run_tool(
        dashboard_tools.create_dashboard, admin, title="Field Ops", chart_ids=[chart.id]
    )

    dashboard = Dashboard.objects.get(id=artifact["object_id"])
    assert f"chart-{chart.id}" in dashboard.tabs[0]["components"]
    # same default footprint the dashboard builder gives a new chart
    assert {k: dashboard.tabs[0]["layout_config"][0][k] for k in ("w", "h")} == {"w": 4, "h": 18}
    assert dashboard.is_org_default  # admin + org had no default yet
    assert ResourceShare.objects.filter(  # owner's self-share materialised
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_id=admin.id,
        parent__isnull=True,
    ).exists()
    actions = [call.kwargs["action"] for call in mock_audit_log.call_args_list]
    assert actions == ["create", "update"]  # create, then the chart placement


@patch("ddpui.services.dashboard_service.create_audit_log")
def test_charts_added_to_a_shared_dashboard_are_shared_too(mock_audit_log, org, admin, member):
    """The bug: the tool saved tabs itself and skipped sync_dashboard_cascade,
    so people the dashboard was shared with could not see the new chart."""
    first, added = _chart(org, admin, "First"), _chart(org, admin, "Added")
    _, artifact = run_tool(
        dashboard_tools.create_dashboard, admin, title="Shared", chart_ids=[first.id]
    )
    dashboard = Dashboard.objects.get(id=artifact["object_id"])
    _share_dashboard(admin, dashboard, member, "view")
    assert not _chart_share_exists(added, member)

    _, artifact = run_tool(
        dashboard_tools.add_charts_to_dashboard,
        admin,
        dashboard_id=dashboard.id,
        chart_ids=[added.id],
    )

    assert artifact["type"] == "dashboard"
    assert _chart_share_exists(added, member)
