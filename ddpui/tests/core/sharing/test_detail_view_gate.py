"""View-gate the 5 single-resource GET endpoints.

After the existing org-scoped fetch, deny with 403 when
`effective_permission(viewer, rtype, resource)` is None. 403 (not 404) is
used deliberately — the codebase already uses 403 widely for "exists but
you may not act on it" (e.g. dashboard sharing-settings guards, delete
guards), and the plan calls for a later frontend "request access" flow to
key off 403. 404 stays reserved for "doesn't exist / wrong org" (unchanged,
service-layer NotFoundError path).

Endpoints are called directly (as the rest of the API test suite does),
via `mock_request(orguser)` — this exercises the real `has_permission`
decorator plus the view-gate logic added in this task.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.api.dashboard_native_api import get_dashboard
from ddpui.api.alert_api import get_alert
from ddpui.api.metric_api import get_metric
from ddpui.api.kpi_api import get_kpi
from ddpui.api.report_api import get_snapshot_view
from ddpui.models.alert import Alert, AlertType
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.report import ReportSnapshot
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="DetailGate Org", slug="detail-gate-org", airbyte_workspace_id="w1"
    )
    yield org
    # KPI.metric is PROTECT — delete KPIs before the Metric/Org CASCADE runs.
    KPI.objects.filter(org=org).delete()
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "detailgate-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "detailgate-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "detailgate-member")
    yield ou
    ou.delete()


def _grant(org_obj, rtype, resource, principal_orguser, permission="view"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal_orguser.id,
        permission=permission,
        status="active",
    )


# ================================================================================
# Dashboard — get_dashboard
# ================================================================================


def _dashboard(org_obj, owner, analyst_level, member_level):
    return Dashboard.objects.create(
        title="Detail Gate Test Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestDashboardDetailGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_dashboard(request, dashboard.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        response = get_dashboard(request, dashboard.id)
        assert response.id == dashboard.id

    def test_member_allowed_on_all_users_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)
        request = mock_request(member)

        response = get_dashboard(request, dashboard.id)
        assert response.id == dashboard.id

    def test_owner_allowed_on_own_private_resource(self, org, member):
        dashboard = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        response = get_dashboard(request, dashboard.id)
        assert response.id == dashboard.id

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        dashboard = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_dashboard(request, dashboard.id)
        assert response.id == dashboard.id

    def test_wrong_org_still_404_not_403(self, org, member):
        other_org = Org.objects.create(
            name="Other Org", slug="detail-gate-other", airbyte_workspace_id="w2"
        )
        other_dashboard = Dashboard.objects.create(
            title="Other org dashboard",
            org=other_org,
            analyst_level=AccessLevel.VIEW,
            member_level=AccessLevel.VIEW,
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_dashboard(request, other_dashboard.id)
        assert exc_info.value.status_code == 404

        other_dashboard.delete()
        other_org.delete()


# ================================================================================
# Alert — get_alert
# ================================================================================


def _alert(org_obj, owner, analyst_level, member_level, name="Detail Gate Test Alert"):
    return Alert.objects.create(
        org=org_obj,
        name=name,
        alert_type=AlertType.STANDALONE,
        standalone_config={
            "schema_name": "public",
            "table_name": "t",
            "column": "amount",
            "aggregation": "sum",
        },
        condition={"operator": "gt", "value": 0},
        schedule_cron="0 9 * * *",
        message_template="test",
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestAlertDetailGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        alert = _alert(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_alert(request, alert.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        alert = _alert(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "alert", alert, member)
        request = mock_request(member)

        response = get_alert(request, alert.id)
        assert response.id == alert.id

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        alert = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_alert(request, alert.id)
        assert response.id == alert.id


# ================================================================================
# Metric — get_metric
# ================================================================================


def _metric(org_obj, owner, analyst_level, member_level, name="Detail Gate Test Metric"):
    return Metric.objects.create(
        name=name,
        schema_name="public",
        table_name="beneficiaries",
        column="amount",
        aggregation="sum",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestMetricDetailGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_metric(request, metric.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "metric", metric, member)
        request = mock_request(member)

        response = get_metric(request, metric.id)
        assert response.id == metric.id

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        metric = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_metric(request, metric.id)
        assert response.id == metric.id


# ================================================================================
# KPI — get_kpi
# ================================================================================


def _kpi_with_metric(org_obj, owner, analyst_level, member_level, name="Detail Gate Test KPI"):
    metric = Metric.objects.create(
        name=f"{name} metric",
        schema_name="public",
        table_name="beneficiaries",
        column="amount",
        aggregation="sum",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=AccessLevel.VIEW,
        member_level=AccessLevel.VIEW,
    )
    return KPI.objects.create(
        name=name,
        metric=metric,
        direction="increase",
        time_grain="monthly",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestKPIDetailGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_kpi(request, kpi.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "kpi", kpi, member)
        request = mock_request(member)

        response = get_kpi(request, kpi.id)
        assert response.id == kpi.id

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        kpi = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_kpi(request, kpi.id)
        assert response.id == kpi.id


# ================================================================================
# Report — get_snapshot_view
# ================================================================================


def _snapshot(org_obj, owner, analyst_level, member_level):
    return ReportSnapshot.objects.create(
        title="Detail Gate Test Snapshot",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestReportDetailGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_snapshot_view(request, snapshot.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = get_snapshot_view(request, snapshot.id)
        assert response["success"] is True

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_snapshot_view(request, snapshot.id)
        assert response["success"] is True
