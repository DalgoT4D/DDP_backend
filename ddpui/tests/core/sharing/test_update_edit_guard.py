"""Resolver-`edit` object checks on the 5 rtypes' update
endpoints, after their existing slug gate (deferred from Task 3 by design).

The slug gate (`can_edit_*`) says what the ROLE may do in general; the
resolver-edit gate says what the viewer may do to THIS object. A user whose
role carries the edit slug but who only has view on the object (via general
access) must get 403. Delete endpoints stay on `can_delete_resource` —
untouched here.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.auth import ANALYST_ROLE
from ddpui.api.alert_api import toggle_alert, update_alert
from ddpui.api.dashboard_native_api import update_dashboard
from ddpui.api.kpi_api import update_kpi
from ddpui.api.metric_api import update_metric
from ddpui.api.report_api import update_snapshot
from ddpui.models.alert import Alert, AlertType
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.schemas.alert_schema import AlertToggle, AlertUpdate
from ddpui.schemas.dashboard_schema import DashboardUpdate
from ddpui.schemas.kpi_schema import KPIExtraConfig, KPIUpdate
from ddpui.schemas.metric_schema import MetricPayload
from ddpui.schemas.report_schema import SnapshotUpdate
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(name="EditGuard Org", slug="edit-guard-org")
    yield org
    KPI.objects.filter(org=org).delete()
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first()
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def owner(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "editguard-owner")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    """Non-owner analyst: carries every can_edit_* slug, but object access
    comes only from general access / grants."""
    ou = _make_orguser(org, ANALYST_ROLE, "editguard-analyst")
    yield ou
    ou.delete()


def _grant_edit(org_obj, rtype, resource, principal):
    ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal.id,
        permission="edit",
        status="active",
    )


VIEW_ONLY = {"analyst_level": AccessLevel.VIEW, "member_level": AccessLevel.NONE}


def _dashboard(org_obj, owner_ou, **general):
    return Dashboard.objects.create(
        title="EditGuard Dashboard", org=org_obj, owner=owner_ou, created_by=owner_ou, **general
    )


def _snapshot(org_obj, owner_ou, **general):
    return ReportSnapshot.objects.create(
        title="EditGuard Snapshot", org=org_obj, owner=owner_ou, created_by=owner_ou, **general
    )


def _alert(org_obj, owner_ou, **general):
    return Alert.objects.create(
        org=org_obj,
        name="EditGuard Alert",
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
        owner=owner_ou,
        created_by=owner_ou,
        **general,
    )


def _metric(org_obj, owner_ou, name="editguard-metric", **general):
    return Metric.objects.create(
        name=name,
        schema_name="public",
        table_name="beneficiaries",
        column="amount",
        aggregation="sum",
        org=org_obj,
        owner=owner_ou,
        created_by=owner_ou,
        **general,
    )


def _kpi(org_obj, owner_ou, **general):
    metric = _metric(org_obj, owner_ou, name="editguard-kpi-metric")
    return KPI.objects.create(
        name="EditGuard KPI",
        metric=metric,
        direction="increase",
        time_grain="monthly",
        org=org_obj,
        owner=owner_ou,
        created_by=owner_ou,
        **general,
    )


class TestViewOnlyCannotUpdate:
    def test_dashboard(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            update_dashboard(mock_request(analyst), dashboard.id, DashboardUpdate(title="nope"))
        assert excinfo.value.status_code == 403
        dashboard.refresh_from_db()
        assert dashboard.title == "EditGuard Dashboard"

    def test_report(self, org, owner, analyst):
        snapshot = _snapshot(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            update_snapshot(mock_request(analyst), snapshot.id, SnapshotUpdate(summary="nope"))
        assert excinfo.value.status_code == 403

    def test_alert(self, org, owner, analyst):
        alert = _alert(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            update_alert(mock_request(analyst), alert.id, AlertUpdate(name="nope"))
        assert excinfo.value.status_code == 403

    def test_alert_toggle(self, org, owner, analyst):
        alert = _alert(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            toggle_alert(mock_request(analyst), alert.id, AlertToggle(is_active=False))
        assert excinfo.value.status_code == 403

    def test_metric(self, org, owner, analyst):
        metric = _metric(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            update_metric(
                mock_request(analyst),
                metric.id,
                MetricPayload(
                    name="nope", schema_name="public", table_name="t", column="c", aggregation="sum"
                ),
            )
        assert excinfo.value.status_code == 403

    def test_kpi(self, org, owner, analyst):
        kpi = _kpi(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            update_kpi(
                mock_request(analyst),
                kpi.id,
                KPIUpdate(name="nope", extra_config=KPIExtraConfig()),
            )
        assert excinfo.value.status_code == 403


class TestEditorCanUpdate:
    def test_editor_via_grant_can_update_dashboard(self, org, owner, analyst):
        dashboard = _dashboard(
            org,
            owner,
            analyst_level=AccessLevel.NONE,
            member_level=AccessLevel.NONE,
        )
        _grant_edit(org, "dashboard", dashboard, analyst)

        update_dashboard(mock_request(analyst), dashboard.id, DashboardUpdate(title="updated"))
        dashboard.refresh_from_db()
        assert dashboard.title == "updated"

    def test_editor_via_general_edit_can_update_metric(self, org, owner, analyst):
        metric = _metric(
            org,
            owner,
            analyst_level=AccessLevel.EDIT,
            member_level=AccessLevel.NONE,
        )
        update_metric(
            mock_request(analyst),
            metric.id,
            MetricPayload(
                name="updated-metric",
                schema_name="public",
                table_name="beneficiaries",
                column="amount",
                aggregation="sum",
            ),
        )
        metric.refresh_from_db()
        assert metric.name == "updated-metric"

    def test_owner_can_update_own_private_alert(self, org, owner):
        alert = _alert(
            org,
            owner,
            analyst_level=AccessLevel.NONE,
            member_level=AccessLevel.NONE,
        )
        toggle_alert(mock_request(owner), alert.id, AlertToggle(is_active=False))
        alert.refresh_from_db()
        assert alert.is_active is False
