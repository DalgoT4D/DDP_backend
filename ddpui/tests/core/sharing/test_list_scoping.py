"""Part A of Task 3: `accessible_filter` wired into the 5 list services.

Each rtype gets the same shape of test: a Member sees exactly the admitted
set (locked-down/hidden, both-roles-view/visible, analyst-only/
tier-excluded, user-granted/visible, owned/visible); an Analyst with no
grants sees analyst-only + both-roles-view but not locked-down; an Admin
sees everything. Also asserts the filtered queryset evaluates in the same
number of queries as before wiring (no N+1 introduced by
`accessible_filter`).

D1 (permission-model rework): general access is now one independent
``AccessLevel`` per role (``analyst_level``/``member_level``). The old
(audience, level) fixtures below are expressed as the equivalent
(analyst_level, member_level) pair:
    private/admins   -> (none, none)
    analysts_plus    -> (view, none)
    all_users        -> (view, view)
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.core.alerts.alert_service import AlertService
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.metric.metric_service import MetricService
from ddpui.core.reports.report_service import ReportService
from ddpui.services.dashboard_service import DashboardService
from ddpui.models.alert import Alert, AlertType
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="ListScoping Org", slug="list-scoping-org", airbyte_workspace_id="w1"
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
    ou = _make_orguser(org, ADMIN_ROLE, "listscoping-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "listscoping-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "listscoping-member")
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
# Dashboard
# ================================================================================


def _dashboard(org_obj, owner, analyst_level, member_level):
    return Dashboard.objects.create(
        title="Scoping Test Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestDashboardListScoping:
    def test_member_sees_exactly_admitted_set(self, org, member, analyst):
        private_hidden = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        admins_hidden = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        all_users_visible = _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)
        owned_visible = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        granted_visible = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "dashboard", granted_visible, member)

        result = DashboardService.list_dashboards(org=org, orguser=member)
        visible_ids = {d.id for d in result}

        assert visible_ids == {all_users_visible.id, owned_visible.id, granted_visible.id}
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_analyst_sees_analysts_plus_and_all_users_not_private_or_admins(
        self, org, analyst, member
    ):
        private_hidden = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        admins_hidden = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        analysts_plus_visible = _dashboard(org, member, AccessLevel.VIEW, AccessLevel.NONE)
        all_users_visible = _dashboard(org, member, AccessLevel.VIEW, AccessLevel.VIEW)

        result = DashboardService.list_dashboards(org=org, orguser=analyst)
        visible_ids = {d.id for d in result}

        assert visible_ids == {analysts_plus_visible.id, all_users_visible.id}
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_admin_sees_all(self, org, admin, member):
        d1 = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        d2 = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        d3 = _dashboard(org, member, AccessLevel.VIEW, AccessLevel.NONE)
        d4 = _dashboard(org, member, AccessLevel.VIEW, AccessLevel.VIEW)

        result = DashboardService.list_dashboards(org=org, orguser=admin)
        visible_ids = {d.id for d in result}

        assert visible_ids == {d1.id, d2.id, d3.id, d4.id}

    def test_query_count_no_n_plus_one(self, org, member, analyst, django_assert_num_queries):
        for _ in range(5):
            _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)

        with django_assert_num_queries(1):
            list(DashboardService.list_dashboards(org=org, orguser=member))


# ================================================================================
# Report (ReportSnapshot)
# ================================================================================


def _snapshot(org_obj, owner, analyst_level, member_level):
    return ReportSnapshot.objects.create(
        title="Scoping Test Snapshot",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


class TestReportListScoping:
    def test_member_sees_exactly_admitted_set(self, org, member, analyst):
        private_hidden = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        admins_hidden = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        all_users_visible = _snapshot(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)
        owned_visible = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        granted_visible = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", granted_visible, member)

        result = ReportService.list_snapshots(org, orguser=member)
        visible_ids = {s.id for s in result}

        assert visible_ids == {all_users_visible.id, owned_visible.id, granted_visible.id}
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_analyst_sees_analysts_plus_and_all_users_not_private_or_admins(
        self, org, analyst, member
    ):
        private_hidden = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        admins_hidden = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        analysts_plus_visible = _snapshot(org, member, AccessLevel.VIEW, AccessLevel.NONE)
        all_users_visible = _snapshot(org, member, AccessLevel.VIEW, AccessLevel.VIEW)

        result = ReportService.list_snapshots(org, orguser=analyst)
        visible_ids = {s.id for s in result}

        assert visible_ids == {analysts_plus_visible.id, all_users_visible.id}
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_admin_sees_all(self, org, admin, member):
        s1 = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        s2 = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        s3 = _snapshot(org, member, AccessLevel.VIEW, AccessLevel.NONE)
        s4 = _snapshot(org, member, AccessLevel.VIEW, AccessLevel.VIEW)

        result = ReportService.list_snapshots(org, orguser=admin)
        visible_ids = {s.id for s in result}

        assert visible_ids == {s1.id, s2.id, s3.id, s4.id}

    def test_query_count_no_n_plus_one(self, org, member, analyst, django_assert_num_queries):
        for _ in range(5):
            _snapshot(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)

        with django_assert_num_queries(1):
            list(ReportService.list_snapshots(org, orguser=member))


# ================================================================================
# Alert
# ================================================================================


def _alert(org_obj, owner, analyst_level, member_level, name="Scoping Test Alert"):
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


class TestAlertListScoping:
    def test_member_sees_exactly_admitted_set(self, org, member, analyst):
        private_hidden = _alert(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="alert-private"
        )
        admins_hidden = _alert(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="alert-admins"
        )
        all_users_visible = _alert(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name="alert-all"
        )
        owned_visible = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE, name="alert-owned")
        granted_visible = _alert(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="alert-granted"
        )
        _grant(org, "alert", granted_visible, member)

        alerts, total = AlertService.list_alerts(org=org, orguser=member)
        visible_ids = {a.id for a in alerts}

        assert visible_ids == {all_users_visible.id, owned_visible.id, granted_visible.id}
        assert total == 3
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_analyst_sees_analysts_plus_and_all_users_not_private_or_admins(
        self, org, analyst, member
    ):
        private_hidden = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE, name="a-private")
        admins_hidden = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE, name="a-admins")
        analysts_plus_visible = _alert(
            org, member, AccessLevel.VIEW, AccessLevel.NONE, name="a-aplus"
        )
        all_users_visible = _alert(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="a-all")

        alerts, total = AlertService.list_alerts(org=org, orguser=analyst)
        visible_ids = {a.id for a in alerts}

        assert visible_ids == {analysts_plus_visible.id, all_users_visible.id}
        assert total == 2
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_admin_sees_all(self, org, admin, member):
        a1 = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE, name="b-private")
        a2 = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE, name="b-admins")
        a3 = _alert(org, member, AccessLevel.VIEW, AccessLevel.NONE, name="b-aplus")
        a4 = _alert(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="b-all")

        alerts, total = AlertService.list_alerts(org=org, orguser=admin)
        visible_ids = {a.id for a in alerts}

        assert visible_ids == {a1.id, a2.id, a3.id, a4.id}
        assert total == 4

    def test_query_count_matches_baseline_pagination_shape(
        self, org, member, analyst, django_assert_num_queries
    ):
        for i in range(5):
            _alert(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name=f"c-all-{i}")

        # Pre-existing pagination shape does count() + a slice = 2 queries;
        # wiring accessible_filter must not add a 3rd (no N+1).
        with django_assert_num_queries(2):
            AlertService.list_alerts(org=org, orguser=member)


# ================================================================================
# Metric
# ================================================================================


def _metric(org_obj, owner, analyst_level, member_level, name="Scoping Test Metric"):
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


class TestMetricListScoping:
    def test_member_sees_exactly_admitted_set(self, org, member, analyst):
        private_hidden = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="m-private")
        admins_hidden = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="m-admins")
        all_users_visible = _metric(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name="m-all")
        owned_visible = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="m-owned")
        granted_visible = _metric(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="m-granted"
        )
        _grant(org, "metric", granted_visible, member)

        metrics, total = MetricService.list_metrics(org=org, orguser=member)
        visible_ids = {m.id for m in metrics}

        assert visible_ids == {all_users_visible.id, owned_visible.id, granted_visible.id}
        assert total == 3
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_analyst_sees_analysts_plus_and_all_users_not_private_or_admins(
        self, org, analyst, member
    ):
        private_hidden = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="n-private")
        admins_hidden = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="n-admins")
        analysts_plus_visible = _metric(
            org, member, AccessLevel.VIEW, AccessLevel.NONE, name="n-aplus"
        )
        all_users_visible = _metric(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="n-all")

        metrics, total = MetricService.list_metrics(org=org, orguser=analyst)
        visible_ids = {m.id for m in metrics}

        assert visible_ids == {analysts_plus_visible.id, all_users_visible.id}
        assert total == 2
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_admin_sees_all(self, org, admin, member):
        m1 = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="o-private")
        m2 = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="o-admins")
        m3 = _metric(org, member, AccessLevel.VIEW, AccessLevel.NONE, name="o-aplus")
        m4 = _metric(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="o-all")

        metrics, total = MetricService.list_metrics(org=org, orguser=admin)
        visible_ids = {m.id for m in metrics}

        assert visible_ids == {m1.id, m2.id, m3.id, m4.id}
        assert total == 4

    def test_query_count_matches_baseline_pagination_shape(
        self, org, member, analyst, django_assert_num_queries
    ):
        for i in range(5):
            _metric(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name=f"p-all-{i}")

        with django_assert_num_queries(2):
            MetricService.list_metrics(org=org, orguser=member)


# ================================================================================
# KPI
# ================================================================================


def _kpi_with_metric(org_obj, owner, analyst_level, member_level, name="Scoping Test KPI"):
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


class TestKPIListScoping:
    def test_member_sees_exactly_admitted_set(self, org, member, analyst):
        private_hidden = _kpi_with_metric(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="k-private"
        )
        admins_hidden = _kpi_with_metric(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="k-admins"
        )
        all_users_visible = _kpi_with_metric(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name="k-all"
        )
        owned_visible = _kpi_with_metric(
            org, member, AccessLevel.NONE, AccessLevel.NONE, name="k-owned"
        )
        granted_visible = _kpi_with_metric(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="k-granted"
        )
        _grant(org, "kpi", granted_visible, member)

        kpis, total = KPIService.list_kpis(org=org, orguser=member)
        visible_ids = {k.id for k in kpis}

        assert visible_ids == {all_users_visible.id, owned_visible.id, granted_visible.id}
        assert total == 3
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_analyst_sees_analysts_plus_and_all_users_not_private_or_admins(
        self, org, analyst, member
    ):
        private_hidden = _kpi_with_metric(
            org, member, AccessLevel.NONE, AccessLevel.NONE, name="l-private"
        )
        admins_hidden = _kpi_with_metric(
            org, member, AccessLevel.NONE, AccessLevel.NONE, name="l-admins"
        )
        analysts_plus_visible = _kpi_with_metric(
            org, member, AccessLevel.VIEW, AccessLevel.NONE, name="l-aplus"
        )
        all_users_visible = _kpi_with_metric(
            org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="l-all"
        )

        kpis, total = KPIService.list_kpis(org=org, orguser=analyst)
        visible_ids = {k.id for k in kpis}

        assert visible_ids == {analysts_plus_visible.id, all_users_visible.id}
        assert total == 2
        assert private_hidden.id not in visible_ids
        assert admins_hidden.id not in visible_ids

    def test_admin_sees_all(self, org, admin, member):
        k1 = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="q-private")
        k2 = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE, name="q-admins")
        k3 = _kpi_with_metric(org, member, AccessLevel.VIEW, AccessLevel.NONE, name="q-aplus")
        k4 = _kpi_with_metric(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="q-all")

        kpis, total = KPIService.list_kpis(org=org, orguser=admin)
        visible_ids = {k.id for k in kpis}

        assert visible_ids == {k1.id, k2.id, k3.id, k4.id}
        assert total == 4

    def test_query_count_matches_baseline_pagination_shape(
        self, org, member, analyst, django_assert_num_queries
    ):
        for i in range(5):
            _kpi_with_metric(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name=f"r-all-{i}")

        with django_assert_num_queries(2):
            KPIService.list_kpis(org=org, orguser=member)


class TestKPISummaryScoping:
    def test_member_sees_summaries_only_for_admitted_kpis(self, org, member, analyst):
        private_hidden = _kpi_with_metric(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, name="s-private"
        )
        all_users_visible = _kpi_with_metric(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, name="s-all"
        )

        results = KPIService.get_kpi_summary(org, member)
        visible_ids = {r["id"] for r in results}

        assert visible_ids == {all_users_visible.id}
        assert private_hidden.id not in visible_ids

    def test_admin_sees_all_summaries(self, org, admin, member):
        k1 = _kpi_with_metric(
            org, member, AccessLevel.NONE, AccessLevel.NONE, name="s-admin-private"
        )
        k2 = _kpi_with_metric(org, member, AccessLevel.VIEW, AccessLevel.VIEW, name="s-admin-all")

        results = KPIService.get_kpi_summary(org, admin)
        visible_ids = {r["id"] for r in results}

        assert visible_ids == {k1.id, k2.id}
