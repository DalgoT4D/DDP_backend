"""API Tests for KPI endpoints"""

import os
import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.metric import Metric, KPI
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.kpi_api import (
    list_kpis,
    create_kpi,
    get_kpi,
    update_kpi,
    delete_kpi,
)
from ddpui.schemas.kpi_schema import KPICreate, KPIUpdate, KPIExtraConfig
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="kpiapiuser", email="kpiapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="KPI API Test Org",
        slug="kpi-api-test",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_metric(orguser, org):
    metric = Metric.objects.create(
        name="API KPI Metric",
        schema_name="public",
        table_name="beneficiaries",
        column="amount",
        aggregation="sum",
        org=org,
        created_by=orguser,
    )
    yield metric
    try:
        metric.refresh_from_db()
        metric.delete()
    except Metric.DoesNotExist:
        pass


@pytest.fixture
def sample_kpi(orguser, org, sample_metric):
    kpi = KPI.objects.create(
        name="API Test KPI",
        metric=sample_metric,
        target_value=1000.0,
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    yield kpi
    try:
        kpi.refresh_from_db()
        kpi.delete()
    except KPI.DoesNotExist:
        pass


# ── Tests ───────────────────────────────────────────────────────────────────


class TestListKPIs:
    def test_list_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request)
        assert response.total >= 1

    def test_list_search(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request, search="API Test")
        assert response.total >= 1

    def test_list_empty(self, orguser, seed_db):
        request = mock_request(orguser)
        response = list_kpis(request, search="nonexistent_xyz")
        assert response.total == 0

    def test_list_kpis_includes_created_by(self, orguser, sample_kpi, seed_db):
        """list_kpis returns the creator's email in created_by"""
        request = mock_request(orguser)

        response = list_kpis(request)

        assert response.data[0].created_by == "kpiapiuser@test.com"


class TestCreateKPI:
    def test_create_success(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            target_value=500.0,
            extra_config=KPIExtraConfig(),
        )
        response = create_kpi(request, payload)
        assert response.id is not None
        assert response.name == sample_metric.name
        assert response.created_by == "kpiapiuser@test.com"
        KPI.objects.filter(id=response.id).delete()

    def test_create_invalid_metric(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=99999,
            direction="increase",
            time_grain="monthly",
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(HttpError) as exc_info:
            create_kpi(request, payload)
        assert exc_info.value.status_code == 404

    def test_create_invalid_direction(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="sideways",
            time_grain="monthly",
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(HttpError) as exc_info:
            create_kpi(request, payload)
        assert exc_info.value.status_code == 400


class TestGetKPI:
    def test_get_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = get_kpi(request, sample_kpi.id)
        assert response.id == sample_kpi.id
        assert response.metric.id == sample_kpi.metric_id
        assert response.created_by == "kpiapiuser@test.com"

    def test_get_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_kpi(request, 99999)
        assert exc_info.value.status_code == 404


class TestUpdateKPI:
    def test_update_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        payload = KPIUpdate(
            name="Updated API KPI", target_value=2000.0, extra_config=KPIExtraConfig()
        )
        response = update_kpi(request, sample_kpi.id, payload)
        assert response.name == "Updated API KPI"
        assert response.target_value == 2000.0

    def test_update_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = KPIUpdate(name="Nope", extra_config=KPIExtraConfig())
        with pytest.raises(HttpError) as exc_info:
            update_kpi(request, 99999, payload)
        assert exc_info.value.status_code == 404


class TestDeleteKPI:
    def test_delete_success(self, orguser, sample_kpi, seed_db):
        request = mock_request(orguser)
        response = delete_kpi(request, sample_kpi.id)
        assert response["success"] is True

    def test_delete_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_kpi(request, 99999)
        assert exc_info.value.status_code == 404


# ── Audit Log Tests ─────────────────────────────────────────────────────────
from unittest.mock import patch
from ddpui.models.audit_log import AuditLogResourceType, AuditLogAction


class TestKPIAuditLogs:
    @patch("ddpui.api.kpi_api.create_audit_log")
    def test_create_kpi_creates_audit_log(self, mock_audit_log, orguser, sample_metric, seed_db):
        """Test that creating a KPI creates an audit log entry."""
        request = mock_request(orguser)
        payload = KPICreate(
            name="Audit Log Test KPI",
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            target_value=1000.0,
            extra_config=KPIExtraConfig(),
        )

        response = create_kpi(request, payload)

        assert response.name == "Audit Log Test KPI"
        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.KPI
        assert call_kwargs["action"] == AuditLogAction.CREATE

        # Metric resolved to its real name, not a bare metric_id.
        resource_fields = call_kwargs["resource_fields"]
        assert resource_fields["metric"] == sample_metric.name
        assert "metric_id" not in resource_fields
        assert resource_fields["target_value"] == 1000.0
        assert resource_fields["direction"] == "increase"
        assert resource_fields["time_grain"] == "monthly"

        # Cleanup
        KPI.objects.filter(name="Audit Log Test KPI").delete()

    @patch("ddpui.api.kpi_api.create_audit_log")
    def test_update_kpi_creates_audit_log(self, mock_audit_log, orguser, sample_kpi, seed_db):
        """Test that updating a KPI creates an audit log entry."""
        request = mock_request(orguser)
        payload = KPIUpdate(
            name="Updated KPI Name",
            target_value=2000.0,
            extra_config=KPIExtraConfig(),
        )

        response = update_kpi(request, sample_kpi.id, payload)

        assert response.name == "Updated KPI Name"
        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.KPI
        assert call_kwargs["action"] == AuditLogAction.UPDATE
        assert call_kwargs["resource_id"] == str(sample_kpi.id)

        # KPIUpdate is a genuine partial patch — only the fields actually sent
        # (name, target_value, extra_config) should appear, not the whole KPI.
        resource_fields = call_kwargs["resource_fields"]
        assert resource_fields["name"] == "Updated KPI Name"
        assert resource_fields["target_value"] == 2000.0
        assert "direction" not in resource_fields
        assert "time_grain" not in resource_fields
        assert "metric" not in resource_fields

    @patch("ddpui.api.kpi_api.create_audit_log")
    def test_update_kpi_change_metric_logs_resolved_name(
        self, mock_audit_log, orguser, org, sample_kpi, seed_db
    ):
        """Changing metric_id logs the new metric's name, not the raw id."""
        other_metric = Metric.objects.create(
            name="Other API KPI Metric",
            schema_name="public",
            table_name="other",
            column="amount",
            aggregation="sum",
            org=org,
            created_by=orguser,
        )
        request = mock_request(orguser)
        payload = KPIUpdate(metric_id=other_metric.id, extra_config=KPIExtraConfig())

        try:
            update_kpi(request, sample_kpi.id, payload)

            call_kwargs = mock_audit_log.call_args[1]
            resource_fields = call_kwargs["resource_fields"]
            assert resource_fields["metric"] == "Other API KPI Metric"
            assert "metric_id" not in resource_fields
            # name is always included now (current value), so the row stays
            # self-identifying without a separate name column.
            assert resource_fields["name"] == sample_kpi.name
        finally:
            # sample_kpi now references other_metric (PROTECT) — delete the KPI
            # first so other_metric's teardown doesn't hit a ProtectedError.
            KPI.objects.filter(id=sample_kpi.id).delete()
            other_metric.delete()

    @patch("ddpui.api.kpi_api.create_audit_log")
    def test_delete_kpi_creates_audit_log(
        self, mock_audit_log, orguser, sample_metric, org, seed_db
    ):
        """Test that deleting a KPI creates an audit log entry."""
        kpi = KPI.objects.create(
            name="KPI To Delete",
            metric=sample_metric,
            target_value=500.0,
            extra_config={},
            created_by=orguser,
            org=org,
        )
        kpi_id = kpi.id

        request = mock_request(orguser)
        delete_kpi(request, kpi_id)

        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.KPI
        assert call_kwargs["action"] == AuditLogAction.DELETE
        assert call_kwargs["resource_id"] == str(kpi_id)
        assert call_kwargs["resource_fields"] == {"name": "KPI To Delete"}
