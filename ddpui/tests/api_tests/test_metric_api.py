"""API Tests for Metric endpoints"""

import os
import django
from unittest.mock import patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.metric import Metric, KPI
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.metric_api import (
    list_metrics,
    create_metric,
    get_metric,
    update_metric,
    delete_metric,
    preview_metric,
    get_metric_consumers,
    validate_metric,
)
from ddpui.schemas.metric_schema import MetricPayload
from ddpui.core.metric.metric_service import MetricValidationError
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="metricapiuser", email="metricapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Metric API Test Org",
        slug="metric-api-test",
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
        name="API Test Metric",
        description="A test metric",
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


# ── List Tests ──────────────────────────────────────────────────────────────


class TestListMetrics:
    def test_list_metrics_success(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = list_metrics(request)
        assert response.total >= 1
        assert any(m.id == sample_metric.id for m in response.data)

    def test_list_metrics_includes_created_by(self, orguser, sample_metric, seed_db):
        """list_metrics returns the creator's email in created_by"""
        request = mock_request(orguser)

        response = list_metrics(request)

        assert response.data[0].created_by == "metricapiuser@test.com"

    def test_list_metrics_search(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = list_metrics(request, search="API Test")
        assert response.total >= 1

    def test_list_metrics_filter_dataset(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = list_metrics(request, schema_name="public", table_name="beneficiaries")
        assert response.total >= 1

    def test_list_metrics_empty(self, orguser, seed_db):
        request = mock_request(orguser)
        response = list_metrics(request, search="nonexistent_xyz_123")
        assert response.total == 0
        assert response.data == []


# ── Create Tests ────────────────────────────────────────────────────────────


class TestCreateMetric:
    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    def test_create_simple_metric(self, mock_validate, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="New API Metric",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
        )
        response = create_metric(request, payload)
        assert response.id is not None
        assert response.name == "New API Metric"
        assert response.created_by == "metricapiuser@test.com"
        Metric.objects.filter(id=response.id).delete()

    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    def test_create_expression_metric(self, mock_validate, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Expr API Metric",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="SUM(col_a) / COUNT(DISTINCT id)",
        )
        response = create_metric(request, payload)
        assert response.column_expression == "SUM(col_a) / COUNT(DISTINCT id)"
        Metric.objects.filter(id=response.id).delete()

    def test_create_invalid_both_paths(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Bad Metric",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
            column_expression="SUM(amount)",
        )
        with pytest.raises(HttpError) as exc_info:
            create_metric(request, payload)
        assert exc_info.value.status_code == 400

    def test_create_invalid_aggregation(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Bad Agg Metric",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="median",
        )
        with pytest.raises(HttpError) as exc_info:
            create_metric(request, payload)
        assert exc_info.value.status_code == 400


# ── Get Tests ───────────────────────────────────────────────────────────────


class TestGetMetric:
    def test_get_metric_success(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = get_metric(request, sample_metric.id)
        assert response.id == sample_metric.id
        assert response.name == sample_metric.name
        assert response.created_by == "metricapiuser@test.com"

    def test_get_metric_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_metric(request, 99999)
        assert exc_info.value.status_code == 404


# ── Update Tests ────────────────────────────────────────────────────────────


class TestUpdateMetric:
    def test_update_metric_name(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Updated Name",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
        )
        response = update_metric(request, sample_metric.id, payload)
        assert response.name == "Updated Name"

    def test_update_metric_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="No Metric",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
        )
        with pytest.raises(HttpError) as exc_info:
            update_metric(request, 99999, payload)
        assert exc_info.value.status_code == 404


# ── Delete Tests ────────────────────────────────────────────────────────────


class TestDeleteMetric:
    def test_delete_metric_success(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        metric_id = sample_metric.id
        response = delete_metric(request, metric_id)
        assert response["success"] is True

    def test_delete_metric_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_metric(request, 99999)
        assert exc_info.value.status_code == 404

    def test_delete_metric_blocked(self, orguser, sample_metric, seed_db):
        kpi = KPI.objects.create(
            name="Blocking KPI",
            metric=sample_metric,
            direction="increase",
            time_grain="monthly",
            org=sample_metric.org,
            created_by=orguser,
        )
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_metric(request, sample_metric.id)
        assert exc_info.value.status_code == 409
        kpi.delete()


# ── Preview Tests ───────────────────────────────────────────────────────────


class TestPreviewMetric:
    def test_preview_no_warehouse(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = preview_metric(request, sample_metric.id)
        assert response.value is None
        assert response.error == "Warehouse not configured"

    def test_preview_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            preview_metric(request, 99999)
        assert exc_info.value.status_code == 404


# ── Validate Tests ──────────────────────────────────────────────────────


class TestValidateMetric:
    def test_validate_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="test",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="SUM(amount)",
        )
        response = validate_metric(request, payload)
        assert response.valid is False
        assert response.error == "Warehouse not configured"

    def test_validate_rejects_sql_statement(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="test",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="SELECT COUNT(*) FROM users",
        )
        response = validate_metric(request, payload)
        assert response.valid is False
        assert "SQL statements" in response.error

    def test_validate_rejects_invalid_payload(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricPayload(
            name="test",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
            column_expression="SUM(amount)",
        )
        response = validate_metric(request, payload)
        assert response.valid is False
        assert "not both" in response.error

    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    def test_validate_success(self, mock_query, orguser, seed_db):
        OrgWarehouse.objects.create(org=orguser.org, wtype="postgres", credentials={})
        request = mock_request(orguser)
        payload = MetricPayload(
            name="test",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="SUM(amount)",
        )
        response = validate_metric(request, payload)
        assert response.valid is True
        mock_query.assert_called_once()
        OrgWarehouse.objects.filter(org=orguser.org).delete()

    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    def test_validate_warehouse_error(self, mock_query, orguser, seed_db):
        mock_query.side_effect = MetricValidationError("column xyz does not exist")
        OrgWarehouse.objects.create(org=orguser.org, wtype="postgres", credentials={})
        request = mock_request(orguser)
        payload = MetricPayload(
            name="test",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="SUM(xyz)",
        )
        response = validate_metric(request, payload)
        assert response.valid is False
        assert "xyz" in response.error
        OrgWarehouse.objects.filter(org=orguser.org).delete()


# ── Consumers Tests ─────────────────────────────────────────────────────────


class TestMetricConsumers:
    def test_consumers_empty(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        response = get_metric_consumers(request, sample_metric.id)
        assert response.charts == []
        assert response.kpis == []

    def test_consumers_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_metric_consumers(request, 99999)
        assert exc_info.value.status_code == 404


# ── Audit Log Tests ─────────────────────────────────────────────────────────
from ddpui.models.audit_log import AuditLogResourceType, AuditLogAction


class TestMetricAuditLogs:
    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    @patch("ddpui.api.metric_api.create_audit_log")
    def test_create_metric_creates_audit_log(self, mock_audit_log, mock_validate, orguser, seed_db):
        """Test that creating a metric creates an audit log entry."""
        OrgWarehouse.objects.create(org=orguser.org, wtype="postgres", credentials={})
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Audit Log Test Metric",
            schema_name="public",
            table_name="beneficiaries",
            column_expression="COUNT(*)",
        )

        response = create_metric(request, payload)

        assert response.name == "Audit Log Test Metric"
        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.METRIC
        assert call_kwargs["action"] == AuditLogAction.CREATE
        assert call_kwargs["resource_name"] == "Audit Log Test Metric"

        resource_fields = call_kwargs["resource_fields"]
        assert resource_fields["name"] == "Audit Log Test Metric"
        assert resource_fields["schema_name"] == "public"
        assert resource_fields["table_name"] == "beneficiaries"
        assert resource_fields["column_expression"] == "COUNT(*)"

        # Cleanup
        Metric.objects.filter(name="Audit Log Test Metric").delete()
        OrgWarehouse.objects.filter(org=orguser.org).delete()

    @patch("ddpui.core.metric.metric_service.MetricService.validate_metric_query")
    @patch("ddpui.api.metric_api.create_audit_log")
    def test_update_metric_creates_audit_log(
        self, mock_audit_log, mock_validate, orguser, sample_metric, seed_db
    ):
        """Test that updating a metric creates an audit log entry."""
        OrgWarehouse.objects.create(org=orguser.org, wtype="postgres", credentials={})
        request = mock_request(orguser)
        payload = MetricPayload(
            name="Updated Metric Name",
            schema_name=sample_metric.schema_name,
            table_name=sample_metric.table_name,
            column_expression=sample_metric.column_expression,
        )

        response = update_metric(request, sample_metric.id, payload)

        assert response.name == "Updated Metric Name"
        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.METRIC
        assert call_kwargs["action"] == AuditLogAction.UPDATE
        assert call_kwargs["resource_id"] == str(sample_metric.id)

        # Curated snapshot, not a diff — every field from the payload is logged,
        # not just the one that actually changed (name here).
        resource_fields = call_kwargs["resource_fields"]
        assert resource_fields["name"] == "Updated Metric Name"
        assert resource_fields["schema_name"] == sample_metric.schema_name
        assert resource_fields["table_name"] == sample_metric.table_name
        assert resource_fields["column_expression"] == sample_metric.column_expression

        # Cleanup
        OrgWarehouse.objects.filter(org=orguser.org).delete()

    @patch("ddpui.api.metric_api.create_audit_log")
    def test_delete_metric_creates_audit_log(self, mock_audit_log, orguser, org, seed_db):
        """Test that deleting a metric creates an audit log entry."""
        metric = Metric.objects.create(
            name="Metric To Delete",
            schema_name="public",
            table_name="users",
            column_expression="COUNT(*)",
            created_by=orguser,
            org=org,
        )
        metric_id = metric.id

        request = mock_request(orguser)
        delete_metric(request, metric_id)

        mock_audit_log.assert_called_once()
        call_kwargs = mock_audit_log.call_args[1]
        assert call_kwargs["org"] == orguser.org
        assert call_kwargs["resource_type"] == AuditLogResourceType.METRIC
        assert call_kwargs["action"] == AuditLogAction.DELETE
        assert call_kwargs["resource_id"] == str(metric_id)
        assert call_kwargs["resource_name"] == "Metric To Delete"
