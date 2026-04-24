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
)
from ddpui.schemas.metric_schema import MetricCreate, MetricUpdate
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
    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_create_simple_metric(self, mock_validate, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricCreate(
            name="New API Metric",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
        )
        response = create_metric(request, payload)
        assert response.id is not None
        assert response.name == "New API Metric"
        Metric.objects.filter(id=response.id).delete()

    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_create_expression_metric(self, mock_validate, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricCreate(
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
        payload = MetricCreate(
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
        payload = MetricCreate(
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

    def test_get_metric_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_metric(request, 99999)
        assert exc_info.value.status_code == 404


# ── Update Tests ────────────────────────────────────────────────────────────


class TestUpdateMetric:
    def test_update_metric_name(self, orguser, sample_metric, seed_db):
        request = mock_request(orguser)
        payload = MetricUpdate(name="Updated Name")
        response = update_metric(request, sample_metric.id, payload)
        assert response.name == "Updated Name"

    def test_update_metric_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = MetricUpdate(name="No Metric")
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
