"""Tests for MetricService business logic"""

import os
import django
import pytest
from unittest.mock import patch, MagicMock

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.metric import Metric, KPI
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.services.metric_service import (
    MetricService,
    MetricNotFoundError,
    MetricValidationError,
    MetricDeleteBlockedError,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="metricserviceuser", email="metricserviceuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Metric Service Test Org",
        slug="metric-svc-test",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def other_org():
    org = Org.objects.create(
        name="Other Org",
        slug="other-org-metric",
        airbyte_workspace_id="workspace-id-2",
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
        name="Test Metric",
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


@pytest.fixture
def expression_metric(orguser, org):
    metric = Metric.objects.create(
        name="Expression Metric",
        description="A test expression metric",
        schema_name="public",
        table_name="beneficiaries",
        column_expression="SUM(col_a) / COUNT(DISTINCT id)",
        org=org,
        created_by=orguser,
    )
    yield metric
    try:
        metric.refresh_from_db()
        metric.delete()
    except Metric.DoesNotExist:
        pass


# ── Validation Tests ────────────────────────────────────────────────────────


class TestMetricValidation:
    def test_validate_simple_valid(self):
        MetricService.validate_metric_definition("amount", "sum", None)

    def test_validate_count_star(self):
        """COUNT(*) — column is None, aggregation is count"""
        MetricService.validate_metric_definition(None, "count", None)

    def test_validate_expression_valid(self):
        MetricService.validate_metric_definition(None, None, "SUM(col_a) / COUNT(DISTINCT id)")

    def test_validate_both_paths_rejected(self):
        with pytest.raises(MetricValidationError, match="not both"):
            MetricService.validate_metric_definition("amount", "sum", "SUM(amount)")

    def test_validate_neither_path_rejected(self):
        with pytest.raises(MetricValidationError, match="Provide either"):
            MetricService.validate_metric_definition(None, None, None)

    def test_validate_empty_expression_rejected(self):
        with pytest.raises(MetricValidationError, match="Provide either"):
            MetricService.validate_metric_definition(None, None, "   ")

    def test_validate_invalid_aggregation(self):
        with pytest.raises(MetricValidationError, match="Invalid aggregation"):
            MetricService.validate_metric_definition("amount", "median", None)

    def test_validate_missing_aggregation(self):
        with pytest.raises(MetricValidationError, match="aggregation is required"):
            MetricService.validate_metric_definition("amount", None, None)

    def test_validate_missing_column_for_non_count(self):
        with pytest.raises(MetricValidationError, match="column is required"):
            MetricService.validate_metric_definition(None, "sum", None)


# ── CRUD Tests ──────────────────────────────────────────────────────────────


class TestMetricCRUD:
    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_create_simple_metric(self, mock_validate, orguser, seed_db):
        metric = MetricService.create_metric(
            name="New Metric",
            description="desc",
            schema_name="public",
            table_name="beneficiaries",
            column="amount",
            aggregation="sum",
            column_expression=None,
            orguser=orguser,
        )
        assert metric.id is not None
        assert metric.name == "New Metric"
        assert metric.column == "amount"
        assert metric.aggregation == "sum"
        assert metric.column_expression is None
        metric.delete()

    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_create_expression_metric(self, mock_validate, orguser, seed_db):
        metric = MetricService.create_metric(
            name="Expr Metric",
            description="desc",
            schema_name="public",
            table_name="beneficiaries",
            column=None,
            aggregation=None,
            column_expression="SUM(col_a) / COUNT(DISTINCT id)",
            orguser=orguser,
        )
        assert metric.id is not None
        assert metric.column_expression == "SUM(col_a) / COUNT(DISTINCT id)"
        assert metric.column is None
        assert metric.aggregation is None
        metric.delete()

    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_create_duplicate_name_rejected(self, mock_validate, orguser, sample_metric, seed_db):
        with pytest.raises(MetricValidationError, match="already exists"):
            MetricService.create_metric(
                name="Test Metric",
                description="dup",
                schema_name="public",
                table_name="beneficiaries",
                column="amount",
                aggregation="sum",
                column_expression=None,
                orguser=orguser,
            )

    def test_get_metric(self, orguser, org, sample_metric, seed_db):
        metric = MetricService.get_metric(sample_metric.id, org)
        assert metric.id == sample_metric.id

    def test_get_metric_not_found(self, org, seed_db):
        with pytest.raises(MetricNotFoundError):
            MetricService.get_metric(99999, org)

    def test_get_metric_wrong_org(self, other_org, sample_metric, seed_db):
        with pytest.raises(MetricNotFoundError):
            MetricService.get_metric(sample_metric.id, other_org)

    def test_list_metrics(self, org, sample_metric, seed_db):
        metrics, total = MetricService.list_metrics(org)
        assert total >= 1
        assert any(m.id == sample_metric.id for m in metrics)

    def test_list_metrics_search(self, org, sample_metric, seed_db):
        metrics, total = MetricService.list_metrics(org, search="Test")
        assert total >= 1

        metrics, total = MetricService.list_metrics(org, search="nonexistent_xyz")
        assert total == 0

    def test_list_metrics_filter_by_dataset(self, org, sample_metric, seed_db):
        metrics, total = MetricService.list_metrics(
            org, schema_name="public", table_name="beneficiaries"
        )
        assert total >= 1

        metrics, total = MetricService.list_metrics(org, schema_name="other_schema")
        assert total == 0

    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_update_metric_name(self, mock_validate, orguser, org, sample_metric, seed_db):
        updated = MetricService.update_metric(sample_metric.id, org, orguser, name="Renamed Metric")
        assert updated.name == "Renamed Metric"

    @patch("ddpui.services.metric_service.MetricService.validate_metric_against_warehouse")
    def test_update_metric_definition(self, mock_validate, orguser, org, sample_metric, seed_db):
        # Need an OrgWarehouse so the validation path is triggered
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})
        updated = MetricService.update_metric(
            sample_metric.id, org, orguser, column="other_col", aggregation="avg"
        )
        assert updated.column == "other_col"
        assert updated.aggregation == "avg"
        mock_validate.assert_called_once()
        OrgWarehouse.objects.filter(org=org).delete()

    def test_delete_metric(self, orguser, org, sample_metric, seed_db):
        metric_id = sample_metric.id
        MetricService.delete_metric(metric_id, org, orguser)
        with pytest.raises(MetricNotFoundError):
            MetricService.get_metric(metric_id, org)

    def test_delete_metric_blocked_by_kpi(self, orguser, org, sample_metric, seed_db):
        kpi = KPI.objects.create(
            name="Test KPI",
            metric=sample_metric,
            direction="increase",
            time_grain="monthly",
            org=org,
            created_by=orguser,
        )
        with pytest.raises(MetricDeleteBlockedError, match="referenced"):
            MetricService.delete_metric(sample_metric.id, org, orguser)
        kpi.delete()

    def test_delete_metric_blocked_by_chart(self, orguser, org, sample_metric, seed_db):
        chart = Chart.objects.create(
            title="Chart with saved metric",
            chart_type="bar",
            schema_name="public",
            table_name="beneficiaries",
            extra_config={"metrics": [{"saved_metric_id": sample_metric.id}]},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )
        with pytest.raises(MetricDeleteBlockedError, match="referenced"):
            MetricService.delete_metric(sample_metric.id, org, orguser)
        chart.delete()


# ── Consumer Tracking Tests ─────────────────────────────────────────────────


class TestMetricConsumers:
    def test_no_consumers(self, org, sample_metric, seed_db):
        consumers = MetricService.get_metric_consumers(sample_metric.id, org)
        assert consumers["charts"] == []
        assert consumers["kpis"] == []

    def test_kpi_consumer(self, orguser, org, sample_metric, seed_db):
        kpi = KPI.objects.create(
            name="KPI Consumer",
            metric=sample_metric,
            direction="increase",
            time_grain="monthly",
            org=org,
            created_by=orguser,
        )
        consumers = MetricService.get_metric_consumers(sample_metric.id, org)
        assert len(consumers["kpis"]) == 1
        assert consumers["kpis"][0]["id"] == kpi.id
        kpi.delete()

    def test_chart_consumer(self, orguser, org, sample_metric, seed_db):
        chart = Chart.objects.create(
            title="Chart Ref",
            chart_type="bar",
            schema_name="public",
            table_name="beneficiaries",
            extra_config={"metrics": [{"saved_metric_id": sample_metric.id}]},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )
        consumers = MetricService.get_metric_consumers(sample_metric.id, org)
        assert len(consumers["charts"]) == 1
        assert consumers["charts"][0]["id"] == chart.id
        chart.delete()


# ── Preview Tests ───────────────────────────────────────────────────────────


class TestMetricPreview:
    def test_preview_no_warehouse(self, org, sample_metric, seed_db):
        result = MetricService.preview_metric_value(sample_metric.id, org)
        assert result["value"] is None
        assert result["error"] == "Warehouse not configured"

    @patch("ddpui.services.metric_service.MetricService.compute_metric_value")
    def test_preview_success(self, mock_compute, orguser, org, sample_metric, seed_db):
        mock_compute.return_value = 42.0
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = MetricService.preview_metric_value(sample_metric.id, org)
        assert result["value"] == 42.0
        assert result["error"] is None

        OrgWarehouse.objects.filter(org=org).delete()

    @patch("ddpui.services.metric_service.MetricService.compute_metric_value")
    def test_preview_error(self, mock_compute, orguser, org, sample_metric, seed_db):
        mock_compute.side_effect = Exception("query failed")
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = MetricService.preview_metric_value(sample_metric.id, org)
        assert result["value"] is None
        assert "query failed" in result["error"]

        OrgWarehouse.objects.filter(org=org).delete()
