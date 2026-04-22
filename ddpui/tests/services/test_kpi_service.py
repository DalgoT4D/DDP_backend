"""Tests for KPIService business logic"""

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
from ddpui.models.dashboard import Dashboard
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.schemas.kpi_schema import KPICreate, KPIUpdate
from ddpui.services.kpi_service import (
    KPIService,
    KPINotFoundError,
    KPIValidationError,
    compute_rag_status,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="kpiserviceuser", email="kpiserviceuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="KPI Service Test Org",
        slug="kpi-svc-test",
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
        name="KPI Test Metric",
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
        name="Test KPI",
        metric=sample_metric,
        target_value=1000.0,
        direction="increase",
        green_threshold_pct=100.0,
        amber_threshold_pct=80.0,
        time_grain="monthly",
        trend_periods=12,
        org=org,
        created_by=orguser,
    )
    yield kpi
    try:
        kpi.refresh_from_db()
        kpi.delete()
    except KPI.DoesNotExist:
        pass


# ── RAG Computation Tests ──────────────────────────────────────────────────


class TestComputeRagStatus:
    def test_green_increase(self):
        assert compute_rag_status(100, 100, "increase", 100, 80) == "green"

    def test_green_increase_above_target(self):
        assert compute_rag_status(120, 100, "increase", 100, 80) == "green"

    def test_amber_increase(self):
        assert compute_rag_status(85, 100, "increase", 100, 80) == "amber"

    def test_red_increase(self):
        assert compute_rag_status(50, 100, "increase", 100, 80) == "red"

    def test_green_decrease(self):
        assert compute_rag_status(90, 100, "decrease", 100, 120) == "green"

    def test_amber_decrease(self):
        assert compute_rag_status(110, 100, "decrease", 100, 120) == "amber"

    def test_red_decrease(self):
        assert compute_rag_status(150, 100, "decrease", 100, 120) == "red"

    def test_null_target(self):
        assert compute_rag_status(100, None, "increase", 100, 80) is None

    def test_null_current(self):
        assert compute_rag_status(None, 100, "increase", 100, 80) is None

    def test_zero_target(self):
        assert compute_rag_status(100, 0, "increase", 100, 80) is None

    def test_boundary_amber_increase(self):
        """Exactly at amber threshold"""
        assert compute_rag_status(80, 100, "increase", 100, 80) == "amber"

    def test_boundary_green_increase(self):
        """Exactly at green threshold"""
        assert compute_rag_status(100, 100, "increase", 100, 80) == "green"

    def test_just_below_amber_increase(self):
        assert compute_rag_status(79.9, 100, "increase", 100, 80) == "red"


# ── CRUD Tests ──────────────────────────────────────────────────────────────


class TestKPICRUD:
    def test_create_kpi(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            target_value=500.0,
        )
        kpi = KPIService.create_kpi(payload, orguser)
        assert kpi.id is not None
        assert kpi.name == sample_metric.name  # defaults to metric name
        assert kpi.target_value == 500.0
        assert kpi.direction == "increase"
        kpi.delete()

    def test_create_kpi_custom_name(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            name="Custom KPI Name",
            direction="decrease",
            time_grain="quarterly",
        )
        kpi = KPIService.create_kpi(payload, orguser)
        assert kpi.name == "Custom KPI Name"
        assert kpi.direction == "decrease"
        assert kpi.time_grain == "quarterly"
        kpi.delete()

    def test_create_kpi_invalid_direction(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="sideways",
            time_grain="monthly",
        )
        with pytest.raises(KPIValidationError, match="Invalid direction"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_time_grain(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="hourly",
        )
        with pytest.raises(KPIValidationError, match="Invalid time_grain"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_metric(self, orguser, seed_db):
        from ddpui.services.metric_service import MetricNotFoundError

        payload = KPICreate(
            metric_id=99999,
            direction="increase",
            time_grain="monthly",
        )
        with pytest.raises(MetricNotFoundError):
            KPIService.create_kpi(payload, orguser)

    def test_get_kpi(self, orguser, org, sample_kpi, seed_db):
        kpi = KPIService.get_kpi(sample_kpi.id, org)
        assert kpi.id == sample_kpi.id
        assert kpi.metric is not None

    def test_get_kpi_not_found(self, org, seed_db):
        with pytest.raises(KPINotFoundError):
            KPIService.get_kpi(99999, org)

    def test_list_kpis(self, org, sample_kpi, seed_db):
        kpis, total = KPIService.list_kpis(org)
        assert total >= 1
        assert any(k.id == sample_kpi.id for k in kpis)

    def test_list_kpis_search(self, org, sample_kpi, seed_db):
        kpis, total = KPIService.list_kpis(org, search="Test")
        assert total >= 1

        kpis, total = KPIService.list_kpis(org, search="nonexistent_xyz")
        assert total == 0

    def test_update_kpi(self, orguser, org, sample_kpi, seed_db):
        payload = KPIUpdate(name="Updated KPI", target_value=2000.0)
        updated = KPIService.update_kpi(sample_kpi.id, org, orguser, payload)
        assert updated.name == "Updated KPI"
        assert updated.target_value == 2000.0

    def test_update_kpi_invalid_direction(self, orguser, org, sample_kpi, seed_db):
        payload = KPIUpdate(direction="sideways")
        with pytest.raises(KPIValidationError):
            KPIService.update_kpi(sample_kpi.id, org, orguser, payload)

    def test_delete_kpi(self, orguser, org, sample_kpi, seed_db):
        kpi_id = sample_kpi.id
        KPIService.delete_kpi(kpi_id, org, orguser)
        with pytest.raises(KPINotFoundError):
            KPIService.get_kpi(kpi_id, org)

    def test_delete_kpi_cleans_dashboard(self, orguser, org, sample_kpi, seed_db):
        dashboard = Dashboard.objects.create(
            title="Test Dashboard",
            org=org,
            created_by=orguser,
            components={
                "comp1": {"type": "kpi", "config": {"kpiId": sample_kpi.id}},
                "comp2": {"type": "chart", "config": {"chartId": 1}},
            },
            layout_config=[
                {"i": "comp1", "x": 0, "y": 0, "w": 4, "h": 2},
                {"i": "comp2", "x": 4, "y": 0, "w": 4, "h": 2},
            ],
        )

        KPIService.delete_kpi(sample_kpi.id, org, orguser)

        dashboard.refresh_from_db()
        assert "comp1" not in dashboard.components
        assert "comp2" in dashboard.components
        assert len(dashboard.layout_config) == 1
        dashboard.delete()


# ── Summary Tests ───────────────────────────────────────────────────────────


class TestKPISummary:
    def test_summary_no_warehouse(self, org, sample_kpi, seed_db):
        results = KPIService.get_kpi_summary(org)
        assert len(results) >= 1
        item = next(r for r in results if r["id"] == sample_kpi.id)
        assert item["current_value"] is None
        assert item["rag_status"] is None
        assert item["name"] == "Test KPI"

    @patch("ddpui.services.metric_service.MetricService.compute_metric_value")
    def test_summary_with_value(self, mock_compute, orguser, org, sample_kpi, seed_db):
        mock_compute.return_value = 900.0
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        results = KPIService.get_kpi_summary(org)
        item = next(r for r in results if r["id"] == sample_kpi.id)
        assert item["current_value"] == 900.0
        assert item["rag_status"] == "amber"  # 900/1000 = 90%, below 100% green, above 80% amber
        assert item["achievement_pct"] == 90.0

        OrgWarehouse.objects.filter(org=org).delete()


# ── Data Tests ──────────────────────────────────────────────────────────────


class TestKPIData:
    def test_data_no_warehouse(self, org, sample_kpi, seed_db):
        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"] == {}
        assert result["echarts_config"] == {}

    def test_data_not_found(self, org, seed_db):
        with pytest.raises(KPINotFoundError):
            KPIService.get_kpi_data(99999, org)

    @patch("ddpui.services.metric_service.MetricService.compute_metric_value")
    def test_data_with_value(self, mock_compute, orguser, org, sample_kpi, seed_db):
        mock_compute.return_value = 800.0
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"]["current_value"] == 800.0
        assert result["data"]["rag_status"] == "amber"
        assert result["data"]["target_value"] == 1000.0
        assert isinstance(result["echarts_config"], dict)

        OrgWarehouse.objects.filter(org=org).delete()
