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
from ddpui.auth import ACCOUNT_MANAGER_ROLE, ANALYST_ROLE
from ddpui.schemas.kpi_schema import KPICreate, KPIUpdate, KPIExtraConfig
from ddpui.core.kpi.kpi_service import (
    KPIService,
    KPINotFoundError,
    KPIValidationError,
    KPIPermissionError,
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
def analyst_orguser(org):
    """A second user in the same org with the (non-admin) analyst role."""
    user = User.objects.create(username="kpisvcanalyst", email="kpisvcanalyst@test.com")
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ANALYST_ROLE).first(),
    )
    yield orguser
    orguser.delete()
    user.delete()


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
            extra_config=KPIExtraConfig(),
        )
        kpi = KPIService.create_kpi(payload, orguser)
        assert kpi.id is not None
        assert kpi.name == sample_metric.name  # defaults to metric name
        assert kpi.target_value == 500.0
        assert kpi.direction == "increase"
        # extra_config defaults to {} on the model — never null
        assert kpi.extra_config == {"customizations": None}
        kpi.delete()

    def test_create_kpi_with_customizations(self, orguser, sample_metric, seed_db):
        """v1.1: KPICreate accepts and persists number-format customizations."""
        from ddpui.schemas.chart_schemas.customizations import NumberChartCustomizations

        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            target_value=500.0,
            extra_config=KPIExtraConfig(
                customizations=NumberChartCustomizations(
                    numberFormat="indian",
                    decimalPlaces=0,
                    numberPrefix="₹",
                    numberSuffix="",
                )
            ),
        )
        kpi = KPIService.create_kpi(payload, orguser)
        c = kpi.extra_config["customizations"]
        assert c["numberFormat"] == "indian"
        assert c["decimalPlaces"] == 0
        assert c["numberPrefix"] == "₹"

        # Round-trip through kpi_to_response — customizations survives the
        # Pydantic → dict → Pydantic conversion
        response = KPIService.kpi_to_response(kpi)
        assert response.extra_config.customizations.numberFormat == "indian"
        assert response.extra_config.customizations.numberPrefix == "₹"
        kpi.delete()

    def test_update_kpi_replaces_customizations(self, orguser, org, sample_kpi, seed_db):
        """Updating a KPI with a new customizations payload replaces the stored config."""
        from ddpui.schemas.chart_schemas.customizations import NumberChartCustomizations

        payload = KPIUpdate(
            name="Updated",
            extra_config=KPIExtraConfig(
                customizations=NumberChartCustomizations(
                    numberFormat="adaptive_indian", decimalPlaces=2
                )
            ),
        )
        updated = KPIService.update_kpi(sample_kpi.id, org, orguser, payload)
        assert updated.extra_config["customizations"]["numberFormat"] == "adaptive_indian"
        assert updated.extra_config["customizations"]["decimalPlaces"] == 2

    def test_create_kpi_custom_name(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            name="Custom KPI Name",
            direction="decrease",
            time_grain="quarterly",
            green_threshold_pct=80.0,
            amber_threshold_pct=110.0,
            extra_config=KPIExtraConfig(),
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
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(KPIValidationError, match="Invalid direction"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_time_grain(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="hourly",
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(KPIValidationError, match="Invalid time_grain"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_thresholds_increase(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="increase",
            time_grain="monthly",
            green_threshold_pct=50.0,
            amber_threshold_pct=80.0,
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(KPIValidationError, match="green_threshold_pct"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_thresholds_decrease(self, orguser, sample_metric, seed_db):
        payload = KPICreate(
            metric_id=sample_metric.id,
            direction="decrease",
            time_grain="monthly",
            green_threshold_pct=80.0,
            amber_threshold_pct=50.0,
            extra_config=KPIExtraConfig(),
        )
        with pytest.raises(KPIValidationError, match="amber_threshold_pct"):
            KPIService.create_kpi(payload, orguser)

    def test_create_kpi_invalid_metric(self, orguser, seed_db):
        from ddpui.core.metric.metric_service import MetricNotFoundError

        payload = KPICreate(
            metric_id=99999,
            direction="increase",
            time_grain="monthly",
            extra_config=KPIExtraConfig(),
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

    def test_list_kpis_search_by_name(self, org, sample_kpi, seed_db):
        _, total = KPIService.list_kpis(org, search="Test")
        assert total >= 1

        _, total = KPIService.list_kpis(org, search="nonexistent_xyz")
        assert total == 0

    def test_list_kpis_search_by_program_tag(self, orguser, org, sample_metric, seed_db):
        """Search should match program_tags as well as name."""
        kpi = KPI.objects.create(
            name="Enrollment KPI",
            metric=sample_metric,
            direction="increase",
            time_grain="monthly",
            program_tags=["Health Program", "Education"],
            org=org,
            created_by=orguser,
        )
        kpis, total = KPIService.list_kpis(org, search="Health")
        assert total >= 1
        assert any(k.id == kpi.id for k in kpis)

        # Search by name still works
        _, total = KPIService.list_kpis(org, search="Enrollment")
        assert total >= 1

        kpi.delete()

    def test_update_kpi(self, orguser, org, sample_kpi, seed_db):
        payload = KPIUpdate(name="Updated KPI", target_value=2000.0, extra_config=KPIExtraConfig())
        updated = KPIService.update_kpi(sample_kpi.id, org, orguser, payload)
        assert updated.name == "Updated KPI"
        assert updated.target_value == 2000.0

    def test_update_kpi_invalid_direction(self, orguser, org, sample_kpi, seed_db):
        payload = KPIUpdate(direction="sideways", extra_config=KPIExtraConfig())
        with pytest.raises(KPIValidationError):
            KPIService.update_kpi(sample_kpi.id, org, orguser, payload)

    def test_delete_kpi(self, orguser, org, sample_kpi, seed_db):
        kpi_id = sample_kpi.id
        KPIService.delete_kpi(kpi_id, org, orguser)
        with pytest.raises(KPINotFoundError):
            KPIService.get_kpi(kpi_id, org)

    def test_delete_kpi_non_owner_analyst_denied(self, analyst_orguser, org, sample_kpi, seed_db):
        """A non-admin who didn't create the KPI cannot delete it."""
        with pytest.raises(KPIPermissionError):
            KPIService.delete_kpi(sample_kpi.id, org, analyst_orguser)
        assert KPI.objects.filter(id=sample_kpi.id).exists()

    def test_delete_kpi_non_admin_creator_can_delete_own(
        self, analyst_orguser, org, sample_metric, seed_db
    ):
        """A non-admin (analyst) can delete a KPI they created."""
        kpi = KPI.objects.create(
            name="Analyst KPI",
            metric=sample_metric,
            target_value=1000.0,
            direction="increase",
            time_grain="monthly",
            org=org,
            created_by=analyst_orguser,
        )
        KPIService.delete_kpi(kpi.id, org, analyst_orguser)
        assert not KPI.objects.filter(id=kpi.id).exists()

    def test_get_kpi_dashboards(self, orguser, org, sample_kpi, seed_db):
        dashboard = Dashboard.objects.create(
            title="Test Dashboard",
            org=org,
            created_by=orguser,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "comp1": {"type": "kpi", "config": {"kpiId": sample_kpi.id}},
                    },
                }
            ],
        )

        result = KPIService.get_kpi_dashboards(sample_kpi.id, org)
        assert len(result) == 1
        assert result[0]["id"] == dashboard.id
        assert result[0]["title"] == "Test Dashboard"
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

    @patch("ddpui.core.kpi.kpi_service.KPIService._compute_trend")
    def test_summary_with_trend(self, mock_trend, orguser, org, sample_kpi, seed_db):
        """Current value comes from last trend period."""
        mock_trend.return_value = [
            {"period": "Jan 2026", "value": 800.0},
            {"period": "Feb 2026", "value": 900.0},
        ]
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        results = KPIService.get_kpi_summary(org)
        item = next(r for r in results if r["id"] == sample_kpi.id)
        assert item["current_value"] == 900.0
        assert item["rag_status"] == "amber"  # 900/1000 = 90%
        assert item["achievement_pct"] == 90.0
        assert item["period_over_period_change"] == 12.5  # (900-800)/800 * 100

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

    @patch("ddpui.core.kpi.kpi_service.KPIService._compute_trend")
    def test_data_current_value_from_trend(self, mock_trend, orguser, org, sample_kpi, seed_db):
        """Current value is the last trend period's value."""
        mock_trend.return_value = [
            {"period": "Jan 2026", "value": 700.0},
            {"period": "Feb 2026", "value": 800.0},
        ]
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"]["current_value"] == 800.0
        assert result["data"]["rag_status"] == "amber"
        assert result["data"]["target_value"] == 1000.0
        assert result["data"]["data_last_date"] == "Feb 2026"
        assert len(result["data"]["periods"]) == 2
        assert isinstance(result["echarts_config"], dict)

        OrgWarehouse.objects.filter(org=org).delete()

    @patch("ddpui.core.kpi.kpi_service.KPIService._compute_trend")
    def test_data_no_trend_no_value(self, mock_trend, orguser, org, sample_kpi, seed_db):
        """No trend → current_value is None, data_last_date is None, empty echarts."""
        mock_trend.return_value = []
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"]["current_value"] is None
        assert result["data"]["data_last_date"] is None
        assert result["data"]["periods"] == []
        assert result["echarts_config"] == {}

        OrgWarehouse.objects.filter(org=org).delete()

    @patch("ddpui.core.kpi.kpi_service.KPIService._compute_trend")
    def test_data_rag_green(self, mock_trend, orguser, org, sample_kpi, seed_db):
        """Value >= target → green."""
        mock_trend.return_value = [{"period": "Mar 2026", "value": 1200.0}]
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"]["current_value"] == 1200.0
        assert result["data"]["rag_status"] == "green"

        OrgWarehouse.objects.filter(org=org).delete()

    @patch("ddpui.core.kpi.kpi_service.KPIService._compute_trend")
    def test_data_rag_red(self, mock_trend, orguser, org, sample_kpi, seed_db):
        """Value far below target → red."""
        mock_trend.return_value = [{"period": "Mar 2026", "value": 500.0}]
        OrgWarehouse.objects.create(org=org, wtype="postgres", credentials={})

        result = KPIService.get_kpi_data(sample_kpi.id, org)
        assert result["data"]["current_value"] == 500.0
        assert result["data"]["rag_status"] == "red"


# ── Annotation Tests ──────────────────────────────────────────────────


class TestAnnotations:
    def _clear_annotations(self, sample_kpi):
        sample_kpi.annotations = []
        sample_kpi.save(update_fields=["annotations"])

    def test_list_empty(self, org, sample_kpi, seed_db):
        self._clear_annotations(sample_kpi)
        results = KPIService.list_annotations(sample_kpi.id, org)
        assert results == []

    def test_create_annotation(self, orguser, org, sample_kpi, seed_db):
        from ddpui.schemas.kpi_schema import AnnotationEntryCreate

        self._clear_annotations(sample_kpi)
        payload = AnnotationEntryCreate(
            note_type="beneficiary_quote",
            period_key="Jan 01, 2026",
            period_date="2026-01-01",
            content="Enrollment peaked after the campaign.",
            snapshot_value=2847.0,
            snapshot_pop_change=8.3,
        )
        result = KPIService.create_annotation(sample_kpi.id, org, orguser, payload)
        assert result.id is not None
        assert result.note_type == "beneficiary_quote"
        assert result.period_key == "Jan 01, 2026"
        assert result.content == "Enrollment peaked after the campaign."
        assert result.snapshot_value == 2847.0
        assert result.snapshot_pop_change == 8.3
        assert result.created_by_email == orguser.user.email

        self._clear_annotations(sample_kpi)

    def test_create_and_list(self, orguser, org, sample_kpi, seed_db):
        from ddpui.schemas.kpi_schema import AnnotationEntryCreate

        self._clear_annotations(sample_kpi)
        payload = AnnotationEntryCreate(
            note_type="note",
            period_key="Feb 01, 2026",
            content="Post-holiday dip expected.",
        )
        created = KPIService.create_annotation(sample_kpi.id, org, orguser, payload)
        results = KPIService.list_annotations(sample_kpi.id, org)
        assert len(results) >= 1
        assert any(r.id == created.id for r in results)

        self._clear_annotations(sample_kpi)

    def test_update_annotation(self, orguser, org, sample_kpi, seed_db):
        from ddpui.schemas.kpi_schema import AnnotationEntryCreate, AnnotationEntryUpdate

        self._clear_annotations(sample_kpi)
        create_payload = AnnotationEntryCreate(
            note_type="note",
            period_key="Mar 01, 2026",
            content="Original note.",
        )
        created = KPIService.create_annotation(sample_kpi.id, org, orguser, create_payload)

        update_payload = AnnotationEntryUpdate(
            content="Updated note content.",
            note_type="beneficiary_quote",
        )
        updated = KPIService.update_annotation(
            sample_kpi.id, created.id, org, orguser, update_payload
        )
        assert updated.content == "Updated note content."
        assert updated.note_type == "beneficiary_quote"

        self._clear_annotations(sample_kpi)

    def test_delete_annotation(self, orguser, org, sample_kpi, seed_db):
        from ddpui.schemas.kpi_schema import AnnotationEntryCreate

        self._clear_annotations(sample_kpi)
        payload = AnnotationEntryCreate(
            note_type="note",
            period_key="Apr 01, 2026",
            content="To be deleted.",
        )
        created = KPIService.create_annotation(sample_kpi.id, org, orguser, payload)

        sample_kpi.refresh_from_db()
        assert len(sample_kpi.annotations) == 1

        KPIService.delete_annotation(sample_kpi.id, created.id, org, orguser)

        sample_kpi.refresh_from_db()
        assert len(sample_kpi.annotations) == 0

    def test_delete_nonexistent(self, orguser, org, sample_kpi, seed_db):
        with pytest.raises(KPINotFoundError):
            KPIService.delete_annotation(sample_kpi.id, 99999, org, orguser)

    def test_period_date_stored(self, orguser, org, sample_kpi, seed_db):
        from ddpui.schemas.kpi_schema import AnnotationEntryCreate

        self._clear_annotations(sample_kpi)
        payload = AnnotationEntryCreate(
            note_type="note",
            period_key="May 01, 2026",
            period_date="2026-05-01",
            content="Testing date storage.",
        )
        created = KPIService.create_annotation(sample_kpi.id, org, orguser, payload)
        assert created.period_date == "2026-05-01"

        sample_kpi.refresh_from_db()
        entry = next(e for e in sample_kpi.annotations if e["id"] == created.id)
        assert entry["period_date"] == "2026-05-01"

        self._clear_annotations(sample_kpi)

        OrgWarehouse.objects.filter(org=org).delete()
