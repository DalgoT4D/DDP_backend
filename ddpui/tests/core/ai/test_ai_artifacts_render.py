"""Artifacts saved by the AI tools must render on dashboards and in reports.

The AI tools persist through ChartCreate / KPICreate model_dump(), so optional
extra_config keys (customizations, filters, sort, pagination) are stored as
None rather than omitted — unlike UI-built charts, which always send
customizations. These tests save artifacts the same way and render them.
"""

import os
from datetime import date
from unittest.mock import patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.charts_api import get_chart_data_by_id
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import Metric
from ddpui.models.org import OrgWarehouse
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.chart_schemas.crud import ChartCreate
from ddpui.schemas.kpi_schema import KPICreate, KPIExtraConfig
from ddpui.services.chart_service import ChartData, ChartService
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    org_without_workspace,
    authuser,
    orguser,
    mock_request,
)

pytestmark = pytest.mark.django_db

CHARTS_SERVICE = "ddpui.api.charts_api.charts_service"

AI_CHART_PAYLOADS = {
    "bar": {
        "dimension_column": "region",
        "metrics": [{"column": "amount", "aggregation": "sum", "alias": "total"}],
    },
    "line": {
        "dimension_column": "region",
        "metrics": [{"column": "amount", "aggregation": "sum", "alias": "total"}],
    },
    "pie": {
        "dimension_column": "region",
        "metrics": [{"column": "amount", "aggregation": "sum", "alias": "total"}],
    },
    "number": {"metrics": [{"column": "amount", "aggregation": "sum", "alias": "total"}]},
}

WAREHOUSE_ROWS = {
    "bar": [{"region": "North", "total": 10}, {"region": "South", "total": 20}],
    "line": [{"region": "North", "total": 10}, {"region": "South", "total": 20}],
    "pie": [{"region": "North", "total": 10}, {"region": "South", "total": 20}],
    "number": [{"total": 30}],
}


@pytest.fixture
def org_warehouse(orguser):
    return OrgWarehouse.objects.create(org=orguser.org, wtype="postgres", name="wh")


def save_like_ai_tool(orguser, chart_type: str):
    """Mirror ddpui.core.ai.tools.chart_tools._save_chart."""
    payload = ChartCreate(
        title=f"AI {chart_type}",
        chart_type=chart_type,
        schema_name="public",
        table_name="orders",
        extra_config=AI_CHART_PAYLOADS[chart_type],
    )
    return ChartService.create_chart(
        ChartData(
            title=payload.title,
            description=payload.description,
            chart_type=payload.chart_type,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            extra_config=payload.extra_config.model_dump(),
        ),
        orguser,
    )


@pytest.mark.parametrize("chart_type", ["bar", "line", "pie", "number"])
def test_ai_chart_renders_on_dashboard(orguser, org_warehouse, seed_db, chart_type):
    chart = save_like_ai_tool(orguser, chart_type)
    assert chart.extra_config["customizations"] is None

    with patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client"), patch(
        f"{CHARTS_SERVICE}.get_warehouse_client"
    ), patch(f"{CHARTS_SERVICE}.build_chart_query"), patch(
        f"{CHARTS_SERVICE}.execute_chart_query", return_value=WAREHOUSE_ROWS[chart_type]
    ):
        response = get_chart_data_by_id(mock_request(orguser), chart.id)

    assert response.echarts_config


def dashboard_with(orguser, components: dict) -> Dashboard:
    return Dashboard.objects.create(
        title="AI Dashboard",
        dashboard_type="native",
        grid_columns=12,
        tabs=[{"id": "tab-1", "title": "T", "layout_config": [], "components": components}],
        created_by=orguser,
        org=orguser.org,
    )


def snapshot_of(orguser, dashboard):
    return ReportService.create_snapshot(
        title="AI Report",
        dashboard_id=dashboard.id,
        orguser=orguser,
        date_column={},
        period_start=None,
        period_end=date(2025, 1, 31),
    )


@pytest.mark.parametrize("chart_type", ["bar", "line", "pie", "number"])
def test_ai_chart_renders_in_report(orguser, org_warehouse, seed_db, chart_type):
    chart = save_like_ai_tool(orguser, chart_type)
    dashboard = dashboard_with(
        orguser, {f"chart-{chart.id}": {"type": "chart", "config": {"chartId": chart.id}}}
    )
    snapshot = snapshot_of(orguser, dashboard)

    with patch(f"{CHARTS_SERVICE}.get_warehouse_client"), patch(
        f"{CHARTS_SERVICE}.build_chart_query"
    ), patch(f"{CHARTS_SERVICE}.execute_chart_query", return_value=WAREHOUSE_ROWS[chart_type]):
        result = ReportService.get_report_chart_data(snapshot.id, chart.id, orguser.org)

    assert result["echarts_config"]


@pytest.fixture
def ai_kpi(orguser):
    """Mirror ddpui.core.ai.tools.metric_tools.create_kpi."""
    metric = Metric.objects.create(
        name="Revenue",
        schema_name="public",
        table_name="orders",
        column="amount",
        aggregation="sum",
        org=orguser.org,
        created_by=orguser,
    )
    kpi = KPIService.create_kpi(
        KPICreate(
            metric_id=metric.id,
            direction="increase",
            time_grain="monthly",
            time_dimension_column="created_at",
            extra_config=KPIExtraConfig(),
        ),
        orguser,
    )
    yield kpi
    ReportSnapshot.objects.filter(org=orguser.org).delete()
    kpi.delete()
    metric.delete()


PERIODS = [{"period": "2025-01", "value": 30}]


def test_ai_kpi_renders_live(orguser, org_warehouse, ai_kpi):
    with patch.object(KPIService, "_compute_trend", return_value=PERIODS):
        result = KPIService.compute_kpi_data(KPIService.kpi_to_response(ai_kpi), orguser.org)

    assert result["data"]["current_value"] == 30
    assert result["data"]["customizations"] is None


def test_ai_kpi_renders_in_report(orguser, org_warehouse, ai_kpi):
    dashboard = dashboard_with(
        orguser, {f"kpi-{ai_kpi.id}": {"type": "kpi", "config": {"kpiId": ai_kpi.id}}}
    )
    with patch.object(KPIService, "_compute_trend", return_value=PERIODS):
        snapshot = snapshot_of(orguser, dashboard)
        result = ReportService.get_report_kpi_data(snapshot.id, ai_kpi.id, orguser.org)

    assert result["data"]["current_value"] == 30
