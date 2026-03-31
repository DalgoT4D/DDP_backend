"""Tests for populate_from_extra_config helper and its integration with endpoints.

Covers:
1. Unit tests for populate_from_extra_config — field population, no-override, edge cases
2. Integration tests — verifying endpoints call populate_from_extra_config correctly
3. Chart-type-specific tests — bar/line with time_grain, table with dimensions, map
4. Public report endpoint integration
"""

import os
import json
import django
from unittest.mock import patch, MagicMock, call
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.test import RequestFactory
from ninja.errors import HttpError

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.report import ReportSnapshot
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from datetime import date
from ddpui.api.charts_api import (
    populate_from_extra_config,
    get_chart_data,
    get_chart_data_preview,
    get_chart_data_preview_total_rows,
    get_chart_data_by_id,
)
from ddpui.api.public_api import (
    get_public_report_chart_data,
    get_public_report_table_data,
    get_public_report_table_total_rows,
    get_public_report_map_data,
)
from ddpui.schemas.chart_schema import ChartDataPayload
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.core.reports.report_service import ReportService

pytestmark = pytest.mark.django_db

rf = RequestFactory()


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="populateuser", email="populateuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Populate Test Org",
        slug="populate-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def org_warehouse(org):
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="Test Warehouse",
        airbyte_destination_id="dest-id",
    )
    yield warehouse
    warehouse.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


FULL_EXTRA_CONFIG = {
    "x_axis_column": "date",
    "y_axis_column": "revenue",
    "dimension_column": "region",
    "extra_dimension_column": "product",
    "dimension_columns": ["region", "product"],
    "metrics": [{"column": "revenue", "aggregation": "sum", "alias": "total_revenue"}],
    "geographic_column": "country",
    "value_column": "gdp",
    "selected_geojson_id": 42,
    "customizations": {"title": "My Chart", "colors": ["#ff0000"]},
    "time_grain": "month",
    "time_grain_option": "month",
    "filters": [{"column": "status", "operator": "equals", "value": "active"}],
    "pagination": {"page": 0, "limit": 50},
    "sort": [{"column": "date", "direction": "desc"}],
}


def _make_public_request(body=None):
    if body:
        request = rf.post(
            "/api/v1/public/reports/",
            data=json.dumps(body),
            content_type="application/json",
        )
    else:
        request = rf.get("/api/v1/public/reports/")
    request.META["REMOTE_ADDR"] = "127.0.0.1"
    request.META["HTTP_USER_AGENT"] = "TestAgent"
    return request


# ================================================================================
# 1. Unit Tests for populate_from_extra_config
# ================================================================================


class TestPopulateFromExtraConfig:
    """Pure unit tests — no DB, no mocks."""

    def test_fills_all_fields_from_extra_config(self):
        """When all top-level fields are None, they should be populated from extra_config."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config=FULL_EXTRA_CONFIG,
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis == "date"
        assert result.y_axis == "revenue"
        assert result.dimension_col == "region"
        assert result.extra_dimension == "product"
        assert result.dimensions == ["region", "product"]
        assert result.metrics == FULL_EXTRA_CONFIG["metrics"]
        assert result.geographic_column == "country"
        assert result.value_column == "gdp"
        assert result.selected_geojson_id == 42
        assert result.customizations == {"title": "My Chart", "colors": ["#ff0000"]}

    def test_never_overrides_explicit_values(self):
        """Explicitly set fields must NOT be overridden by extra_config values."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            x_axis="explicit_x",
            y_axis="explicit_y",
            dimension_col="explicit_dim",
            extra_dimension="explicit_extra_dim",
            dimensions=["explicit_col_a"],
            metrics=[{"column": "count_col", "aggregation": "count", "alias": "cnt"}],
            geographic_column="explicit_geo",
            value_column="explicit_val",
            selected_geojson_id=99,
            customizations={"title": "Explicit Title"},
            extra_config=FULL_EXTRA_CONFIG,
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis == "explicit_x"
        assert result.y_axis == "explicit_y"
        assert result.dimension_col == "explicit_dim"
        assert result.extra_dimension == "explicit_extra_dim"
        assert result.dimensions == ["explicit_col_a"]
        assert result.metrics == [{"column": "count_col", "aggregation": "count", "alias": "cnt"}]
        assert result.geographic_column == "explicit_geo"
        assert result.value_column == "explicit_val"
        assert result.selected_geojson_id == 99
        assert result.customizations == {"title": "Explicit Title"}

    def test_none_extra_config_is_safe(self):
        """Calling with extra_config=None should not crash; fields stay None."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config=None,
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis is None
        assert result.y_axis is None
        assert result.dimension_col is None
        assert result.metrics is None
        assert result.customizations is not None  # defaults to {}

    def test_empty_extra_config_is_safe(self):
        """Calling with extra_config={} should not crash; fields stay None."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="orders",
            extra_config={},
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis is None
        assert result.dimension_col is None
        assert result.customizations == {}

    def test_partial_extra_config(self):
        """Only fields present in extra_config are filled; others stay None."""
        payload = ChartDataPayload(
            chart_type="pie",
            schema_name="public",
            table_name="orders",
            extra_config={
                "dimension_column": "category",
                "metrics": [{"column": "amount", "aggregation": "sum", "alias": "total"}],
            },
        )

        result = populate_from_extra_config(payload)

        assert result.dimension_col == "category"
        assert result.metrics == [{"column": "amount", "aggregation": "sum", "alias": "total"}]
        assert result.x_axis is None
        assert result.y_axis is None
        assert result.geographic_column is None

    def test_returns_same_payload_object(self):
        """The function mutates and returns the same object (not a copy)."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config={"x_axis_column": "date"},
        )

        result = populate_from_extra_config(payload)

        assert result is payload

    def test_preserves_extra_config_passthrough(self):
        """extra_config itself must still be available after populate (for time_grain, filters, etc.)."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="orders",
            extra_config=FULL_EXTRA_CONFIG,
        )

        result = populate_from_extra_config(payload)

        assert result.extra_config["time_grain"] == "month"
        assert result.extra_config["filters"] == FULL_EXTRA_CONFIG["filters"]
        assert result.extra_config["pagination"] == FULL_EXTRA_CONFIG["pagination"]
        assert result.extra_config["sort"] == FULL_EXTRA_CONFIG["sort"]


# ================================================================================
# 2. Chart-Type-Specific Scenario Tests
# ================================================================================


class TestTimeGrainScenario:
    """Reproduces the original bug: time_grain in extra_config must reach the backend."""

    def test_minimal_payload_preserves_time_grain(self):
        """A report-style minimal payload (only extra_config) should carry time_grain through."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config={
                "x_axis_column": "order_date",
                "y_axis_column": "amount",
                "time_grain": "month",
                "time_grain_option": "month",
            },
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis == "order_date"
        assert result.y_axis == "amount"
        assert result.extra_config["time_grain"] == "month"

    def test_full_payload_still_preserves_time_grain(self):
        """A dashboard-style full payload (everything set) should also keep time_grain."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="sales",
            x_axis="order_date",
            y_axis="amount",
            extra_config={
                "x_axis_column": "order_date",
                "y_axis_column": "amount",
                "time_grain": "month",
            },
        )

        result = populate_from_extra_config(payload)

        assert result.x_axis == "order_date"
        assert result.extra_config["time_grain"] == "month"


class TestTableDrillDownScenario:
    """Table charts with drill-down set dimensions explicitly; auto-populate must not override."""

    def test_explicit_dimensions_preserved(self):
        """Frontend sends dimensions for current drill-down level; must not be overridden."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="sales",
            dimensions=["product_category"],
            extra_config={
                "dimension_columns": ["region", "product_category", "sku"],
                "metrics": [{"column": "revenue", "aggregation": "sum", "alias": "total"}],
            },
        )

        result = populate_from_extra_config(payload)

        assert result.dimensions == ["product_category"]
        assert result.metrics == [{"column": "revenue", "aggregation": "sum", "alias": "total"}]

    def test_no_dimensions_set_uses_extra_config(self):
        """When dimensions is None, auto-populate fills from dimension_columns."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="sales",
            extra_config={
                "dimension_columns": ["region", "product_category"],
            },
        )

        result = populate_from_extra_config(payload)

        assert result.dimensions == ["region", "product_category"]


class TestMapScenario:
    """Map charts need geographic_column, value_column, and full extra_config."""

    def test_minimal_map_payload_populated(self):
        """Map chart with only extra_config should get geographic/value columns filled."""
        payload = ChartDataPayload(
            chart_type="map",
            schema_name="public",
            table_name="country_data",
            extra_config={
                "geographic_column": "country_name",
                "value_column": "population",
                "selected_geojson_id": 5,
                "aggregate_function": "sum",
                "filters": [{"column": "year", "operator": "equals", "value": "2024"}],
            },
        )

        result = populate_from_extra_config(payload)

        assert result.geographic_column == "country_name"
        assert result.value_column == "population"
        assert result.selected_geojson_id == 5
        assert result.extra_config["filters"][0]["value"] == "2024"

    def test_map_with_explicit_dimension_col_not_overridden(self):
        """Map endpoints set dimension_col=geographic_column explicitly; must be preserved."""
        payload = ChartDataPayload(
            chart_type="map",
            schema_name="public",
            table_name="country_data",
            dimension_col="country_name",
            metrics=[{"column": "population", "aggregation": "sum", "alias": "value"}],
            extra_config={
                "dimension_column": "state_name",
                "geographic_column": "country_name",
            },
        )

        result = populate_from_extra_config(payload)

        assert result.dimension_col == "country_name"
        assert result.metrics == [{"column": "population", "aggregation": "sum", "alias": "value"}]


# ================================================================================
# 3. Integration: get_chart_data endpoint calls populate_from_extra_config
# ================================================================================


class TestGetChartDataIntegration:
    """Verify that POST /chart-data/ auto-populates fields from extra_config."""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_minimal_payload_gets_populated(self, mock_generate, orguser, org_warehouse, seed_db):
        """A minimal payload with only extra_config should work — backend fills the rest."""
        mock_generate.return_value = {
            "data": {"categories": ["Sep 2022", "Oct 2022"], "values": [1200, 2500]},
            "echarts_config": {"type": "line"},
        }

        request = mock_request(orguser)
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config={
                "x_axis_column": "order_date",
                "y_axis_column": "revenue",
                "time_grain": "month",
            },
        )

        response = get_chart_data(request, payload)

        called_payload = mock_generate.call_args[0][0]
        assert called_payload.x_axis == "order_date"
        assert called_payload.y_axis == "revenue"
        assert called_payload.extra_config["time_grain"] == "month"
        assert response.data["categories"] == ["Sep 2022", "Oct 2022"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_full_payload_unchanged(self, mock_generate, orguser, org_warehouse, seed_db):
        """A full payload (like dashboard/creation) should pass through without changes."""
        mock_generate.return_value = {
            "data": {"values": [10]},
            "echarts_config": {"type": "bar"},
        }

        request = mock_request(orguser)
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="sales",
            x_axis="category",
            dimension_col="region",
            metrics=[{"column": "revenue", "aggregation": "sum", "alias": "total"}],
            extra_config={
                "x_axis_column": "different_col",
                "dimension_column": "different_dim",
            },
        )

        get_chart_data(request, payload)

        called_payload = mock_generate.call_args[0][0]
        assert called_payload.x_axis == "category"
        assert called_payload.dimension_col == "region"


# ================================================================================
# 4. Integration: get_chart_data_by_id uses populate_from_extra_config
# ================================================================================


class TestGetChartDataByIdIntegration:
    """Verify the GET endpoint for dashboard charts uses the simplified helper."""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    @patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client")
    def test_chart_with_time_grain(
        self, mock_wh_client, mock_generate, orguser, org, org_warehouse, seed_db
    ):
        """Chart with time_grain in extra_config should have it in the payload."""
        chart = Chart.objects.create(
            title="Monthly Revenue",
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config={
                "x_axis_column": "order_date",
                "y_axis_column": "revenue",
                "time_grain": "month",
                "customizations": {},
            },
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        mock_generate.return_value = {
            "data": {"categories": ["Sep", "Oct"], "values": [100, 200]},
            "echarts_config": {"type": "line"},
        }
        mock_wh_client.return_value = MagicMock()

        request = mock_request(orguser)
        response = get_chart_data_by_id(request, chart.id)

        called_payload = mock_generate.call_args[0][0]
        assert called_payload.x_axis == "order_date"
        assert called_payload.y_axis == "revenue"
        assert called_payload.extra_config["time_grain"] == "month"
        assert called_payload.chart_type == "line"

        chart.delete()

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    @patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client")
    def test_chart_title_in_customizations(
        self, mock_wh_client, mock_generate, orguser, org, org_warehouse, seed_db
    ):
        """Chart title should be injected into customizations by get_chart_data_by_id."""
        chart = Chart.objects.create(
            title="Revenue Trend",
            chart_type="bar",
            schema_name="public",
            table_name="sales",
            extra_config={
                "dimension_column": "category",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
                "customizations": {"colors": ["blue"]},
            },
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        mock_generate.return_value = {
            "data": {},
            "echarts_config": {},
        }
        mock_wh_client.return_value = MagicMock()

        request = mock_request(orguser)
        get_chart_data_by_id(request, chart.id)

        called_payload = mock_generate.call_args[0][0]
        assert called_payload.customizations["title"] == "Revenue Trend"
        assert called_payload.customizations["colors"] == ["blue"]
        assert called_payload.dimension_col == "category"

        chart.delete()


# ================================================================================
# 5. Integration: Chart data preview endpoints
# ================================================================================


class TestChartDataPreviewIntegration:
    @patch("ddpui.api.charts_api.charts_service.get_chart_data_table_preview")
    def test_preview_populates_from_extra_config(
        self, mock_preview, orguser, org_warehouse, seed_db
    ):
        """POST /chart-data-preview/ should auto-populate fields from extra_config."""
        mock_preview.return_value = {
            "columns": ["region", "revenue"],
            "column_types": {"region": "str", "revenue": "float"},
            "data": [{"region": "US", "revenue": 1000}],
            "page": 0,
            "limit": 100,
        }

        request = mock_request(orguser)
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="sales",
            extra_config={
                "dimension_columns": ["region"],
                "metrics": [{"column": "revenue", "aggregation": "sum", "alias": "revenue"}],
            },
        )

        get_chart_data_preview(request, payload)

        called_payload = mock_preview.call_args[0][1]
        assert called_payload.dimensions == ["region"]
        assert called_payload.metrics is not None

    @patch("ddpui.api.charts_api.charts_service.get_chart_data_total_rows")
    def test_total_rows_populates_from_extra_config(
        self, mock_total, orguser, org_warehouse, seed_db
    ):
        """POST /chart-data-preview/total-rows/ should auto-populate fields."""
        mock_total.return_value = 42

        request = mock_request(orguser)
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="sales",
            extra_config={
                "dimension_columns": ["region"],
                "metrics": [{"column": "revenue", "aggregation": "sum", "alias": "revenue"}],
            },
        )

        result = get_chart_data_preview_total_rows(request, payload)

        called_payload = mock_total.call_args[0][1]
        assert called_payload.dimensions == ["region"]


# ================================================================================
# 6. Integration: Public report endpoints
# ================================================================================


@pytest.fixture
def sample_dashboard_for_report(orguser, org):
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": 1, "chartType": "bar", "title": "Bar"},
            }
        },
        created_by=orguser,
        org=org,
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


@pytest.fixture
def sample_filter_for_report(sample_dashboard_for_report):
    f = DashboardFilter.objects.create(
        dashboard=sample_dashboard_for_report,
        name="Date Filter",
        filter_type="datetime",
        schema_name="public",
        table_name="orders",
        column_name="created_at",
        settings={},
        order=0,
    )
    yield f
    try:
        f.refresh_from_db()
        f.delete()
    except DashboardFilter.DoesNotExist:
        pass


@pytest.fixture
def sample_chart_for_report(orguser, org):
    chart = Chart.objects.create(
        title="Test Chart",
        chart_type="bar",
        schema_name="public",
        table_name="orders",
        extra_config={
            "dimension_column": "category",
            "metrics": [{"column": "amount", "aggregation": "sum"}],
            "time_grain": "month",
        },
        created_by=orguser,
        org=org,
    )
    yield chart
    try:
        chart.refresh_from_db()
        chart.delete()
    except Chart.DoesNotExist:
        pass


@pytest.fixture
def public_snapshot(
    orguser, org, sample_dashboard_for_report, sample_filter_for_report, sample_chart_for_report
):
    from ddpui.api.report_api import toggle_report_sharing
    from ddpui.schemas.dashboard_schema import ShareToggle

    snapshot = ReportService.create_snapshot(
        title="Public Report",
        dashboard_id=sample_dashboard_for_report.id,
        date_column={
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "created_at",
        },
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 31),
        orguser=orguser,
    )

    request = mock_request(orguser)
    toggle_report_sharing(request, snapshot.id, ShareToggle(is_public=True))
    snapshot.refresh_from_db()

    yield snapshot
    try:
        snapshot.refresh_from_db()
        snapshot.delete()
    except ReportSnapshot.DoesNotExist:
        pass


class TestPublicReportChartDataPopulate:
    """Verify public report endpoints call populate_from_extra_config."""

    def test_minimal_payload_auto_populated(self, public_snapshot, seed_db):
        """Public chart-data endpoint should auto-populate from extra_config."""
        mock_result = {
            "data": [{"x": "Jan", "y": 100}],
            "echarts_config": {"type": "bar"},
        }

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.charts_api.generate_chart_data_and_config"
        ) as mock_gen:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_gen.return_value = mock_result

            request = _make_public_request(
                body={
                    "chart_type": "bar",
                    "schema_name": "public",
                    "table_name": "orders",
                    "extra_config": {
                        "dimension_column": "category",
                        "metrics": [{"column": "amount", "aggregation": "sum"}],
                        "time_grain": "month",
                    },
                }
            )
            response = get_public_report_chart_data(request, public_snapshot.public_share_token)

            called_payload = mock_gen.call_args[0][0]
            assert called_payload.dimension_col == "category"
            assert called_payload.extra_config["time_grain"] == "month"
            assert response["is_valid"] is True

    def test_table_data_auto_populated(self, public_snapshot, seed_db):
        """Public table-data endpoint should auto-populate dimensions from extra_config."""
        mock_preview = {
            "columns": ["region"],
            "column_types": {"region": "str"},
            "data": [{"region": "US"}],
            "page": 0,
            "limit": 100,
        }

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_table_preview"
        ) as mock_fn:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_fn.return_value = mock_preview

            request = _make_public_request(
                body={
                    "chart_type": "table",
                    "schema_name": "public",
                    "table_name": "orders",
                    "extra_config": {
                        "dimension_columns": ["region"],
                    },
                }
            )
            response = get_public_report_table_data(request, public_snapshot.public_share_token)

            called_payload = mock_fn.call_args[0][1]
            assert called_payload.dimensions == ["region"]
            assert response["is_valid"] is True

    def test_total_rows_auto_populated(self, public_snapshot, seed_db):
        """Public total-rows endpoint should auto-populate from extra_config."""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_total_rows"
        ) as mock_fn:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_fn.return_value = 100

            request = _make_public_request(
                body={
                    "chart_type": "table",
                    "schema_name": "public",
                    "table_name": "orders",
                    "extra_config": {
                        "dimension_columns": ["region"],
                        "metrics": [{"column": "amount", "aggregation": "count"}],
                    },
                }
            )
            response = get_public_report_table_total_rows(
                request, public_snapshot.public_share_token
            )

            called_payload = mock_fn.call_args[0][1]
            assert called_payload.dimensions == ["region"]
            assert response["total_rows"] == 100


# ================================================================================
# 7. Parity test: GET (dashboard) vs POST (report) produce equivalent payloads
# ================================================================================


class TestDashboardReportParity:
    """The core invariant: a chart's GET endpoint and a report-style POST
    with the same extra_config must produce equivalent ChartDataPayload."""

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    @patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client")
    def test_get_and_post_produce_same_payload(
        self, mock_wh_client, mock_generate, orguser, org, org_warehouse, seed_db
    ):
        extra_config = {
            "x_axis_column": "order_date",
            "y_axis_column": "revenue",
            "dimension_column": "region",
            "extra_dimension_column": "product",
            "metrics": [{"column": "revenue", "aggregation": "sum", "alias": "total"}],
            "time_grain": "month",
            "customizations": {},
            "filters": [{"column": "status", "operator": "equals", "value": "active"}],
        }

        chart = Chart.objects.create(
            title="Parity Chart",
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config=extra_config,
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        mock_generate.return_value = {
            "data": {},
            "echarts_config": {},
        }
        mock_wh_client.return_value = MagicMock()

        # GET path (dashboard)
        request = mock_request(orguser)
        get_chart_data_by_id(request, chart.id)
        get_payload = mock_generate.call_args[0][0]

        mock_generate.reset_mock()

        # POST path (report-style minimal payload)
        post_payload_input = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="sales",
            extra_config=extra_config.copy(),
        )
        get_chart_data(request, post_payload_input)
        post_payload = mock_generate.call_args[0][0]

        # Core fields must match
        assert get_payload.x_axis == post_payload.x_axis == "order_date"
        assert get_payload.y_axis == post_payload.y_axis == "revenue"
        assert get_payload.dimension_col == post_payload.dimension_col == "region"
        assert get_payload.extra_dimension == post_payload.extra_dimension == "product"
        assert get_payload.metrics == post_payload.metrics
        assert get_payload.extra_config["time_grain"] == post_payload.extra_config["time_grain"]
        assert get_payload.extra_config["filters"] == post_payload.extra_config["filters"]

        chart.delete()
