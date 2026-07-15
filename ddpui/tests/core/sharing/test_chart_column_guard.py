"""Task 6d: column-set guard for context-admitted table/map chart POSTs.

Task 6b let a Member reach `POST /api/charts/chart-data-preview/` (+
`total-rows`) and `POST /api/charts/map-data-overlay/` via a
`chart_id`+`dashboard_id` dashboard context, guarded by
`require_chart_view_access` plus a schema/table match. Residual: the
payload's column references (`dimensions`, `metrics`, `geographic_column`,
`value_column`, `filters`, `extra_config`) stayed attacker-controlled, so a
context-admitted Member could read OTHER columns of the chart's table (and
probe single rows via filters) -- a chart showing an innocuous aggregate
becomes a read primitive for every column, including PII.

`require_payload_within_chart_config` closes this: on the dashboard-context
path (chart_id AND dashboard_id present) every submitted column reference
must be derivable from the SAVED chart's config; filter columns may
additionally come from the framing dashboard's configured filters (that's
how dashboard filtering works -- values stay free, columns don't); the
`dashboard_filters` {filter_id: value} map may only name the framing
dashboard's own filters. Violations 403 with the same generic message as
the schema/table guard -- no oracle about which column failed.

Analyst+ standalone (chart_id without dashboard_id) and config-only (no
chart_id) requests are NOT touched -- pinned below.

Same conventions as test_chart_post_gate.py: endpoints called directly via
`mock_request(orguser)`, reusing the sharing fixtures/builders.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import json
from unittest.mock import MagicMock, patch

import pytest
from ninja.errors import HttpError

from ddpui.api.charts_api import (
    MapDataOverlayPayload,
    get_chart_data_preview,
    get_chart_data_preview_total_rows,
    get_map_data_overlay,
)
from ddpui.models.dashboard import DashboardFilter, DashboardFilterType
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_render_gate import _dashboard_with_charts
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db


PREVIEW_RESULT = {
    "columns": ["district"],
    "column_types": {"district": "string"},
    "data": [{"district": "North"}],
    "page": 0,
    "limit": 10,
}

MAP_ROWS = [{"state": "North", "value": 42}]

# A realistic saved table-chart config: two dimensions, one metric, a
# chart-level filter, a sort. "phone_number" is the sensitive column the
# chart does NOT reference.
TABLE_CONFIG = {
    "dimensions": [
        {"column": "district", "enable_drill_down": False},
        {"column": "category", "enable_drill_down": False},
    ],
    "dimension_columns": ["district", "category"],
    "metrics": [{"column": "amount", "aggregation": "sum", "alias": "Total Amount"}],
    "filters": [{"column": "status", "operator": "equals", "value": "active"}],
    "sort": [{"column": "district", "direction": "asc"}],
    "pagination": {"enabled": True, "page_size": 50},
}

# A realistic saved map-chart config with a drill-down hierarchy.
MAP_CONFIG = {
    "geographic_column": "state",
    "value_column": "amount",
    "aggregate_function": "sum",
    "selected_geojson_id": 1,
    "geographic_hierarchy": {
        "country_code": "IND",
        "base_level": {"level": 0, "column": "state", "region_type": "state", "label": "State"},
        "drill_down_levels": [
            {
                "level": 1,
                "column": "district",
                "region_type": "district",
                "label": "District",
                "parent_level": 0,
                "parent_column": "state",
            }
        ],
    },
    "filters": [],
}


@pytest.fixture
def org_warehouse(org):
    wh = OrgWarehouse.objects.create(
        org=org, wtype="postgres", name="Column Guard Warehouse", airbyte_destination_id="d-6d"
    )
    yield wh
    wh.delete()


def _saved_chart(org_obj, creator, chart_type="table", extra_config=None, title="Guarded Chart"):
    return Chart.objects.create(
        title=title,
        chart_type=chart_type,
        schema_name="public",
        table_name="beneficiaries",
        extra_config=extra_config if extra_config is not None else dict(TABLE_CONFIG),
        created_by=creator,
        owner=creator,
        last_modified_by=creator,
        org=org_obj,
    )


def _dashboard_filter(dashboard, column_name="ward", name="Ward"):
    return DashboardFilter.objects.create(
        dashboard=dashboard,
        name=name,
        filter_type=DashboardFilterType.VALUE.value,
        schema_name="public",
        table_name="beneficiaries",
        column_name=column_name,
    )


def _tile_preview_payload(**overrides):
    """What a legitimate dashboard table tile sends: the chart's own saved
    config (chart-element-v2.tsx builds it from chart.extra_config)."""
    kwargs = dict(
        chart_type="table",
        schema_name="public",
        table_name="beneficiaries",
        dimensions=["district", "category"],
        metrics=[ChartMetric(column="amount", aggregation="sum", alias="Total Amount")],
        extra_config={
            "filters": list(TABLE_CONFIG["filters"]),
            "pagination": dict(TABLE_CONFIG["pagination"]),
            "sort": list(TABLE_CONFIG["sort"]),
        },
    )
    kwargs.update(overrides)
    return ChartDataPayload(**kwargs)


def _tile_map_payload(**overrides):
    """What a legitimate dashboard map tile sends (useChart.ts
    transformMapDataOverlayPayload): metrics constructed from the saved
    aggregate_function/value_column, drill-down filters keyed by the
    hierarchy's parent column."""
    kwargs = dict(
        schema_name="public",
        table_name="beneficiaries",
        geographic_column="state",
        value_column="amount",
        metrics=[ChartMetric(column="amount", aggregation="sum", alias="value")],
        filters={},
        dashboard_filters={},
        extra_config={"filters": [], "pagination": None, "sort": None},
    )
    kwargs.update(overrides)
    return MapDataOverlayPayload(**kwargs)


# ================================================================================
# chart-data-preview: legitimate tiles keep working
# ================================================================================


class TestPreviewLegitimateTiles:
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_tile_with_saved_config_passes(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        mock_preview.return_value = PREVIEW_RESULT
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart_data_preview(
            request, _tile_preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.api.charts_api.DashboardService.resolve_dashboard_filters_for_chart")
    @patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_tile_with_dashboard_filter_passes(
        self, mock_preview, mock_wh, mock_resolve, org, member, analyst, org_warehouse
    ):
        """A real saved config + a dashboard filter applied two ways at once:
        as the dashboard_filters {filter_id: value} query param AND merged
        into extra_config.filters (the view/report-mode pattern). Both must
        pass -- dashboard filters legitimately add clauses to tile queries."""
        mock_preview.return_value = PREVIEW_RESULT
        mock_wh.return_value = MagicMock()
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        dash_filter = _dashboard_filter(dashboard, column_name="ward")
        mock_resolve.return_value = [
            {
                "filter_id": str(dash_filter.id),
                "column": "ward",
                "type": "value",
                "value": "Ward 3",
                "settings": {},
            }
        ]
        payload = _tile_preview_payload()
        payload.extra_config["filters"] = list(TABLE_CONFIG["filters"]) + [
            {"column": "ward", "operator": "equals", "value": "Ward 3"}
        ]
        request = mock_request(member)

        response = get_chart_data_preview(
            request,
            payload,
            dashboard_filters=json.dumps({str(dash_filter.id): "Ward 3"}),
            chart_id=chart.id,
            dashboard_id=dashboard.id,
        )

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_tile_drill_down_dimension_subset_passes(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        """Drill-down narrows dimensions to a subset and filters on a saved
        dimension column (chart-element-v2 drill-down behavior)."""
        mock_preview.return_value = PREVIEW_RESULT
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload(dimensions=["category"])
        payload.extra_config["filters"] = list(TABLE_CONFIG["filters"]) + [
            {"column": "district", "operator": "equals", "value": "North"}
        ]
        request = mock_request(member)

        response = get_chart_data_preview(
            request, payload, chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response.columns == PREVIEW_RESULT["columns"]


# ================================================================================
# chart-data-preview: mutation vectors -> 403, query never runs
# ================================================================================


class TestPreviewMutationVectors:
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_extra_dimension_denied(self, mock_preview, org, member, analyst, org_warehouse):
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload(dimensions=["district", "category", "phone_number"])
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        assert "phone_number" not in exc_info.value.message
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_swapped_metric_column_denied(self, mock_preview, org, member, analyst, org_warehouse):
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload(
            metrics=[ChartMetric(column="salary", aggregation="max", alias="Total Amount")]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_novel_filter_column_denied(self, mock_preview, org, member, analyst, org_warehouse):
        """Filtering on a column that is neither in the chart's config nor a
        dashboard filter is a single-row probe primitive -> 403."""
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload()
        payload.extra_config["filters"] = list(TABLE_CONFIG["filters"]) + [
            {"column": "phone_number", "operator": "like", "value": "98%"}
        ]
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_extra_config_sort_smuggle_denied(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        """ORDER BY on an unreferenced column is a value oracle -> 403."""
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload()
        payload.extra_config["sort"] = [{"column": "phone_number", "direction": "asc"}]
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_column_expression_smuggle_denied(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        """column_expression is raw SQL (literal_column). Not in the saved
        config -> 403 no matter what it says."""
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload(
            metrics=[
                ChartMetric(
                    column_expression="(SELECT string_agg(phone_number, ',') FROM public.beneficiaries)",
                    alias="Total Amount",
                )
            ]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_extra_config_metrics_smuggle_denied(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        """Column names hidden inside extra_config itself (not just the
        top-level payload fields) are checked too."""
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload()
        payload.extra_config["metrics"] = [{"column": "phone_number", "aggregation": "max"}]
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, payload, chart_id=chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.api.charts_api.DashboardService.resolve_dashboard_filters_for_chart")
    @patch("ddpui.api.charts_api.WarehouseFactory.get_warehouse_client")
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_foreign_dashboard_filter_id_denied(
        self, mock_preview, mock_wh, mock_resolve, org, member, analyst, org_warehouse
    ):
        """dashboard_filters may only name the FRAMING dashboard's filters --
        another dashboard's filter id would smuggle in its column."""
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        other_dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        foreign_filter = _dashboard_filter(other_dashboard, column_name="phone_number")
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(
                request,
                _tile_preview_payload(),
                dashboard_filters=json.dumps({str(foreign_filter.id): "98"}),
                chart_id=chart.id,
                dashboard_id=dashboard.id,
            )
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()
        mock_resolve.assert_not_called()


# ================================================================================
# total-rows twin: guarded the same way
# ================================================================================


class TestTotalRowsColumnGuard:
    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_member_tile_with_saved_config_passes(
        self, mock_total, org, member, analyst, org_warehouse
    ):
        mock_total.return_value = 42
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart_data_preview_total_rows(
            request, _tile_preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response == 42

    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_extra_dimension_denied(self, mock_total, org, member, analyst, org_warehouse):
        chart = _saved_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        payload = _tile_preview_payload(dimensions=["district", "phone_number"])
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview_total_rows(
                request, payload, chart_id=chart.id, dashboard_id=dashboard.id
            )
        assert exc_info.value.status_code == 403
        mock_total.assert_not_called()


# ================================================================================
# map-data-overlay: legitimate tiles (incl. drill-down) pass, mutations 403
# ================================================================================


class TestMapOverlayColumnGuard:
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_tile_with_saved_config_passes(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_execute.return_value = MAP_ROWS
        chart = _saved_chart(org, analyst, chart_type="map", extra_config=MAP_CONFIG)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_map_data_overlay(
            request, _tile_map_payload(chart_id=chart.id, dashboard_id=dashboard.id)
        )

        assert response["success"] is True

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_tile_drill_down_passes(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        """Drill-down switches geographic_column to a hierarchy level column
        and filters by the parent column -- both saved-config-derived."""
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_execute.return_value = MAP_ROWS
        chart = _saved_chart(org, analyst, chart_type="map", extra_config=MAP_CONFIG)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_map_data_overlay(
            request,
            _tile_map_payload(
                geographic_column="district",
                filters={"state": "Karnataka"},
                chart_id=chart.id,
                dashboard_id=dashboard.id,
            ),
        )

        assert response["success"] is True

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_swapped_geographic_column_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _saved_chart(org, analyst, chart_type="map", extra_config=MAP_CONFIG)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request,
                _tile_map_payload(
                    geographic_column="phone_number",
                    chart_id=chart.id,
                    dashboard_id=dashboard.id,
                ),
            )
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_novel_drill_filter_key_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        """Map `filters` is {column: value} -- a novel key is a row probe."""
        chart = _saved_chart(org, analyst, chart_type="map", extra_config=MAP_CONFIG)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request,
                _tile_map_payload(
                    filters={"phone_number": "9876543210"},
                    chart_id=chart.id,
                    dashboard_id=dashboard.id,
                ),
            )
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_swapped_metric_column_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _saved_chart(org, analyst, chart_type="map", extra_config=MAP_CONFIG)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request,
                _tile_map_payload(
                    metrics=[ChartMetric(column="salary", aggregation="sum", alias="value")],
                    chart_id=chart.id,
                    dashboard_id=dashboard.id,
                ),
            )
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()


# ================================================================================
# Pinned: non-context paths stay byte-identical
# ================================================================================


class TestNonContextPathsUnchanged:
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_analyst_config_only_arbitrary_columns_200(
        self, mock_preview, org, analyst, org_warehouse
    ):
        """No chart_id: the builder's live preview. Arbitrary columns stay
        allowed for Analyst+ (today's behavior)."""
        mock_preview.return_value = PREVIEW_RESULT
        request = mock_request(analyst)

        payload = _tile_preview_payload(
            dimensions=["phone_number"],
            metrics=[ChartMetric(column="salary", aggregation="max")],
        )
        response = get_chart_data_preview(request, payload)

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_analyst_standalone_chart_id_arbitrary_columns_200(
        self, mock_preview, org, analyst, org_warehouse
    ):
        """chart_id WITHOUT dashboard_id: standalone Analyst+ (builder editing
        a saved chart) -- the column guard must NOT apply."""
        mock_preview.return_value = PREVIEW_RESULT
        chart = _saved_chart(org, analyst)
        request = mock_request(analyst)

        payload = _tile_preview_payload(
            dimensions=["phone_number"],
            metrics=[ChartMetric(column="salary", aggregation="max")],
        )
        response = get_chart_data_preview(request, payload, chart_id=chart.id)

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_analyst_map_config_only_arbitrary_columns_200(
        self, mock_get_wh, mock_build, mock_execute, org, analyst, org_warehouse
    ):
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_execute.return_value = MAP_ROWS
        request = mock_request(analyst)

        response = get_map_data_overlay(
            request,
            _tile_map_payload(
                geographic_column="phone_number",
                metrics=[ChartMetric(column="salary", aggregation="sum", alias="value")],
            ),
        )

        assert response["success"] is True
