"""Closing Task 4's gap for table/map dashboard tiles.

`POST /api/charts/chart-data-preview/` and `POST /api/charts/map-data-overlay/`
serve chart data for table/map tile types but were out of Task 4's scope
(the two by-id chart GETs only). Dashboard tiles use these POST endpoints
too, so a Member hitting a dashboard with a table/map chart had no gated
path -- they either 403'd unconditionally (`map-data-overlay`, gated by
`can_view_warehouse_data`, which Members never hold) or, worse,
`chart-data-preview` (gated by `can_view_charts`, which Members DO hold)
let a Member query ANY schema/table directly with no chart/dashboard
context at all (`has_schema_access` is a `# TODO` stub returning `True`
unconditionally).

Both payload schemas gain optional `chart_id`/`dashboard_id` access-context
fields, routed through the SAME `require_chart_view_access` helper Task 4
built:

- WITH `chart_id`: the request claims to render a saved chart (e.g. a
  dashboard table/map tile). Fetched org-scoped (404 if missing/cross-org).
  Guarded against becoming a "query anything via a chart I can see" oracle:
  the payload's schema_name/table_name must exactly match the chart's own
  (403 on mismatch) -- `require_chart_view_access` only proves the viewer
  may see *that chart*, not arbitrary tables. `dashboard_id` (if also
  given) is the dashboard that framed the render, same semantics as
  Task 4's by-id GETs (membership + resolver view; cross-org 404).
  Without `dashboard_id`, standalone rules apply: Analyst+ or the chart's
  owner.
- WITHOUT `chart_id` (the chart-builder's live/unsaved-config preview --
  there's no chart to check ownership against): stays role-gated at
  Analyst+ via the new `chart_access.require_analyst_plus` (Members can't
  reach the builder). This is a *tightening* for `chart-data-preview`
  specifically -- its decorator (`can_view_charts`) already admits
  Members; before this task nothing else stopped a Member's config-only
  request.

`map-data-overlay`'s decorator moves from `can_view_warehouse_data` (which
excludes Members outright, and was that endpoint's only access control) to
`can_view_charts` (matching `chart-data-preview` and Task 4's GETs) --
Members must reach the endpoint body for the new `chart_id`+`dashboard_id`
path to admit them at all. `require_analyst_plus` on the config-only path
restores the pre-task restriction for that path (Analyst+ only), so
standalone map queries are no more permissive than before.

`/chart-data-preview/total-rows/` -- the preview endpoint's twin (same
decorator, same frontend hook pair, same pre-task hole) -- gets the
identical treatment; see its test class below.

Same conventions as `test_chart_render_gate.py`: endpoints called directly
via `mock_request(orguser)`, reusing its org/admin/analyst/member fixtures
and chart/dashboard builders.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import MagicMock, patch

import pytest
from ninja.errors import HttpError

from ddpui.api.charts_api import (
    MapDataOverlayPayload,
    get_chart_data_preview,
    get_chart_data_preview_total_rows,
    get_map_data_overlay,
)
from ddpui.core.sharing.chart_access import ChartRenderContext
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org, OrgWarehouse
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_render_gate import (
    _chart,
    _dashboard_with_charts,
    _grant,
)
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db


PREVIEW_RESULT = {
    "columns": ["category"],
    "column_types": {"category": "string"},
    "data": [{"category": "A"}],
    "page": 0,
    "limit": 10,
}

MAP_ROWS = [{"category": "North", "value": 42}]


@pytest.fixture
def org_warehouse(org):
    wh = OrgWarehouse.objects.create(
        org=org, wtype="postgres", name="Chart Post Gate Warehouse", airbyte_destination_id="d-1"
    )
    yield wh
    wh.delete()


def _preview_payload(schema_name="public", table_name="beneficiaries"):
    return ChartDataPayload(
        chart_type="table",
        schema_name=schema_name,
        table_name=table_name,
        dimensions=["category"],
        metrics=[ChartMetric(aggregation="sum", column="amount", alias="value")],
    )


def _map_payload(schema_name="public", table_name="beneficiaries", **overrides):
    kwargs = dict(
        schema_name=schema_name,
        table_name=table_name,
        geographic_column="category",
        value_column="amount",
        metrics=[ChartMetric(aggregation="sum", column="amount", alias="value")],
    )
    kwargs.update(overrides)
    return MapDataOverlayPayload(**kwargs)


# ================================================================================
# POST /api/charts/chart-data-preview/
# ================================================================================


class TestChartDataPreviewGate:
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_with_chart_and_dashboard_view_access_gets_data(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        mock_preview.return_value = PREVIEW_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart_data_preview(
            request,
            _preview_payload(),
            page=0,
            limit=10,
            chart_id=chart.id,
            dashboard_id=dashboard.id,
        )

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_denied_when_chart_not_on_dashboard(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        chart = _chart(org, analyst)
        other_chart = _chart(org, analyst, title="Other Chart")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [other_chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(
                request, _preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
            )
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_with_chart_id_but_no_dashboard_id_denied(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        """Standalone (chart_id present, no dashboard context): Members
        without ownership are denied, matching Task 4."""
        chart = _chart(org, analyst)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, _preview_payload(), chart_id=chart.id)
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_config_only_no_chart_id_denied(self, mock_preview, org, member, org_warehouse):
        """No chart_id at all (chart-builder-style raw config): Members
        can't reach the builder, still 403 -- this is the fix for the
        pre-existing gap (has_schema_access is a no-op stub)."""
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(request, _preview_payload())
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_analyst_config_only_no_chart_id_200(self, mock_preview, org, analyst, org_warehouse):
        """Today's Analyst+ config-only/builder behavior is preserved."""
        mock_preview.return_value = PREVIEW_RESULT
        request = mock_request(analyst)

        response = get_chart_data_preview(request, _preview_payload())

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_cross_org_dashboard_id_404(self, mock_preview, org, member, analyst, org_warehouse):
        chart = _chart(org, analyst)
        other_org = Org.objects.create(
            name="Chart Post Gate Other Org", slug="cpg-other", airbyte_workspace_id="w9"
        )
        other_dashboard = _dashboard_with_charts(
            other_org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(
                request, _preview_payload(), chart_id=chart.id, dashboard_id=other_dashboard.id
            )
        assert exc_info.value.status_code == 404
        mock_preview.assert_not_called()

        other_dashboard.delete()
        other_org.delete()

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_member_granted_on_private_dashboard(
        self, mock_preview, org, member, analyst, org_warehouse
    ):
        mock_preview.return_value = PREVIEW_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        response = get_chart_data_preview(
            request, _preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_owner_can_preview_own_chart_standalone(self, mock_preview, org, member, org_warehouse):
        """chart_id present, no dashboard_id, caller owns the chart ->
        allowed (the builder editing your own saved chart)."""
        mock_preview.return_value = PREVIEW_RESULT
        chart = _chart(org, member, owner=member)
        request = mock_request(member)

        response = get_chart_data_preview(request, _preview_payload(), chart_id=chart.id)

        assert response.columns == PREVIEW_RESULT["columns"]

    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_schema_table_mismatch_denied(self, mock_preview, org, member, analyst, org_warehouse):
        """chart_id + dashboard_id grants access to THAT chart's own
        table -- not an oracle for querying an unrelated schema/table."""
        chart = _chart(org, analyst)  # schema_name="public", table_name="beneficiaries"
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview(
                request,
                _preview_payload(schema_name="public", table_name="some_other_table"),
                chart_id=chart.id,
                dashboard_id=dashboard.id,
            )
        assert exc_info.value.status_code == 403
        mock_preview.assert_not_called()


# ================================================================================
# POST /api/charts/map-data-overlay/
# ================================================================================


class TestMapDataOverlayGate:
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_with_chart_and_dashboard_view_access_gets_data(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_execute.return_value = MAP_ROWS
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_map_data_overlay(
            request, _map_payload(chart_id=chart.id, dashboard_id=dashboard.id)
        )

        assert response["success"] is True
        assert response["count"] == 1

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_denied_when_chart_not_on_dashboard(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _chart(org, analyst)
        other_chart = _chart(org, analyst, title="Other Chart")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [other_chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request, _map_payload(chart_id=chart.id, dashboard_id=dashboard.id)
            )
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_without_dashboard_id_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _chart(org, analyst)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(request, _map_payload(chart_id=chart.id))
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_member_config_only_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, org_warehouse
    ):
        """No chart_id: Members are still denied -- map-data-overlay's
        pre-task posture (can_view_warehouse_data excluded Members
        outright) is preserved for the config-only path even though the
        decorator itself now admits Members through to the chart_id path."""
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(request, _map_payload())
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_analyst_config_only_200(
        self, mock_get_wh, mock_build, mock_execute, org, analyst, org_warehouse
    ):
        """Today's Analyst+ behavior is preserved."""
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_execute.return_value = MAP_ROWS
        request = mock_request(analyst)

        response = get_map_data_overlay(request, _map_payload())

        assert response["success"] is True

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_cross_org_dashboard_id_404(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _chart(org, analyst)
        other_org = Org.objects.create(
            name="Map Post Gate Other Org", slug="mpg-other", airbyte_workspace_id="w8"
        )
        other_dashboard = _dashboard_with_charts(
            other_org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request, _map_payload(chart_id=chart.id, dashboard_id=other_dashboard.id)
            )
        assert exc_info.value.status_code == 404
        mock_execute.assert_not_called()

        other_dashboard.delete()
        other_org.delete()

    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_schema_table_mismatch_denied(
        self, mock_get_wh, mock_build, mock_execute, org, member, analyst, org_warehouse
    ):
        chart = _chart(org, analyst)  # schema_name="public", table_name="beneficiaries"
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_map_data_overlay(
                request,
                _map_payload(
                    schema_name="public",
                    table_name="some_other_table",
                    chart_id=chart.id,
                    dashboard_id=dashboard.id,
                ),
            )
        assert exc_info.value.status_code == 403
        mock_execute.assert_not_called()


# ================================================================================
# run_chart_query seam — pin that the chart_id-present path actually routes
# through it, not just that its pass-through result comes back unchanged.
# ================================================================================


class TestRunChartQuerySeamForPostEndpoints:
    @patch("ddpui.api.charts_api.run_chart_query")
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_chart_data_preview_routes_through_seam(
        self, mock_preview, mock_run, org, analyst, org_warehouse
    ):
        mock_run.return_value = PREVIEW_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(analyst)

        response = get_chart_data_preview(
            request, _preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response.columns == PREVIEW_RESULT["columns"]
        mock_run.assert_called_once()
        mock_preview.assert_not_called()  # only reachable via the seam's execute()
        viewer_ctx, seam_chart, context = mock_run.call_args.args[:3]
        assert viewer_ctx == analyst
        assert seam_chart == chart
        assert context == ChartRenderContext(dashboard_id=dashboard.id)

    @patch("ddpui.api.charts_api.run_chart_query")
    @patch("ddpui.core.charts.charts_service.get_chart_data_table_preview")
    def test_chart_data_preview_config_only_bypasses_seam(
        self, mock_preview, mock_run, org, analyst, org_warehouse
    ):
        """No chart_id -- no Chart row to hand run_chart_query, so it calls
        straight through."""
        mock_preview.return_value = PREVIEW_RESULT
        request = mock_request(analyst)

        response = get_chart_data_preview(request, _preview_payload())

        assert response.columns == PREVIEW_RESULT["columns"]
        mock_run.assert_not_called()

    @patch("ddpui.api.charts_api.run_chart_query")
    @patch("ddpui.core.charts.charts_service.execute_chart_query")
    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_map_data_overlay_routes_through_seam(
        self, mock_get_wh, mock_build, mock_execute, mock_run, org, analyst, org_warehouse
    ):
        mock_get_wh.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        mock_run.return_value = MAP_ROWS
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(analyst)

        response = get_map_data_overlay(
            request, _map_payload(chart_id=chart.id, dashboard_id=dashboard.id)
        )

        assert response["success"] is True
        mock_run.assert_called_once()
        mock_execute.assert_not_called()  # only reachable via the seam's execute()
        viewer_ctx, seam_chart, context = mock_run.call_args.args[:3]
        assert viewer_ctx == analyst
        assert seam_chart == chart
        assert context == ChartRenderContext(dashboard_id=dashboard.id)


# ================================================================================
# POST /api/charts/chart-data-preview/total-rows/ -- chart-data-preview's
# twin: same decorator, same frontend hook (useChartDataPreviewTotalRows),
# same pre-task hole (no gate beyond the has_schema_access stub). Given the
# identical shape, it gets the identical fix rather than being left as a
# residual leak next to its now-gated sibling.
# ================================================================================


class TestChartDataPreviewTotalRowsGate:
    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_member_with_chart_and_dashboard_view_access_gets_count(
        self, mock_total, org, member, analyst, org_warehouse
    ):
        mock_total.return_value = 42
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart_data_preview_total_rows(
            request, _preview_payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )

        assert response == 42

    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_member_config_only_denied(self, mock_total, org, member, org_warehouse):
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview_total_rows(request, _preview_payload())
        assert exc_info.value.status_code == 403
        mock_total.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_analyst_config_only_200(self, mock_total, org, analyst, org_warehouse):
        mock_total.return_value = 7
        request = mock_request(analyst)

        response = get_chart_data_preview_total_rows(request, _preview_payload())

        assert response == 7

    @patch("ddpui.core.charts.charts_service.get_chart_data_total_rows")
    def test_schema_table_mismatch_denied(self, mock_total, org, member, analyst, org_warehouse):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_preview_total_rows(
                request,
                _preview_payload(schema_name="public", table_name="some_other_table"),
                chart_id=chart.id,
                dashboard_id=dashboard.id,
            )
        assert exc_info.value.status_code == 403
        mock_total.assert_not_called()
