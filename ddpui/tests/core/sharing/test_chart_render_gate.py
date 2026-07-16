"""The chart render-path contract (plan Sec 3.3).

Charts are NOT shareable — a chart is visible wherever its dashboards are
visible. The two by-id chart GETs (`/api/charts/{id}/` detail and
`/api/charts/{id}/data/`) gain an optional ``dashboard_id`` access-context
param:

- WITH ``dashboard_id``: serve iff the chart is actually ON that dashboard
  (membership — without this check ``dashboard_id`` is an oracle to read
  arbitrary charts) AND the viewer has >= view on that dashboard via the
  resolver. Same-org enforced; cross-org dashboard ids 404.
- WITHOUT ``dashboard_id`` (standalone: builder / Charts page): Analyst+
  keeps today's behavior; the chart's owner (owner_id, created_by
  fallback) is allowed; plain Members are denied 403 — they keep chart
  data only through dashboard/report context.

``dashboard_id`` (access context) is distinct from the pre-existing
``dashboard_filters`` param (a filter-values payload); one test proves
they compose. All warehouse-bound execution on this path is routed through
``run_chart_query`` — the single choke-point Layer 2/3 will hook.

Same conventions as `test_detail_view_gate.py`: endpoints called directly
via `mock_request(orguser)`, reusing that file's org/admin/analyst/member
fixtures.
"""

import json
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import MagicMock, patch

import pytest
from ninja.errors import HttpError

from ddpui.api.charts_api import get_chart, get_chart_data_by_id
from ddpui.core.sharing.chart_access import ChartRenderContext, run_chart_query
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardFilterType
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.tests.core.sharing.test_detail_view_gate import (
    _grant,
    admin,
    analyst,
    member,
    org,
)

pytestmark = pytest.mark.django_db

CHART_RESULT = {"data": {"categories": ["A"], "values": [1]}, "echarts_config": {"type": "bar"}}


# ================================================================================
# Fixtures / builders
# ================================================================================


@pytest.fixture
def warehouse(org):
    """An OrgWarehouse row plus a mocked warehouse client — the data endpoint
    builds a client unconditionally and there are no real credentials here."""
    wh = OrgWarehouse.objects.create(
        org=org, wtype="postgres", name="Render Gate Warehouse", airbyte_destination_id="dest-1"
    )
    with patch(
        "ddpui.api.charts_api.WarehouseFactory.get_warehouse_client", return_value=MagicMock()
    ):
        yield wh
    wh.delete()


def _chart(org_obj, creator, owner=None, title="Render Gate Chart"):
    return Chart.objects.create(
        title=title,
        chart_type="bar",
        schema_name="public",
        table_name="beneficiaries",
        extra_config={
            "dimension_column": "category",
            "metrics": [{"column": "amount", "aggregation": "sum"}],
        },
        created_by=creator,
        owner=owner,
        last_modified_by=creator,
        org=org_obj,
    )


def _dashboard_with_charts(org_obj, owner, analyst_level, member_level, charts):
    """A dashboard whose single tab holds one chart tile per chart in `charts`."""
    return Dashboard.objects.create(
        title="Render Gate Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [],
                "components": {
                    str(i): {"type": "chart", "config": {"chartId": chart.id}}
                    for i, chart in enumerate(charts, start=1)
                },
            }
        ],
    )


# ================================================================================
# GET /api/charts/{id}/data/ — dashboard context
# ================================================================================


class TestChartDataDashboardContext:
    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_with_dashboard_view_access_gets_data(
        self, mock_generate, org, member, analyst, warehouse
    ):
        """Member + valid dashboard_id + view access on that dashboard -> 200 with data."""
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart_data_by_id(request, chart.id, dashboard_id=dashboard.id)

        assert response.data == CHART_RESULT["data"]
        mock_generate.assert_called_once()

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_denied_when_chart_not_on_dashboard(
        self, mock_generate, org, member, analyst, warehouse
    ):
        """dashboard_id must not be an oracle: an accessible dashboard the
        chart is NOT on grants nothing."""
        chart = _chart(org, analyst)
        other_chart = _chart(org, analyst, title="Other Chart")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [other_chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_by_id(request, chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_generate.assert_not_called()

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_denied_on_private_dashboard(
        self, mock_generate, org, member, analyst, warehouse
    ):
        """Chart IS on the dashboard, but the resolver denies view on it."""
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_by_id(request, chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403
        mock_generate.assert_not_called()

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_granted_on_private_dashboard(
        self, mock_generate, org, member, analyst, warehouse
    ):
        """A ResourceShare grant on the framing dashboard admits the member."""
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        response = get_chart_data_by_id(request, chart.id, dashboard_id=dashboard.id)

        assert response.data == CHART_RESULT["data"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_cross_org_dashboard_id_404(self, mock_generate, org, member, analyst, warehouse):
        """A dashboard id from another org is indistinguishable from a
        nonexistent one -> 404, even though the chart itself is in-org."""
        chart = _chart(org, analyst)
        other_org = Org.objects.create(
            name="Render Gate Other Org", slug="render-gate-other", airbyte_workspace_id="w2"
        )
        other_dashboard = Dashboard.objects.create(
            title="Other org dashboard",
            org=other_org,
            analyst_level=AccessLevel.VIEW,
            member_level=AccessLevel.VIEW,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "Tab 1",
                    "layout_config": [],
                    "components": {"1": {"type": "chart", "config": {"chartId": chart.id}}},
                }
            ],
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_by_id(request, chart.id, dashboard_id=other_dashboard.id)
        assert exc_info.value.status_code == 404
        mock_generate.assert_not_called()

        other_dashboard.delete()
        other_org.delete()


# ================================================================================
# GET /api/charts/{id}/data/ — standalone (no dashboard_id)
# ================================================================================


class TestChartDataStandalone:
    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_standalone_403(self, mock_generate, org, member, analyst, warehouse):
        """Members only get chart data through dashboard/report context."""
        chart = _chart(org, analyst)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_by_id(request, chart.id)
        assert exc_info.value.status_code == 403
        mock_generate.assert_not_called()

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_analyst_standalone_200(self, mock_generate, org, analyst, warehouse):
        """Today's Analyst+ behavior is preserved."""
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        request = mock_request(analyst)

        response = get_chart_data_by_id(request, chart.id)

        assert response.data == CHART_RESULT["data"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_admin_standalone_200(self, mock_generate, org, admin, analyst, warehouse):
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        request = mock_request(admin)

        response = get_chart_data_by_id(request, chart.id)

        assert response.data == CHART_RESULT["data"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_owner_standalone_200(self, mock_generate, org, member, analyst, warehouse):
        """The chart's owner (owner FK) may render it standalone."""
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst, owner=member)
        request = mock_request(member)

        response = get_chart_data_by_id(request, chart.id)

        assert response.data == CHART_RESULT["data"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_creator_standalone_200_when_owner_null(
        self, mock_generate, org, member, warehouse
    ):
        """created_by is the ownership fallback when owner is null (mirrors
        can_delete_resource)."""
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, member, owner=None)
        request = mock_request(member)

        response = get_chart_data_by_id(request, chart.id)

        assert response.data == CHART_RESULT["data"]

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_creator_denied_when_owned_by_someone_else(
        self, mock_generate, org, member, analyst, warehouse
    ):
        """owner_id wins over created_by: a member who created the chart but
        no longer owns it is denied standalone."""
        chart = _chart(org, member, owner=analyst)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart_data_by_id(request, chart.id)
        assert exc_info.value.status_code == 403
        mock_generate.assert_not_called()


# ================================================================================
# dashboard_filters (filter payload) composes with dashboard_id (access context)
# ================================================================================


class TestDashboardFiltersWithDashboardId:
    @patch("ddpui.api.charts_api.DashboardService.resolve_dashboard_filters_for_chart")
    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_filters_still_resolved_alongside_access_context(
        self, mock_generate, mock_resolve, org, member, analyst, warehouse
    ):
        """Passing both params: dashboard_id gates access, dashboard_filters
        still flows into the generated payload — neither breaks the other."""
        mock_generate.return_value = CHART_RESULT
        resolved = [{"column": "region", "operator": "eq", "value": "north"}]
        mock_resolve.return_value = resolved
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        filter_obj = DashboardFilter.objects.create(
            dashboard=dashboard,
            name="Region",
            filter_type=DashboardFilterType.VALUE.value,
            schema_name="public",
            table_name="beneficiaries",
            column_name="region",
        )
        request = mock_request(member)

        response = get_chart_data_by_id(
            request,
            chart.id,
            dashboard_filters=json.dumps({str(filter_obj.id): "north"}),
            dashboard_id=dashboard.id,
        )

        assert response.data == CHART_RESULT["data"]
        mock_resolve.assert_called_once()
        payload = mock_generate.call_args.args[0]
        assert payload.dashboard_filters == resolved


# ================================================================================
# run_chart_query — the single warehouse-execution choke-point (Layer 2/3 hook)
# ================================================================================


class TestRunChartQuerySeam:
    @patch("ddpui.api.charts_api.run_chart_query")
    def test_endpoint_routes_through_seam(self, mock_run, org, analyst, warehouse):
        """The data endpoint's execution goes through run_chart_query — patching
        it out means no generate/warehouse call ever happens."""
        mock_run.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(analyst)

        response = get_chart_data_by_id(request, chart.id, dashboard_id=dashboard.id)

        assert response.data == CHART_RESULT["data"]
        mock_run.assert_called_once()
        viewer_ctx, seam_chart, context = mock_run.call_args.args[:3]
        assert viewer_ctx == analyst
        assert seam_chart == chart
        assert context == ChartRenderContext(dashboard_id=dashboard.id)

    def test_run_chart_query_is_a_passthrough(self, org, analyst):
        """Access no-op today: returns exactly what its executor returns."""
        chart = _chart(org, analyst)

        result = run_chart_query(
            analyst, chart, ChartRenderContext(dashboard_id=None), lambda: CHART_RESULT
        )

        assert result == CHART_RESULT


# ================================================================================
# GET /api/charts/{id}/ — the detail/config GET used by dashboard tiles gets
# the same context rule (the dashboard view fetches chart metadata per tile).
# ================================================================================


class TestChartDetailContextGate:
    def test_member_standalone_403(self, org, member, analyst):
        chart = _chart(org, analyst)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart(request, chart.id)
        assert exc_info.value.status_code == 403

    def test_member_with_dashboard_view_access_200(self, org, member, analyst):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        request = mock_request(member)

        response = get_chart(request, chart.id, dashboard_id=dashboard.id)
        assert response.id == chart.id

    def test_member_denied_when_chart_not_on_dashboard(self, org, member, analyst):
        chart = _chart(org, analyst)
        other_chart = _chart(org, analyst, title="Other Chart")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [other_chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart(request, chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403

    def test_member_denied_on_private_dashboard(self, org, member, analyst):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_chart(request, chart.id, dashboard_id=dashboard.id)
        assert exc_info.value.status_code == 403

    def test_analyst_standalone_200(self, org, analyst):
        chart = _chart(org, analyst)
        request = mock_request(analyst)

        response = get_chart(request, chart.id)
        assert response.id == chart.id

    def test_member_owner_standalone_200(self, org, member, analyst):
        chart = _chart(org, analyst, owner=member)
        request = mock_request(member)

        response = get_chart(request, chart.id)
        assert response.id == chart.id
