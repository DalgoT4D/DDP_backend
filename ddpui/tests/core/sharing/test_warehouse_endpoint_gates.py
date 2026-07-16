"""v1.1 M2 — gating the raw-payload warehouse endpoints (promoted M1-review
Important).

`POST /api/charts/chart-data/`, `/map-data/` and `/download-csv/` take a
raw schema/table/metrics payload, were reachable with `can_view_charts`
(which Members hold), and consulted no resolver at all (`has_schema_access`
is a TODO stub returning True; `/map-data/` didn't even call it) — a
Member could pull arbitrary warehouse data. They now carry the SAME
access-context contract as the preview siblings (`_gate_raw_chart_payload`):

- `chart_id` (+ optional `dashboard_id`): org-scoped chart, schema/table
  match, `require_chart_view_access`; a dashboard context additionally
  pins the payload's columns to the saved config.
- no `chart_id` (raw config — the chart BUILDER's surface): Analyst+ only.
  Members lose raw access; they were never meant to have it (builder is
  Analyst+, and every Member-facing render path carries a context).

Builder regression: Analyst config-only requests keep working unchanged.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import patch

import pytest
from django.http import StreamingHttpResponse
from ninja.errors import HttpError

from ddpui.api.charts_api import (
    download_chart_data_csv,
    generate_map_chart_data,
    get_chart_data,
)
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org, OrgWarehouse
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_render_gate import (
    _chart,
    _dashboard_with_charts,
)
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db

CHART_RESULT = {"data": {"rows": []}, "echarts_config": {"series": []}}


@pytest.fixture
def org_warehouse(org):
    wh = OrgWarehouse.objects.create(
        org=org, wtype="postgres", name="M2 Gate Warehouse", airbyte_destination_id="d-m2"
    )
    yield wh
    wh.delete()


def _payload(schema_name="public", table_name="beneficiaries"):
    return ChartDataPayload(
        chart_type="bar",
        schema_name=schema_name,
        table_name=table_name,
        dimension_col="category",
        metrics=[ChartMetric(aggregation="sum", column="amount", alias="value")],
    )


class TestChartDataGate:
    def test_member_config_only_403(self, org, member, org_warehouse):
        with pytest.raises(HttpError) as excinfo:
            get_chart_data(mock_request(member), _payload())
        assert excinfo.value.status_code == 403

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_analyst_config_only_still_works(self, mock_generate, org, analyst, org_warehouse):
        mock_generate.return_value = CHART_RESULT
        response = get_chart_data(mock_request(analyst), _payload())
        assert response.echarts_config == {"series": []}

    @patch("ddpui.api.charts_api.generate_chart_data_and_config")
    def test_member_with_dashboard_context_admitted(
        self, mock_generate, org, analyst, member, org_warehouse
    ):
        mock_generate.return_value = CHART_RESULT
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        response = get_chart_data(
            mock_request(member), _payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )
        assert response.echarts_config == {"series": []}

    def test_member_standalone_chart_context_denied(self, org, analyst, member, org_warehouse):
        """Standalone chart context: the resolver decides on the CHART, and
        Members get nothing from chart levels/grants (member_sharing=False)."""
        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            get_chart_data(mock_request(member), _payload(), chart_id=chart.id)
        assert excinfo.value.status_code == 403

    def test_schema_table_mismatch_403(self, org, analyst, org_warehouse):
        chart = _chart(org, analyst)  # public.beneficiaries
        with pytest.raises(HttpError) as excinfo:
            get_chart_data(
                mock_request(analyst),
                _payload(table_name="salaries"),
                chart_id=chart.id,
            )
        assert excinfo.value.status_code == 403

    def test_cross_org_chart_404(self, org, analyst, org_warehouse):
        other_org = Org.objects.create(name="WH Gate Other", slug="wh-gate-other")
        foreign = _chart(other_org, None)
        with pytest.raises(HttpError) as excinfo:
            get_chart_data(mock_request(analyst), _payload(), chart_id=foreign.id)
        assert excinfo.value.status_code == 404
        other_org.delete()

    def test_dashboard_context_pins_payload_columns(self, org, analyst, member, org_warehouse):
        """A context-admitted Member naming a column outside the saved
        config is refused (the Task 6d column guard, now on this endpoint)."""
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        rogue = _payload()
        rogue.dimension_col = "salary"  # not in the saved config
        with pytest.raises(HttpError) as excinfo:
            get_chart_data(
                mock_request(member), rogue, chart_id=chart.id, dashboard_id=dashboard.id
            )
        assert excinfo.value.status_code == 403


class TestMapDataGate:
    def test_member_config_only_403(self, org, member, org_warehouse):
        with pytest.raises(HttpError) as excinfo:
            generate_map_chart_data(mock_request(member), _payload())
        assert excinfo.value.status_code == 403

    def test_analyst_config_only_passes_gate(self, org, analyst, org_warehouse):
        """The gate admits the analyst; the endpoint then fails on its own
        missing-geojson validation (400) — proving the 403 is the gate."""
        with pytest.raises(HttpError) as excinfo:
            generate_map_chart_data(mock_request(analyst), _payload())
        assert excinfo.value.status_code == 400
        assert "geojson" in str(excinfo.value.message).lower()

    def test_member_standalone_chart_context_denied(self, org, analyst, member, org_warehouse):
        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            generate_map_chart_data(mock_request(member), _payload(), chart_id=chart.id)
        assert excinfo.value.status_code == 403


class TestDownloadCsvGate:
    def test_member_config_only_403(self, org, member, org_warehouse):
        with pytest.raises(HttpError) as excinfo:
            download_chart_data_csv(mock_request(member), _payload())
        assert excinfo.value.status_code == 403

    def test_analyst_config_only_still_streams(self, org, analyst, org_warehouse):
        response = download_chart_data_csv(mock_request(analyst), _payload())
        assert isinstance(response, StreamingHttpResponse)

    def test_member_with_dashboard_context_streams(self, org, analyst, member, org_warehouse):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        response = download_chart_data_csv(
            mock_request(member), _payload(), chart_id=chart.id, dashboard_id=dashboard.id
        )
        assert isinstance(response, StreamingHttpResponse)

    def test_member_standalone_chart_context_denied(self, org, analyst, member, org_warehouse):
        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            download_chart_data_csv(mock_request(member), _payload(), chart_id=chart.id)
        assert excinfo.value.status_code == 403
