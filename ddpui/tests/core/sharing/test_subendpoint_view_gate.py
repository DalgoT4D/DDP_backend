"""Task 3b: view-gate the content sub-endpoints (closes the T3 review gap).

Task 3 gated the single detail GET per resource type (report/dashboard/
alert/metric/kpi). This suite covers every OTHER by-id sub-endpoint on the
same routers that serves a resource's actual content or metadata — chart
data, KPI data, previews, consumers lists, logs, notes, filters, PDF
export, email export, dashboard duplication — none of which re-checked
access before this task, so a Member with only the role-slug permission
could reach a private resource's content directly by id even though the
list and detail views already denied them.

Same pattern as `test_detail_view_gate.py`: fetch the resource org-scoped,
then `require_view_access` (the Task 3b helper wrapping
`effective_permission`) denies with 403 when the resolver returns None.
Reuses that file's org/admin/analyst/member fixtures and resource builders.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import patch

import pytest
from ninja.errors import HttpError

from ddpui.api.alert_api import get_alert_logs
from ddpui.api.dashboard_native_api import duplicate_dashboard, get_filter
from ddpui.api.kpi_api import (
    get_kpi_consumers,
    get_kpi_data,
    get_kpi_dashboards,
    list_annotations,
)
from ddpui.api.metric_api import get_metric_consumers, preview_metric
from ddpui.api.report_api import (
    export_report_pdf,
    get_comment_states,
    get_report_chart_data,
    get_report_kpi_data,
    list_comments,
    list_dashboard_datetime_columns,
    share_report_via_email,
)
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardFilterType
from ddpui.models.general_access import AccessLevel
from ddpui.schemas.report_schema import ReportShareViaEmailRequest
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.tests.core.sharing.test_detail_view_gate import (
    _alert,
    _dashboard,
    _grant,
    _kpi_with_metric,
    _metric,
    _snapshot,
    admin,
    analyst,
    member,
    org,
)

pytestmark = pytest.mark.django_db


# ================================================================================
# Report — chart data, KPI data (gated on the parent report)
# ================================================================================


class TestReportChartDataGate:
    @patch("ddpui.api.report_api.ReportService.get_report_chart_data")
    def test_member_denied_on_private_resource(self, mock_get_data, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_report_chart_data(request, snapshot.id, 1)
        assert exc_info.value.status_code == 403
        mock_get_data.assert_not_called()

    @patch("ddpui.api.report_api.ReportService.get_report_chart_data")
    def test_member_allowed_on_granted_resource(self, mock_get_data, org, member, analyst):
        mock_get_data.return_value = {"data": {}, "echarts_config": {}}
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = get_report_chart_data(request, snapshot.id, 1)
        assert response.data == {}
        mock_get_data.assert_called_once()

    @patch("ddpui.api.report_api.ReportService.get_report_chart_data")
    def test_admin_allowed_on_any_resource(self, mock_get_data, org, admin, member):
        mock_get_data.return_value = {"data": {}, "echarts_config": {}}
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_report_chart_data(request, snapshot.id, 1)
        assert response.data == {}


class TestReportKpiDataGate:
    @patch("ddpui.api.report_api.ReportService.get_report_kpi_data")
    def test_member_denied_on_private_resource(self, mock_get_data, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_report_kpi_data(request, snapshot.id, 1)
        assert exc_info.value.status_code == 403
        mock_get_data.assert_not_called()

    @patch("ddpui.api.report_api.ReportService.get_report_kpi_data")
    def test_member_allowed_on_granted_resource(self, mock_get_data, org, member, analyst):
        mock_get_data.return_value = {"data": {}, "echarts_config": {}}
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = get_report_kpi_data(request, snapshot.id, 1)
        assert response.data == {}

    @patch("ddpui.api.report_api.ReportService.get_report_kpi_data")
    def test_admin_allowed_on_any_resource(self, mock_get_data, org, admin, member):
        mock_get_data.return_value = {"data": {}, "echarts_config": {}}
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_report_kpi_data(request, snapshot.id, 1)
        assert response.data == {}


# ================================================================================
# Report — PDF export (gated on the parent report)
# ================================================================================


class TestReportPdfExportGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            export_report_pdf(request, snapshot.id)
        assert exc_info.value.status_code == 403

    @patch("ddpui.api.report_api.PdfExportService.generate_pdf")
    @patch("ddpui.api.report_api.ReportService.ensure_share_token")
    def test_member_allowed_on_granted_resource(self, mock_token, mock_pdf, org, member, analyst):
        mock_token.return_value = "tok"
        mock_pdf.return_value = b"%PDF-1.4"
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = export_report_pdf(request, snapshot.id)
        assert response.content == b"%PDF-1.4"

    @patch("ddpui.api.report_api.PdfExportService.generate_pdf")
    @patch("ddpui.api.report_api.ReportService.ensure_share_token")
    def test_admin_allowed_on_any_resource(self, mock_token, mock_pdf, org, admin, member):
        mock_token.return_value = "tok"
        mock_pdf.return_value = b"%PDF-1.4"
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = export_report_pdf(request, snapshot.id)
        assert response.content == b"%PDF-1.4"


# ================================================================================
# Report — share via email (gated on the parent report)
#
# `can_share_dashboards` is Analyst+ only (Member lacks it — see
# seed/003_role_permissions.json), so Analyst is the least-privileged actor
# that clears the `has_permission` decorator here; using Member would 403
# from the decorator, not from this task's gate.
# ================================================================================


class TestReportShareViaEmailGate:
    @patch("ddpui.api.report_api.send_report_email_task")
    def test_analyst_denied_on_private_resource_not_owned(self, mock_task, org, analyst, admin):
        snapshot = _snapshot(org, admin, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(analyst)
        payload = ReportShareViaEmailRequest(recipient_emails=["x@test.com"])

        with pytest.raises(HttpError) as exc_info:
            share_report_via_email(request, snapshot.id, payload)
        assert exc_info.value.status_code == 403
        mock_task.delay.assert_not_called()

    @patch("ddpui.api.report_api.send_report_email_task")
    def test_analyst_allowed_on_granted_resource(self, mock_task, org, analyst, admin):
        snapshot = _snapshot(org, admin, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, analyst)
        request = mock_request(analyst)
        payload = ReportShareViaEmailRequest(recipient_emails=["x@test.com"])

        share_report_via_email(request, snapshot.id, payload)
        mock_task.delay.assert_called_once()

    @patch("ddpui.api.report_api.send_report_email_task")
    def test_admin_allowed_on_any_resource(self, mock_task, org, admin, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)
        payload = ReportShareViaEmailRequest(recipient_emails=["x@test.com"])

        share_report_via_email(request, snapshot.id, payload)
        mock_task.delay.assert_called_once()


# ================================================================================
# Report — dashboard datetime-columns discovery (gated on the parent DASHBOARD,
# not the report — this sub-endpoint takes a dashboard_id).
# ================================================================================


class TestDashboardDatetimeColumnsGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            list_dashboard_datetime_columns(request, dashboard.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        response = list_dashboard_datetime_columns(request, dashboard.id)
        assert response["success"] is True

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        dashboard = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = list_dashboard_datetime_columns(request, dashboard.id)
        assert response["success"] is True


# ================================================================================
# Report — comment reads (gated on the parent report)
# ================================================================================


class TestReportCommentStatesGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_comment_states(request, snapshot.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = get_comment_states(request, snapshot.id)
        assert response["success"] is True

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_comment_states(request, snapshot.id)
        assert response["success"] is True


class TestReportListCommentsGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            list_comments(request, snapshot.id, target_type="summary")
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        snapshot = _snapshot(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "report", snapshot, member)
        request = mock_request(member)

        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["success"] is True

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        snapshot = _snapshot(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["success"] is True


# ================================================================================
# Metric — preview, consumers
# ================================================================================


class TestMetricPreviewGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            preview_metric(request, metric.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "metric", metric, member)
        request = mock_request(member)

        response = preview_metric(request, metric.id)
        assert response.error == "Warehouse not configured"

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        metric = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = preview_metric(request, metric.id)
        assert response.error == "Warehouse not configured"


class TestMetricConsumersGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_metric_consumers(request, metric.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        metric = _metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "metric", metric, member)
        request = mock_request(member)

        response = get_metric_consumers(request, metric.id)
        assert response.charts == []

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        metric = _metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_metric_consumers(request, metric.id)
        assert response.charts == []


# ================================================================================
# KPI — dashboards, consumers, data, notes
# ================================================================================


class TestKpiDashboardsGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_kpi_dashboards(request, kpi.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "kpi", kpi, member)
        request = mock_request(member)

        response = get_kpi_dashboards(request, kpi.id)
        assert response == []

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        kpi = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_kpi_dashboards(request, kpi.id)
        assert response == []


class TestKpiConsumersGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_kpi_consumers(request, kpi.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "kpi", kpi, member)
        request = mock_request(member)

        response = get_kpi_consumers(request, kpi.id)
        assert response["dashboards"] == []

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        kpi = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_kpi_consumers(request, kpi.id)
        assert response["dashboards"] == []


class TestKpiDataGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_kpi_data(request, kpi.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "kpi", kpi, member)
        request = mock_request(member)

        response = get_kpi_data(request, kpi.id)
        assert response.data == {}

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        kpi = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_kpi_data(request, kpi.id)
        assert response.data == {}


class TestKpiNotesGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            list_annotations(request, kpi.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        kpi = _kpi_with_metric(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "kpi", kpi, member)
        request = mock_request(member)

        response = list_annotations(request, kpi.id)
        assert response == []

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        kpi = _kpi_with_metric(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = list_annotations(request, kpi.id)
        assert response == []


# ================================================================================
# Alert — logs
# ================================================================================


class TestAlertLogsGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        alert = _alert(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_alert_logs(request, alert.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        alert = _alert(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "alert", alert, member)
        request = mock_request(member)

        response = get_alert_logs(request, alert.id)
        assert response.total == 0

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        alert = _alert(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = get_alert_logs(request, alert.id)
        assert response.total == 0


# ================================================================================
# Dashboard sweep — filter detail, duplication
# ================================================================================


class TestDashboardGetFilterGate:
    def test_member_denied_on_private_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        filter_obj = DashboardFilter.objects.create(
            dashboard=dashboard,
            name="Region",
            filter_type=DashboardFilterType.VALUE.value,
            schema_name="public",
            table_name="beneficiaries",
            column_name="region",
        )
        request = mock_request(member)

        with pytest.raises(HttpError) as exc_info:
            get_filter(request, dashboard.id, filter_obj.id)
        assert exc_info.value.status_code == 403

    def test_member_allowed_on_granted_resource(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        filter_obj = DashboardFilter.objects.create(
            dashboard=dashboard,
            name="Region",
            filter_type=DashboardFilterType.VALUE.value,
            schema_name="public",
            table_name="beneficiaries",
            column_name="region",
        )
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        response = get_filter(request, dashboard.id, filter_obj.id)
        assert response.id == filter_obj.id

    def test_admin_allowed_on_any_resource(self, org, admin, member):
        dashboard = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        filter_obj = DashboardFilter.objects.create(
            dashboard=dashboard,
            name="Region",
            filter_type=DashboardFilterType.VALUE.value,
            schema_name="public",
            table_name="beneficiaries",
            column_name="region",
        )
        request = mock_request(admin)

        response = get_filter(request, dashboard.id, filter_obj.id)
        assert response.id == filter_obj.id


class TestDashboardDuplicateGate:
    """Duplicating a dashboard clones its full content (filters, tabs) into a
    new dashboard the caller then owns outright — a permanent content
    exfiltration route, not just a read. Gated on the ORIGINAL dashboard's
    view permission. `can_create_dashboards` is Analyst+ only (Member lacks
    it), so Analyst is the least-privileged actor that clears the decorator.
    """

    def test_analyst_denied_on_private_resource_not_owned(self, org, analyst, admin):
        dashboard = _dashboard(org, admin, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(analyst)

        with pytest.raises(HttpError) as exc_info:
            duplicate_dashboard(request, dashboard.id)
        assert exc_info.value.status_code == 403
        assert Dashboard.objects.filter(org=org, title__startswith="Copy of").count() == 0

    def test_analyst_allowed_on_granted_resource(self, org, analyst, admin):
        dashboard = _dashboard(org, admin, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "dashboard", dashboard, analyst)
        request = mock_request(analyst)

        response = duplicate_dashboard(request, dashboard.id)
        assert response.title == f"Copy of {dashboard.title}"

    def test_admin_allowed_on_any_resource(self, org, admin, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(admin)

        response = duplicate_dashboard(request, dashboard.id)
        assert response.title == f"Copy of {dashboard.title}"
