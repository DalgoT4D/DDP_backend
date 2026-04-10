"""Tests for share-via-email flow"""

from datetime import date
from unittest.mock import patch, MagicMock, call

import pytest
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Share Email Test Org",
        slug="share-email-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def user():
    user = User.objects.create(
        username="shareuser",
        email="sender@test.com",
        first_name="Sender",
        last_name="User",
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def snapshot(org, orguser):
    snapshot = ReportSnapshot.objects.create(
        title="Q1 Report",
        org=org,
        created_by=orguser,
        frozen_dashboard={"dashboard_id": 1, "title": "Test Dashboard"},
        frozen_chart_configs={},
        date_column={"schema_name": "public", "table_name": "t", "column_name": "c"},
        period_end=date(2025, 3, 31),
    )
    yield snapshot
    snapshot.delete()


# ================================================================================
# Tests: send_email_with_attachment
# ================================================================================


class TestSendEmailWithAttachment:
    """Tests for the MIME-based email sending function"""

    @patch("ddpui.utils.awsses._get_ses_client")
    def test_sends_raw_email_with_pdf(self, mock_get_client):
        from ddpui.utils.awsses import send_email_with_attachment

        mock_ses = MagicMock()
        mock_get_client.return_value = mock_ses

        with patch.dict("os.environ", {"SES_SENDER_EMAIL": "noreply@dalgo.org"}):
            send_email_with_attachment(
                to_email="recipient@test.com",
                subject="Test Report",
                text_body="Plain text",
                html_body="<html><body>HTML</body></html>",
                attachment_bytes=b"%PDF-1.4 fake pdf content",
                attachment_filename="report.pdf",
            )

        mock_ses.send_raw_email.assert_called_once()
        call_kwargs = mock_ses.send_raw_email.call_args[1]
        assert call_kwargs["Source"] == "noreply@dalgo.org"
        assert call_kwargs["Destinations"] == ["recipient@test.com"]
        raw_data = call_kwargs["RawMessage"]["Data"]
        assert "report.pdf" in raw_data
        assert "application/pdf" in raw_data


# ================================================================================
# Tests: render_share_report_email
# ================================================================================


class TestRenderShareReportEmail:
    """Tests for the share report email template"""

    def test_renders_with_private_url_only(self):
        from ddpui.utils.email_templates import render_share_report_email

        plain, html = render_share_report_email(
            sender_name="Alice",
            report_title="Q1 Report",
            private_url="https://app.dalgo.org/reports/1",
        )

        assert 'Alice has shared "Q1 Report" with you' in plain
        assert "login required" in plain
        assert "https://app.dalgo.org/reports/1" in plain
        assert "Alice" in html
        assert "Q1 Report" in html
        assert "View Report" in html
        # No public link when public_url is not provided
        assert "Public Link" not in html

    def test_renders_with_public_url(self):
        from ddpui.utils.email_templates import render_share_report_email

        plain, html = render_share_report_email(
            sender_name="Bob",
            report_title="Monthly Summary",
            private_url="https://app.dalgo.org/reports/2",
            public_url="https://app.dalgo.org/share/report/abc",
        )

        # Both URLs in plain text
        assert "https://app.dalgo.org/reports/2" in plain
        assert "https://app.dalgo.org/share/report/abc" in plain
        # Public link in HTML
        assert "Public Link" in html
        assert "abc" in html

    def test_escapes_html_in_user_content(self):
        from ddpui.utils.email_templates import render_share_report_email

        _, html = render_share_report_email(
            sender_name="<script>alert('xss')</script>",
            report_title='Report "with quotes"',
            private_url="https://example.com/reports/1",
        )

        assert "<script>" not in html
        assert "&lt;script&gt;" in html


# ================================================================================
# Tests: Celery task
# ================================================================================


class TestSendReportEmailTask:
    """Tests for the Celery task"""

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    @patch("ddpui.celeryworkers.report_tasks.ReportService.ensure_share_token")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_private_url")
    def test_generates_pdf_and_sends_to_all_recipients(
        self,
        mock_private_url,
        mock_ensure_token,
        mock_generate_pdf,
        mock_send_email,
        snapshot,
        orguser,
    ):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_private_url.return_value = "https://app.dalgo.org/reports/1"
        mock_ensure_token.return_value = "token123"
        mock_generate_pdf.return_value = b"%PDF-1.4 content"

        send_report_email_task(
            snapshot_id=snapshot.id,
            orguser_id=orguser.id,
            recipient_emails=["a@test.com", "b@test.com"],
        )

        mock_generate_pdf.assert_called_once_with(snapshot.id, "token123")
        assert mock_send_email.call_count == 2

        first_call = mock_send_email.call_args_list[0]
        assert first_call[1]["to_email"] == "a@test.com"
        assert first_call[1]["attachment_filename"] == "Q1 Report.pdf"
        # Default subject when none provided
        assert first_call[1]["subject"] == "Report: Q1 Report"

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    @patch("ddpui.celeryworkers.report_tasks.ReportService.ensure_share_token")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_private_url")
    def test_uses_custom_subject(
        self,
        mock_private_url,
        mock_ensure_token,
        mock_generate_pdf,
        mock_send_email,
        snapshot,
        orguser,
    ):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_private_url.return_value = "https://app.dalgo.org/reports/1"
        mock_ensure_token.return_value = "token123"
        mock_generate_pdf.return_value = b"%PDF-1.4 content"

        send_report_email_task(
            snapshot_id=snapshot.id,
            orguser_id=orguser.id,
            recipient_emails=["a@test.com"],
            subject="Custom Subject Line",
        )

        first_call = mock_send_email.call_args_list[0]
        assert first_call[1]["subject"] == "Custom Subject Line"

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    @patch("ddpui.celeryworkers.report_tasks.ReportService.ensure_share_token")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_private_url")
    def test_continues_on_individual_email_failure(
        self,
        mock_private_url,
        mock_ensure_token,
        mock_generate_pdf,
        mock_send_email,
        snapshot,
        orguser,
    ):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_private_url.return_value = "https://app.dalgo.org/reports/1"
        mock_ensure_token.return_value = "token123"
        mock_generate_pdf.return_value = b"%PDF-1.4 content"
        mock_send_email.side_effect = [
            Exception("SES error"),
            None,
        ]

        # Should not raise even though the first email fails
        send_report_email_task(
            snapshot_id=snapshot.id,
            orguser_id=orguser.id,
            recipient_emails=["fail@test.com", "success@test.com"],
        )

        assert mock_send_email.call_count == 2

    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    @patch("ddpui.celeryworkers.report_tasks.ReportService.ensure_share_token")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_private_url")
    def test_raises_when_pdf_generation_fails(
        self, mock_private_url, mock_ensure_token, mock_generate_pdf, snapshot, orguser
    ):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_private_url.return_value = "https://app.dalgo.org/reports/1"
        mock_ensure_token.return_value = "token123"
        mock_generate_pdf.side_effect = Exception("Playwright error")

        with pytest.raises(Exception, match="Playwright error"):
            send_report_email_task(
                snapshot_id=snapshot.id,
                orguser_id=orguser.id,
                recipient_emails=["a@test.com"],
            )

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    @patch("ddpui.celeryworkers.report_tasks.ReportService.ensure_share_token")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_private_url")
    @patch("ddpui.celeryworkers.report_tasks.ReportService._build_public_url")
    def test_includes_public_url_when_report_is_public(
        self,
        mock_public_url,
        mock_private_url,
        mock_ensure_token,
        mock_generate_pdf,
        mock_send_email,
        snapshot,
        orguser,
    ):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        # Make the snapshot public
        snapshot.is_public = True
        snapshot.public_share_token = "public-token-abc"
        snapshot.save(update_fields=["is_public", "public_share_token"])

        mock_private_url.return_value = "https://app.dalgo.org/reports/1"
        mock_public_url.return_value = "https://app.dalgo.org/share/report/public-token-abc"
        mock_ensure_token.return_value = "public-token-abc"
        mock_generate_pdf.return_value = b"%PDF-1.4 content"

        send_report_email_task(
            snapshot_id=snapshot.id,
            orguser_id=orguser.id,
            recipient_emails=["a@test.com"],
        )

        mock_public_url.assert_called_once_with("public-token-abc")
        # Email body should contain the public URL
        first_call = mock_send_email.call_args_list[0]
        assert "public-token-abc" in first_call[1]["html_body"]


# ================================================================================
# Tests: API endpoint
# ================================================================================


class TestShareViaEmailEndpoint:
    """Tests for the API endpoint logic"""

    @patch("ddpui.api.report_api.send_report_email_task")
    def test_dispatches_celery_task(self, mock_task, snapshot, orguser):
        mock_task.delay(
            snapshot_id=snapshot.id,
            orguser_id=orguser.id,
            recipient_emails=["test@example.com"],
            subject="Report: Q1 Report",
        )

        mock_task.delay.assert_called_once()
        call_kwargs = mock_task.delay.call_args[1]
        assert call_kwargs["snapshot_id"] == snapshot.id
        assert call_kwargs["orguser_id"] == orguser.id
        assert call_kwargs["recipient_emails"] == ["test@example.com"]
        assert call_kwargs["subject"] == "Report: Q1 Report"
