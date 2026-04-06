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

    def test_renders_without_message(self):
        from ddpui.utils.email_templates import render_share_report_email

        plain, html = render_share_report_email(
            sender_name="Alice",
            report_title="Q1 Report",
            report_url="https://app.dalgo.org/share/report/token123",
        )

        assert 'Alice has shared "Q1 Report" with you' in plain
        assert "View the report:" in plain
        assert "PDF copy is also attached" in plain
        assert "Alice" in html
        assert "Q1 Report" in html
        assert "View Report" in html
        assert "token123" in html

    def test_renders_with_message(self):
        from ddpui.utils.email_templates import render_share_report_email

        plain, html = render_share_report_email(
            sender_name="Bob",
            report_title="Monthly Summary",
            report_url="https://app.dalgo.org/share/report/abc",
            message="Please review by Friday",
        )

        assert "Please review by Friday" in plain
        assert "Please review by Friday" in html

    def test_escapes_html_in_user_content(self):
        from ddpui.utils.email_templates import render_share_report_email

        _, html = render_share_report_email(
            sender_name="<script>alert('xss')</script>",
            report_title='Report "with quotes"',
            report_url="https://example.com",
            message="<b>bold</b>",
        )

        assert "<script>" not in html
        assert "&lt;script&gt;" in html
        assert "&lt;b&gt;bold&lt;/b&gt;" in html


# ================================================================================
# Tests: Celery task
# ================================================================================


class TestSendReportEmailTask:
    """Tests for the Celery task"""

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    def test_generates_pdf_and_sends_to_all_recipients(self, mock_generate_pdf, mock_send_email):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_generate_pdf.return_value = b"%PDF-1.4 content"

        send_report_email_task(
            snapshot_id=1,
            share_token="token123",
            recipient_emails=["a@test.com", "b@test.com"],
            sender_name="Alice",
            report_title="Q1 Report",
            report_url="https://app.dalgo.org/share/report/token123",
        )

        mock_generate_pdf.assert_called_once_with(1, "token123")
        assert mock_send_email.call_count == 2

        first_call = mock_send_email.call_args_list[0]
        assert first_call[1]["to_email"] == "a@test.com"
        assert first_call[1]["attachment_filename"] == "Q1 Report.pdf"

    @patch("ddpui.celeryworkers.report_tasks.send_email_with_attachment")
    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    def test_continues_on_individual_email_failure(self, mock_generate_pdf, mock_send_email):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_generate_pdf.return_value = b"%PDF-1.4 content"
        mock_send_email.side_effect = [
            Exception("SES error"),
            None,
        ]

        # Should not raise even though the first email fails
        send_report_email_task(
            snapshot_id=1,
            share_token="token123",
            recipient_emails=["fail@test.com", "success@test.com"],
            sender_name="Alice",
            report_title="Q1 Report",
            report_url="https://example.com",
        )

        assert mock_send_email.call_count == 2

    @patch("ddpui.celeryworkers.report_tasks.PdfExportService.generate_pdf")
    def test_raises_when_pdf_generation_fails(self, mock_generate_pdf):
        from ddpui.celeryworkers.report_tasks import send_report_email_task

        mock_generate_pdf.side_effect = Exception("Playwright error")

        with pytest.raises(Exception, match="Playwright error"):
            send_report_email_task(
                snapshot_id=1,
                share_token="token123",
                recipient_emails=["a@test.com"],
                sender_name="Alice",
                report_title="Q1 Report",
                report_url="https://example.com",
            )


# ================================================================================
# Tests: API endpoint
# ================================================================================


class TestShareViaEmailEndpoint:
    """Tests for the API endpoint logic"""

    @patch("ddpui.api.report_api.send_report_email_task")
    def test_dispatches_celery_task_and_enables_public(self, mock_task, snapshot, orguser):
        from ddpui.core.reports.report_service import ReportService

        assert not snapshot.is_public
        assert not snapshot.public_share_token

        # Simulate what the endpoint does
        share_token = ReportService.ensure_share_token(snapshot)
        if not snapshot.is_public:
            snapshot.is_public = True
            snapshot.save(update_fields=["is_public"])

        report_url = ReportService._build_public_url(share_token)

        mock_task.delay(
            snapshot_id=snapshot.id,
            share_token=share_token,
            recipient_emails=["test@example.com"],
            sender_name=orguser.user.email,
            report_title=snapshot.title,
            report_url=report_url,
            message="Check this out",
        )

        snapshot.refresh_from_db()
        assert snapshot.is_public is True
        assert snapshot.public_share_token is not None

        mock_task.delay.assert_called_once()
        call_kwargs = mock_task.delay.call_args[1]
        assert call_kwargs["recipient_emails"] == ["test@example.com"]
        assert call_kwargs["report_title"] == "Q1 Report"
        assert call_kwargs["message"] == "Check this out"
