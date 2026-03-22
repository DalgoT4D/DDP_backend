"""Tests for mention notification dispatch in MentionService"""

from datetime import date
from unittest.mock import patch, MagicMock

import pytest
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.models.comment import Comment, CommentTargetType
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.userpreferences import UserPreferences
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.comments.mention_service import MentionService
from ddpui.utils.email_templates import render_mention_email
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Comment Test Org", slug="comment-test-org", airbyte_workspace_id="workspace-id"
    )
    yield org
    org.delete()


@pytest.fixture
def author_user():
    user = User.objects.create(
        username="commentauthor",
        email="author@test.com",
        first_name="Author",
        last_name="User",
    )
    yield user
    user.delete()


@pytest.fixture
def author_orguser(author_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=author_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def mentioned_user():
    user = User.objects.create(
        username="mentioneduser",
        email="mentioned@test.com",
        first_name="Mentioned",
        last_name="Person",
    )
    yield user
    user.delete()


@pytest.fixture
def mentioned_orguser(mentioned_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=mentioned_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def second_mentioned_user():
    user = User.objects.create(
        username="secondmentioned",
        email="second@test.com",
        first_name="Second",
        last_name="User",
    )
    yield user
    user.delete()


@pytest.fixture
def second_mentioned_orguser(second_mentioned_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=second_mentioned_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def snapshot(org, author_orguser):
    snapshot = ReportSnapshot.objects.create(
        title="Test Report Q1",
        date_column={},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={},
        frozen_chart_configs={"1": {"chartType": "bar", "title": "Chart 1"}},
        created_by=author_orguser,
        org=org,
    )
    yield snapshot
    snapshot.delete()


@pytest.fixture
def comment_with_mention(snapshot, author_orguser, org, mentioned_orguser):
    comment = Comment.objects.create(
        target_type=CommentTargetType.SUMMARY,
        snapshot=snapshot,
        content=f"Check this out @{mentioned_orguser.user.email}",
        author=author_orguser,
        org=org,
    )
    yield comment
    try:
        comment.refresh_from_db()
        comment.delete()
    except Comment.DoesNotExist:
        pass


# ================================================================================
# Tests: Notification creation
# ================================================================================


class TestMentionNotifications:
    """Tests for notification dispatch when users are @mentioned"""

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_process_mentions_creates_notification(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """Comment with @mention creates Notification + NotificationRecipient"""
        MentionService.process_mentions(comment_with_mention, org, author_orguser)

        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1

        notification = Notification.objects.first()
        assert "mentioned you" in notification.message
        assert "Test Report Q1" in notification.message
        assert notification.author == author_orguser.user.email

        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == mentioned_orguser
        assert recipient.read_status is False

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_self_mention_skipped(self, mock_send, snapshot, author_orguser, org):
        """Author mentioning themselves does not create a notification"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Note to self @{author_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        MentionService.process_mentions(comment, org, author_orguser)

        assert Notification.objects.count() == 0
        assert NotificationRecipient.objects.count() == 0
        mock_send.assert_not_called()

        comment.delete()

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_update_only_notifies_new_mentions(
        self,
        mock_send,
        snapshot,
        author_orguser,
        org,
        mentioned_orguser,
        second_mentioned_orguser,
    ):
        """On edit, only users NOT in previous_emails trigger notifications"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{mentioned_orguser.user.email} and @{second_mentioned_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        previous_emails = [mentioned_orguser.user.email]
        MentionService.process_mentions(
            comment, org, author_orguser, previous_emails=previous_emails
        )

        # Only second_mentioned_orguser should get a notification
        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == second_mentioned_orguser

        comment.delete()

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_update_same_mentions_no_notification(
        self, mock_send, snapshot, author_orguser, org, mentioned_orguser
    ):
        """Edit with same mentions does not create new notifications"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{mentioned_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        previous_emails = [mentioned_orguser.user.email]
        MentionService.process_mentions(
            comment, org, author_orguser, previous_emails=previous_emails
        )

        assert Notification.objects.count() == 0
        assert NotificationRecipient.objects.count() == 0
        mock_send.assert_not_called()

        comment.delete()

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_multiple_mentions_create_separate_notifications(
        self,
        mock_send,
        snapshot,
        author_orguser,
        org,
        mentioned_orguser,
        second_mentioned_orguser,
    ):
        """Multiple mentions in one comment create separate notifications"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{mentioned_orguser.user.email} and @{second_mentioned_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        MentionService.process_mentions(comment, org, author_orguser)

        assert Notification.objects.count() == 2
        assert NotificationRecipient.objects.count() == 2

        recipients = set(
            NotificationRecipient.objects.values_list("recipient_id", flat=True)
        )
        assert recipients == {mentioned_orguser.id, second_mentioned_orguser.id}

        comment.delete()


# ================================================================================
# Tests: Email sending
# ================================================================================


class TestMentionEmail:
    """Tests for email sending on @mention"""

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_email_sent_when_preference_enabled(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """Email is sent when user has enable_email_notifications=True"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )

        MentionService.process_mentions(comment_with_mention, org, author_orguser)

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert call_kwargs[1]["to_email"] == mentioned_orguser.user.email
        assert "mentioned" in call_kwargs[1]["subject"].lower()

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_email_not_sent_when_preference_disabled(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """No email when enable_email_notifications=False, but in-app notification still created"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": False},
        )

        MentionService.process_mentions(comment_with_mention, org, author_orguser)

        mock_send.assert_not_called()
        # In-app notification should still exist
        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1

    @patch(
        "ddpui.core.comments.mention_service.send_html_message",
        side_effect=Exception("SES error"),
    )
    def test_email_failure_doesnt_block_notification(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """Email send failure does not prevent notification record creation"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )

        # Should not raise
        MentionService.process_mentions(comment_with_mention, org, author_orguser)

        # Notification still created
        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1


# ================================================================================
# Tests: Email template rendering
# ================================================================================


class TestRenderMentionEmail:
    """Tests for the HTML email template"""

    def test_render_returns_both_formats(self):
        """render_mention_email returns a (plain_text, html) tuple"""
        plain, html_body = render_mention_email(
            author_name="Noopur Raval",
            author_email="noopur@test.com",
            comment_excerpt="This looks great",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/42",
        )

        assert isinstance(plain, str)
        assert isinstance(html_body, str)
        assert len(plain) > 0
        assert len(html_body) > 0

    def test_render_contains_expected_content(self):
        """Template includes author, title, excerpt, and URL"""
        plain, html_body = render_mention_email(
            author_name="Noopur Raval",
            author_email="noopur@test.com",
            comment_excerpt="Check this number",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/42",
        )

        # Plain text checks
        assert "Noopur Raval" in plain
        assert "noopur@test.com" in plain
        assert "Q1 Report" in plain
        assert "Check this number" in plain
        assert "http://localhost:3001/reports/42" in plain

        # HTML checks
        assert "Noopur Raval" in html_body
        assert "noopur@test.com" in html_body
        assert "Q1 Report" in html_body
        assert "Check this number" in html_body
        assert "http://localhost:3001/reports/42" in html_body

    def test_render_with_chart_name(self):
        """Template includes chart name when provided"""
        plain, html_body = render_mention_email(
            author_name="Noopur Raval",
            author_email="noopur@test.com",
            comment_excerpt="Check the bar values",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/42",
            chart_name="Revenue by Region",
        )

        # Plain text should mention chart name and report title
        assert "Revenue by Region" in plain
        assert "Q1 Report" in plain

        # HTML should mention chart name and report title
        assert "Revenue by Region" in html_body
        assert "Q1 Report" in html_body

    def test_render_without_chart_name(self):
        """Template omits chart name when None (summary-level comment)"""
        plain, html_body = render_mention_email(
            author_name="Noopur Raval",
            author_email="noopur@test.com",
            comment_excerpt="Looks good",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/42",
            chart_name=None,
        )

        # Should still contain the report title
        assert "Q1 Report" in plain
        assert "Q1 Report" in html_body

    def test_render_escapes_html_in_user_content(self):
        """User-generated content is HTML-escaped to prevent XSS"""
        plain, html_body = render_mention_email(
            author_name='<script>alert("xss")</script>',
            author_email="attacker@test.com",
            comment_excerpt="<img src=x onerror=alert(1)>",
            snapshot_title="Report <b>bold</b>",
            report_url="http://localhost:3001/reports/42",
        )

        # HTML should contain escaped versions
        assert "&lt;script&gt;" in html_body
        assert "<script>" not in html_body
        assert "&lt;img" in html_body
        assert "&lt;b&gt;" in html_body

        # Plain text should have the raw content (not escaped)
        assert "<script>" in plain


# ================================================================================
# Tests: send_html_message
# ================================================================================


class TestSendHtmlMessage:
    """Tests for the send_html_message SES function"""

    @patch("ddpui.utils.awsses._get_ses_client")
    def test_send_html_message_calls_ses_with_both_bodies(self, mock_get_client):
        """send_html_message sends both Text and Html body to SES"""
        from ddpui.utils.awsses import send_html_message as send_html

        mock_ses = MagicMock()
        mock_get_client.return_value = mock_ses

        send_html(
            to_email="user@test.com",
            subject="Test Subject",
            text_body="Plain text",
            html_body="<h1>HTML</h1>",
        )

        mock_ses.send_email.assert_called_once()
        call_args = mock_ses.send_email.call_args[1]
        assert call_args["Message"]["Body"]["Text"]["Data"] == "Plain text"
        assert call_args["Message"]["Body"]["Html"]["Data"] == "<h1>HTML</h1>"
        assert call_args["Message"]["Subject"]["Data"] == "Test Subject"
        assert call_args["Destination"]["ToAddresses"] == ["user@test.com"]
