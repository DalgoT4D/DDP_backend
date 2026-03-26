"""Tests for mention notification dispatch in MentionService"""

from datetime import date
from unittest.mock import patch, MagicMock

import pytest
from django.conf import settings
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.models.comment import Comment, CommentTargetType
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.userpreferences import UserPreferences
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.reports.mention_service import MentionService
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

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_process_mentions_creates_notification(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """Comment with @mention creates Notification + NotificationRecipient"""
        MentionService.process_mentions(
            comment_with_mention, org, author_orguser, [mentioned_orguser.user.email]
        )

        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1

        notification = Notification.objects.first()
        assert "mentioned you" in notification.message
        assert "Test Report Q1" in notification.message
        assert notification.author == author_orguser.user.email

        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == mentioned_orguser
        assert recipient.read_status is False

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_self_mention_creates_notification(self, mock_send, snapshot, author_orguser, org):
        """Author mentioning themselves still creates a notification."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Note to self @{author_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        MentionService.process_mentions(comment, org, author_orguser, [author_orguser.user.email])

        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == author_orguser

        comment.delete()

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_update_notifies_all_mentioned_users(
        self,
        mock_send,
        snapshot,
        author_orguser,
        org,
        mentioned_orguser,
        second_mentioned_orguser,
    ):
        """On edit, all mentioned users get notifications (content may have changed)."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{mentioned_orguser.user.email} and @{second_mentioned_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        MentionService.process_mentions(
            comment,
            org,
            author_orguser,
            [mentioned_orguser.user.email, second_mentioned_orguser.user.email],
        )

        # Both mentioned users should get notifications
        assert Notification.objects.count() == 2
        recipients = set(NotificationRecipient.objects.values_list("recipient_id", flat=True))
        assert mentioned_orguser.id in recipients
        assert second_mentioned_orguser.id in recipients

        comment.delete()

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_update_same_mentions_still_notifies(
        self, mock_send, snapshot, author_orguser, org, mentioned_orguser
    ):
        """Edit with same mentions still creates notifications (content may have changed)."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{mentioned_orguser.user.email}",
            author=author_orguser,
            org=org,
        )

        MentionService.process_mentions(
            comment, org, author_orguser, [mentioned_orguser.user.email]
        )

        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == mentioned_orguser

        comment.delete()

    @patch("ddpui.core.reports.mention_service.send_html_message")
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

        MentionService.process_mentions(
            comment,
            org,
            author_orguser,
            [mentioned_orguser.user.email, second_mentioned_orguser.user.email],
        )

        assert Notification.objects.count() == 2
        assert NotificationRecipient.objects.count() == 2

        recipients = set(NotificationRecipient.objects.values_list("recipient_id", flat=True))
        assert recipients == {mentioned_orguser.id, second_mentioned_orguser.id}

        comment.delete()


# ================================================================================
# Tests: Email sending
# ================================================================================


class TestMentionEmail:
    """Tests for email sending on @mention"""

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_email_sent_when_preference_enabled(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """Email is sent when user has enable_email_notifications=True"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )

        MentionService.process_mentions(
            comment_with_mention, org, author_orguser, [mentioned_orguser.user.email]
        )

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert call_kwargs[1]["to_email"] == mentioned_orguser.user.email
        assert "mentioned" in call_kwargs[1]["subject"].lower()

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_email_not_sent_when_preference_disabled(
        self, mock_send, comment_with_mention, org, author_orguser, mentioned_orguser
    ):
        """No email when enable_email_notifications=False, but in-app notification still created"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": False},
        )

        MentionService.process_mentions(
            comment_with_mention, org, author_orguser, [mentioned_orguser.user.email]
        )

        mock_send.assert_not_called()
        # In-app notification should still exist
        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1

    @patch(
        "ddpui.core.reports.mention_service.send_html_message",
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
        MentionService.process_mentions(
            comment_with_mention, org, author_orguser, [mentioned_orguser.user.email]
        )

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
        assert "Q1 Report" in plain
        assert "Check this number" in plain
        assert "http://localhost:3001/reports/42" in plain

        # HTML checks
        assert "Noopur Raval" in html_body
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

    def test_render_strips_mention_prefix(self):
        """@email mentions have the @ prefix stripped to prevent auto-linking"""
        plain, html_body = render_mention_email(
            author_name="Author",
            author_email="author@test.com",
            comment_excerpt="Hey @mentioned@test.com check this",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/42",
        )

        # The @ prefix should be stripped from mentions
        assert "@mentioned@test.com" not in plain
        assert "mentioned@test.com" in plain
        assert "@mentioned@test.com" not in html_body
        assert "mentioned@test.com" in html_body

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
# Tests: _build_report_url
# ================================================================================


class TestBuildReportUrl:
    """Tests for MentionService._build_report_url"""

    def test_summary_comment_url(self, snapshot, author_orguser, org):
        """Summary comment builds URL with commentTarget=summary"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="test",
            author=author_orguser,
            org=org,
        )
        url = MentionService._build_report_url(comment)
        assert f"/reports/{snapshot.id}" in url
        assert "commentTarget=summary" in url
        comment.delete()

    def test_chart_comment_url(self, snapshot, author_orguser, org):
        """Chart comment builds URL with commentTarget=chart and chartId"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=1,
            content="test",
            author=author_orguser,
            org=org,
        )
        url = MentionService._build_report_url(comment)
        assert f"/reports/{snapshot.id}" in url
        assert "commentTarget=chart" in url
        assert "chartId=1" in url
        comment.delete()

    @patch.object(settings, "FRONTEND_URL_V2", "https://app.dalgo.in")
    def test_uses_frontend_url_v2(self, snapshot, author_orguser, org):
        """Prefers FRONTEND_URL_V2 setting"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="test",
            author=author_orguser,
            org=org,
        )
        url = MentionService._build_report_url(comment)
        assert url.startswith("https://app.dalgo.in/")
        comment.delete()


# ================================================================================
# Tests: _resolve_chart_name
# ================================================================================


class TestResolveChartName:
    """Tests for MentionService._resolve_chart_name"""

    def test_returns_chart_title(self, snapshot, author_orguser, org):
        """Returns chart title from frozen_chart_configs"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=1,
            content="test",
            author=author_orguser,
            org=org,
        )
        result = MentionService._resolve_chart_name(comment)
        assert result == "Chart 1"
        comment.delete()

    def test_returns_none_for_summary(self, snapshot, author_orguser, org):
        """Returns None for summary comments"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="test",
            author=author_orguser,
            org=org,
        )
        result = MentionService._resolve_chart_name(comment)
        assert result is None
        comment.delete()

    def test_returns_none_for_missing_chart_id(self, snapshot, author_orguser, org):
        """Returns None when chart_id not in frozen_chart_configs"""
        comment = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=999,
            content="test",
            author=author_orguser,
            org=org,
        )
        result = MentionService._resolve_chart_name(comment)
        assert result is None
        comment.delete()


# ================================================================================
# Tests: _create_in_app_notification
# ================================================================================


class TestCreateInAppNotification:
    """Tests for MentionService._create_in_app_notification"""

    def test_creates_notification_and_recipient(self, author_orguser, mentioned_orguser):
        """Creates both Notification and NotificationRecipient records"""
        MentionService._create_in_app_notification(
            author=author_orguser,
            mentioned_user=mentioned_orguser,
            message="Test message",
            email_subject="Test subject",
        )

        assert Notification.objects.count() == 1
        notification = Notification.objects.first()
        assert notification.message == "Test message"
        assert notification.email_subject == "Test subject"
        assert notification.author == author_orguser.user.email
        assert notification.urgent is False

        assert NotificationRecipient.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == mentioned_orguser
        assert recipient.notification == notification


# ================================================================================
# Tests: _send_email_notification
# ================================================================================


class TestSendEmailNotification:
    """Tests for MentionService._send_email_notification"""

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_sends_email_when_preference_enabled(
        self, mock_send, snapshot, author_orguser, mentioned_orguser, org
    ):
        """Sends email when user has email notifications enabled"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey check this",
            author=author_orguser,
            org=org,
        )

        MentionService._send_email_notification(
            comment=comment,
            author=author_orguser,
            mentioned_user=mentioned_orguser,
            author_email=author_orguser.user.email,
            email_subject="Test subject",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/1",
            chart_name=None,
        )

        mock_send.assert_called_once()
        assert mock_send.call_args[1]["to_email"] == mentioned_orguser.user.email
        comment.delete()

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_skips_email_when_preference_disabled(
        self, mock_send, snapshot, author_orguser, mentioned_orguser, org
    ):
        """Does not send email when user has email notifications disabled"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": False},
        )
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey check this",
            author=author_orguser,
            org=org,
        )

        MentionService._send_email_notification(
            comment=comment,
            author=author_orguser,
            mentioned_user=mentioned_orguser,
            author_email=author_orguser.user.email,
            email_subject="Test subject",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/1",
            chart_name=None,
        )

        mock_send.assert_not_called()
        comment.delete()

    @patch(
        "ddpui.core.reports.mention_service.send_html_message",
        side_effect=Exception("SES down"),
    )
    def test_email_failure_logged_not_raised(
        self, mock_send, snapshot, author_orguser, mentioned_orguser, org
    ):
        """Email failure is caught and logged, does not raise"""
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey check this",
            author=author_orguser,
            org=org,
        )

        # Should not raise
        MentionService._send_email_notification(
            comment=comment,
            author=author_orguser,
            mentioned_user=mentioned_orguser,
            author_email=author_orguser.user.email,
            email_subject="Test subject",
            snapshot_title="Q1 Report",
            report_url="http://localhost:3001/reports/1",
            chart_name=None,
        )
        comment.delete()


# ================================================================================
# Tests: _get_thread_context
# ================================================================================


class TestGetThreadContext:
    """Tests for MentionService._get_thread_context"""

    def test_returns_prior_comments(self, snapshot, author_orguser, org):
        """Returns prior comments on the same target, oldest first"""
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First comment",
            author=author_orguser,
            org=org,
        )
        c2 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Second comment",
            author=author_orguser,
            org=org,
        )
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Current comment",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current)
        assert len(thread) == 2
        assert thread[0]["content"] == "First comment"
        assert thread[1]["content"] == "Second comment"

        current.delete()
        c2.delete()
        c1.delete()

    def test_limits_to_max_prior(self, snapshot, author_orguser, org):
        """Respects max_prior limit, returns most recent N"""
        comments = []
        for i in range(5):
            comments.append(
                Comment.objects.create(
                    target_type=CommentTargetType.SUMMARY,
                    snapshot=snapshot,
                    content=f"Comment {i}",
                    author=author_orguser,
                    org=org,
                )
            )
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Current",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current, max_prior=2)
        assert len(thread) == 2
        # Should be the 2 most recent before current
        assert thread[0]["content"] == "Comment 3"
        assert thread[1]["content"] == "Comment 4"

        current.delete()
        for c in reversed(comments):
            c.delete()

    def test_excludes_deleted_comments(self, snapshot, author_orguser, org):
        """Deleted comments are not included in thread context"""
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Visible comment",
            author=author_orguser,
            org=org,
        )
        c_deleted = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Deleted comment",
            author=author_orguser,
            org=org,
            is_deleted=True,
        )
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Current",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current)
        assert len(thread) == 1
        assert thread[0]["content"] == "Visible comment"

        current.delete()
        c_deleted.delete()
        c1.delete()

    def test_truncates_long_content(self, snapshot, author_orguser, org):
        """Content longer than 200 chars is truncated with ellipsis"""
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="A" * 300,
            author=author_orguser,
            org=org,
        )
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Current",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current)
        assert len(thread) == 1
        assert len(thread[0]["content"]) == 203  # 200 + "..."
        assert thread[0]["content"].endswith("...")

        current.delete()
        c1.delete()

    def test_empty_when_no_prior_comments(self, snapshot, author_orguser, org):
        """Returns empty list when there are no prior comments"""
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First ever comment",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current)
        assert thread == []

        current.delete()

    def test_chart_comments_isolated(self, snapshot, author_orguser, org):
        """Chart thread context only includes comments on the same chart"""
        c_other_chart = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=99,
            content="Other chart comment",
            author=author_orguser,
            org=org,
        )
        c_same_chart = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=1,
            content="Same chart comment",
            author=author_orguser,
            org=org,
        )
        current = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=1,
            content="Current",
            author=author_orguser,
            org=org,
        )

        thread = MentionService._get_thread_context(current)
        assert len(thread) == 1
        assert thread[0]["content"] == "Same chart comment"

        current.delete()
        c_same_chart.delete()
        c_other_chart.delete()


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
