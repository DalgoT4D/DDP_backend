"""Tests for MentionService — process_mentions flow and store_mentioned_emails

Complements test_mention_notifications.py which covers notification dispatch,
email rendering, URL building, chart name resolution, and thread context.
This file focuses on the process_mentions orchestration logic and
store_mentioned_emails.
"""

import os
import django
from datetime import date
from unittest.mock import patch, MagicMock

import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

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
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Mention Svc Org", slug="mention-svc-org", airbyte_workspace_id="workspace-id"
    )
    yield org
    org.delete()


@pytest.fixture
def author_user():
    user = User.objects.create(
        username="msvc_author",
        email="msvc_author@test.com",
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
        username="msvc_mentioned",
        email="msvc_mentioned@test.com",
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
def snapshot(org, author_orguser):
    snapshot = ReportSnapshot.objects.create(
        title="Mention Svc Report",
        date_column={},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={},
        frozen_chart_configs={"10": {"title": "Chart A"}},
        created_by=author_orguser,
        org=org,
    )
    yield snapshot
    snapshot.delete()


@pytest.fixture
def comment(snapshot, author_orguser, org):
    comment = Comment.objects.create(
        target_type=CommentTargetType.SUMMARY,
        snapshot=snapshot,
        content="Test comment with mention",
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
# Tests: process_mentions
# ================================================================================


class TestProcessMentions:
    """Tests for MentionService.process_mentions orchestration"""

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_stores_and_notifies(self, mock_send, comment, org, author_orguser, mentioned_orguser):
        result = MentionService.process_mentions(
            comment, org, author_orguser, [mentioned_orguser.user.email]
        )
        assert len(result) == 1
        assert result[0].user.email == mentioned_orguser.user.email

        comment.refresh_from_db()
        assert mentioned_orguser.user.email in comment.mentioned_emails
        assert Notification.objects.count() == 1

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_empty_emails_noop(self, mock_send, comment, org, author_orguser):
        result = MentionService.process_mentions(comment, org, author_orguser, [])
        assert result == []
        assert Notification.objects.count() == 0

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_invalid_emails_ignored(self, mock_send, comment, org, author_orguser):
        result = MentionService.process_mentions(
            comment, org, author_orguser, ["nonexistent@test.com"]
        )
        assert result == []
        assert Notification.objects.count() == 0

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_filters_valid_only(self, mock_send, comment, org, author_orguser, mentioned_orguser):
        result = MentionService.process_mentions(
            comment,
            org,
            author_orguser,
            [mentioned_orguser.user.email, "invalid@test.com"],
        )
        assert len(result) == 1
        assert result[0].user.email == mentioned_orguser.user.email

        comment.refresh_from_db()
        assert mentioned_orguser.user.email in comment.mentioned_emails
        assert "invalid@test.com" not in comment.mentioned_emails


# ================================================================================
# Tests: store_mentioned_emails
# ================================================================================


class TestStoreMentionedEmails:
    """Tests for MentionService.store_mentioned_emails"""

    def test_stores_emails_on_comment(self, comment, mentioned_orguser):
        MentionService.store_mentioned_emails(comment, [mentioned_orguser])
        comment.refresh_from_db()
        assert mentioned_orguser.user.email in comment.mentioned_emails

    def test_deduplicates_emails(self, comment, mentioned_orguser):
        MentionService.store_mentioned_emails(comment, [mentioned_orguser, mentioned_orguser])
        comment.refresh_from_db()
        assert comment.mentioned_emails.count(mentioned_orguser.user.email) == 1


# ================================================================================
# Tests: notify_mentioned_users
# ================================================================================


class TestNotifyMentionedUsers:
    """Tests for MentionService.notify_mentioned_users"""

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_creates_in_app_notification(
        self, mock_send, comment, org, author_orguser, mentioned_orguser
    ):
        MentionService.notify_mentioned_users(
            comment=comment,
            org=org,
            author=author_orguser,
            mentioned_users=[mentioned_orguser],
        )
        assert Notification.objects.count() == 1
        assert NotificationRecipient.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient == mentioned_orguser

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_sends_email_when_enabled(
        self, mock_send, comment, org, author_orguser, mentioned_orguser
    ):
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )
        MentionService.notify_mentioned_users(
            comment=comment,
            org=org,
            author=author_orguser,
            mentioned_users=[mentioned_orguser],
        )
        mock_send.assert_called_once()

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_skips_email_when_disabled(
        self, mock_send, comment, org, author_orguser, mentioned_orguser
    ):
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": False},
        )
        MentionService.notify_mentioned_users(
            comment=comment,
            org=org,
            author=author_orguser,
            mentioned_users=[mentioned_orguser],
        )
        mock_send.assert_not_called()
        # In-app notification still created
        assert Notification.objects.count() == 1

    @patch(
        "ddpui.core.reports.mention_service.send_html_message",
        side_effect=Exception("SES error"),
    )
    def test_email_failure_no_raise(
        self, mock_send, comment, org, author_orguser, mentioned_orguser
    ):
        UserPreferences.objects.update_or_create(
            orguser=mentioned_orguser,
            defaults={"enable_email_notifications": True},
        )
        # Should not raise
        MentionService.notify_mentioned_users(
            comment=comment,
            org=org,
            author=author_orguser,
            mentioned_users=[mentioned_orguser],
        )
        assert Notification.objects.count() == 1


# ================================================================================
# Tests: _build_report_url
# ================================================================================


class TestBuildReportUrl:
    """Tests for MentionService._build_report_url"""

    def test_summary_url(self, snapshot, author_orguser, org):
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

    def test_chart_url(self, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="test",
            author=author_orguser,
            org=org,
        )
        url = MentionService._build_report_url(comment)
        assert "commentTarget=chart" in url
        assert "chartId=10" in url
        comment.delete()


# ================================================================================
# Tests: _resolve_chart_name
# ================================================================================


class TestResolveChartName:
    """Tests for MentionService._resolve_chart_name"""

    def test_returns_title(self, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="test",
            author=author_orguser,
            org=org,
        )
        result = MentionService._resolve_chart_name(comment)
        assert result == "Chart A"
        comment.delete()

    def test_returns_none_for_summary(self, snapshot, author_orguser, org):
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


# ================================================================================
# Tests: _get_thread_context
# ================================================================================


class TestGetThreadContext:
    """Tests for MentionService._get_thread_context"""

    def test_returns_prior_comments(self, snapshot, author_orguser, org):
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First",
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
        assert thread[0]["content"] == "First"
        current.delete()
        c1.delete()

    def test_truncates_long_content(self, snapshot, author_orguser, org):
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="X" * 300,
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
        assert len(thread[0]["content"]) == 203  # 200 + "..."
        assert thread[0]["content"].endswith("...")
        current.delete()
        c1.delete()

    def test_empty_first_comment(self, snapshot, author_orguser, org):
        current = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First ever",
            author=author_orguser,
            org=org,
        )
        thread = MentionService._get_thread_context(current)
        assert thread == []
        current.delete()
