"""Tests for CommentService — list_comments and its helpers"""

from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.models.comment import Comment, CommentReadStatus, CommentTargetType
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.comments.comment_service import CommentService
from ddpui.core.comments.exceptions import CommentValidationError
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Comment Test Org",
        slug="comment-svc-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def author_user():
    user = User.objects.create(
        username="svc_author",
        email="svc_author@test.com",
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
def other_user():
    user = User.objects.create(
        username="svc_other",
        email="svc_other@test.com",
        first_name="Other",
        last_name="Person",
    )
    yield user
    user.delete()


@pytest.fixture
def other_orguser(other_user, org, seed_db):
    orguser = OrgUser.objects.create(
        user=other_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def snapshot(org, author_orguser):
    snapshot = ReportSnapshot.objects.create(
        title="Test Report",
        date_column={},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={},
        frozen_chart_configs={"10": {"title": "Chart A"}, "20": {"title": "Chart B"}},
        created_by=author_orguser,
        org=org,
    )
    yield snapshot
    snapshot.delete()


# ================================================================================
# Tests: _fetch_comments
# ================================================================================


class TestFetchComments:
    """Tests for CommentService._fetch_comments"""

    def test_returns_summary_comments(self, snapshot, author_orguser, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary comment",
            author=author_orguser,
            org=org,
        )
        comments = CommentService._fetch_comments(snapshot, CommentTargetType.SUMMARY, None)
        assert len(comments) == 1
        assert comments[0].content == "Summary comment"

    def test_returns_chart_comments_filtered_by_chart_id(self, snapshot, author_orguser, org):
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart 10 comment",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=20,
            content="Chart 20 comment",
            author=author_orguser,
            org=org,
        )

        comments = CommentService._fetch_comments(snapshot, CommentTargetType.CHART, chart_id=10)
        assert len(comments) == 1
        assert comments[0].snapshot_chart_id == 10

    def test_chart_without_chart_id_raises(self, snapshot):
        with pytest.raises(CommentValidationError, match="chart_id is required"):
            CommentService._fetch_comments(snapshot, CommentTargetType.CHART, chart_id=None)

    def test_returns_chronological_order(self, snapshot, author_orguser, org):
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First",
            author=author_orguser,
            org=org,
        )
        c2 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Second",
            author=author_orguser,
            org=org,
        )

        comments = CommentService._fetch_comments(snapshot, CommentTargetType.SUMMARY, None)
        assert comments[0].id == c1.id
        assert comments[1].id == c2.id

    def test_does_not_return_other_target_types(self, snapshot, author_orguser, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart",
            author=author_orguser,
            org=org,
        )

        comments = CommentService._fetch_comments(snapshot, CommentTargetType.SUMMARY, None)
        assert len(comments) == 1
        assert comments[0].target_type == CommentTargetType.SUMMARY


# ================================================================================
# Tests: _attach_mentioned_users_map
# ================================================================================


class TestAttachMentionedUsersMap:
    """Tests for CommentService._attach_mentioned_users_map"""

    def test_empty_comments_list(self):
        CommentService._attach_mentioned_users_map([])
        # No exception — just a no-op

    def test_comments_without_mentions_get_empty_map(self, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="No mentions here",
            mentioned_emails=[],
            author=author_orguser,
            org=org,
        )

        CommentService._attach_mentioned_users_map([comment])
        assert comment._mentioned_users_map == {}

    def test_mentioned_emails_resolved_to_orgusers(
        self, snapshot, author_orguser, other_orguser, org
    ):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
        )

        CommentService._attach_mentioned_users_map([comment])

        assert other_orguser.user.email in comment._mentioned_users_map
        assert comment._mentioned_users_map[other_orguser.user.email].id == other_orguser.id

    def test_unknown_emails_not_in_map(self, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey @ghost@example.com",
            mentioned_emails=["ghost@example.com"],
            author=author_orguser,
            org=org,
        )

        CommentService._attach_mentioned_users_map([comment])
        assert "ghost@example.com" not in comment._mentioned_users_map

    def test_shared_map_across_comments(self, snapshot, author_orguser, other_orguser, org):
        """All comments in a batch share the same users_map instance."""
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="c1",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
        )
        c2 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="c2",
            mentioned_emails=[],
            author=author_orguser,
            org=org,
        )

        CommentService._attach_mentioned_users_map([c1, c2])
        assert c1._mentioned_users_map is c2._mentioned_users_map


# ================================================================================
# Tests: _annotate_is_new
# ================================================================================


class TestAnnotateIsNew:
    """Tests for CommentService._annotate_is_new"""

    def test_all_new_when_no_read_status(self, snapshot, author_orguser, other_orguser, org):
        """When the user has never opened the thread, all comments are new."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hello",
            author=author_orguser,
            org=org,
        )

        CommentService._annotate_is_new(
            [comment], snapshot, CommentTargetType.SUMMARY, None, other_orguser
        )
        assert comment.is_new is True

    def test_own_comments_never_new(self, snapshot, author_orguser, org):
        """A user's own comments are never marked as new."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="My own comment",
            author=author_orguser,
            org=org,
        )

        CommentService._annotate_is_new(
            [comment], snapshot, CommentTargetType.SUMMARY, None, author_orguser
        )
        assert comment.is_new is False

    def test_comments_before_read_cursor_are_not_new(
        self, snapshot, author_orguser, other_orguser, org
    ):
        """Comments created before last_read_at are not new."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Old comment",
            author=author_orguser,
            org=org,
        )

        # Mark as read AFTER the comment was created
        CommentReadStatus.objects.create(
            user=other_orguser,
            snapshot=snapshot,
            target_type=CommentTargetType.SUMMARY,
            chart_id=None,
            last_read_at=timezone.now() + timedelta(minutes=1),
        )

        CommentService._annotate_is_new(
            [comment], snapshot, CommentTargetType.SUMMARY, None, other_orguser
        )
        assert comment.is_new is False

    def test_comments_after_read_cursor_are_new(self, snapshot, author_orguser, other_orguser, org):
        """Comments created after last_read_at are new."""
        # Set read cursor in the past
        past = timezone.now() - timedelta(hours=1)
        CommentReadStatus.objects.create(
            user=other_orguser,
            snapshot=snapshot,
            target_type=CommentTargetType.SUMMARY,
            chart_id=None,
            last_read_at=past,
        )

        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="New comment",
            author=author_orguser,
            org=org,
        )

        CommentService._annotate_is_new(
            [comment], snapshot, CommentTargetType.SUMMARY, None, other_orguser
        )
        assert comment.is_new is True

    def test_no_orguser_marks_all_new(self, snapshot, author_orguser, org):
        """When orguser is None (anonymous), all comments are marked new."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Anon view",
            author=author_orguser,
            org=org,
        )

        CommentService._annotate_is_new([comment], snapshot, CommentTargetType.SUMMARY, None, None)
        assert comment.is_new is True

    def test_chart_read_status_uses_chart_id(self, snapshot, author_orguser, other_orguser, org):
        """Read status for chart comments uses the chart_id filter."""
        # Read status for chart 10 only
        CommentReadStatus.objects.create(
            user=other_orguser,
            snapshot=snapshot,
            target_type=CommentTargetType.CHART,
            chart_id=10,
            last_read_at=timezone.now() + timedelta(minutes=1),
        )

        comment_chart10 = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart 10",
            author=author_orguser,
            org=org,
        )
        comment_chart20 = Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=20,
            content="Chart 20",
            author=author_orguser,
            org=org,
        )

        # Chart 10 — has read status, should not be new
        CommentService._annotate_is_new(
            [comment_chart10], snapshot, CommentTargetType.CHART, 10, other_orguser
        )
        assert comment_chart10.is_new is False

        # Chart 20 — no read status, should be new
        CommentService._annotate_is_new(
            [comment_chart20], snapshot, CommentTargetType.CHART, 20, other_orguser
        )
        assert comment_chart20.is_new is True


# ================================================================================
# Tests: _group_comments_by_target
# ================================================================================


class TestGroupCommentsByTarget:
    """Tests for CommentService._group_comments_by_target"""

    def test_groups_summary_comments(self):
        comments = [
            (CommentTargetType.SUMMARY, None, "2026-01-01", 1),
            (CommentTargetType.SUMMARY, None, "2026-01-02", 2),
        ]
        result = CommentService._group_comments_by_target(comments)
        assert CommentTargetType.SUMMARY in result
        assert len(result[CommentTargetType.SUMMARY]) == 2

    def test_groups_chart_comments_by_chart_id(self):
        comments = [
            (CommentTargetType.CHART, 10, "2026-01-01", 1),
            (CommentTargetType.CHART, 10, "2026-01-02", 2),
            (CommentTargetType.CHART, 20, "2026-01-01", 3),
        ]
        result = CommentService._group_comments_by_target(comments)
        assert "10" in result
        assert "20" in result
        assert len(result["10"]) == 2
        assert len(result["20"]) == 1

    def test_skips_chart_without_chart_id(self):
        comments = [
            (CommentTargetType.CHART, None, "2026-01-01", 1),
        ]
        result = CommentService._group_comments_by_target(comments)
        assert result == {}

    def test_mixed_targets(self):
        comments = [
            (CommentTargetType.SUMMARY, None, "2026-01-01", 1),
            (CommentTargetType.CHART, 10, "2026-01-01", 2),
        ]
        result = CommentService._group_comments_by_target(comments)
        assert len(result) == 2
        assert CommentTargetType.SUMMARY in result
        assert "10" in result

    def test_empty_input(self):
        result = CommentService._group_comments_by_target([])
        assert result == {}


# ================================================================================
# Tests: _compute_target_states
# ================================================================================


class TestComputeTargetStates:
    """Tests for CommentService._compute_target_states"""

    @staticmethod
    def _find(result, target_type, chart_id=None):
        """Helper to find a state entry in the result list."""
        for entry in result:
            if entry["target_type"] == target_type and entry["chart_id"] == chart_id:
                return entry
        return None

    def test_all_read(self):
        """All comments before read cursor -> state is 'read'."""
        read_statuses = {(CommentTargetType.SUMMARY, None): timezone.now()}
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now() - timedelta(hours=1), 1),
            ],
        }
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids=set())
        entry = self._find(result, CommentTargetType.SUMMARY)
        assert entry["state"] == "read"
        assert entry["unread_count"] == 0

    def test_unread_no_mentions(self):
        """Unread comments but no mentions -> state is 'unread'."""
        past = timezone.now() - timedelta(hours=2)
        read_statuses = {(CommentTargetType.SUMMARY, None): past}
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now() - timedelta(hours=1), 1),
            ],
        }
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids=set())
        entry = self._find(result, CommentTargetType.SUMMARY)
        assert entry["state"] == "unread"
        assert entry["unread_count"] == 1

    def test_mentioned_takes_priority(self):
        """Unread comment with mention -> state is 'mentioned'."""
        past = timezone.now() - timedelta(hours=2)
        read_statuses = {(CommentTargetType.SUMMARY, None): past}
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now() - timedelta(hours=1), 1),
            ],
        }
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids={1})
        entry = self._find(result, CommentTargetType.SUMMARY)
        assert entry["state"] == "mentioned"

    def test_no_read_status_all_unread(self):
        """No read cursor at all -> everything is unread."""
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now(), 1),
                (timezone.now(), 2),
            ],
        }
        result = CommentService._compute_target_states(
            targets, read_statuses={}, mentioned_ids=set()
        )
        entry = self._find(result, CommentTargetType.SUMMARY)
        assert entry["state"] == "unread"
        assert entry["unread_count"] == 2
        assert entry["count"] == 2

    def test_chart_target_uses_chart_id_key(self):
        """Chart targets use (CHART, int(chart_id)) as read status key."""
        read_statuses = {
            (CommentTargetType.CHART, 10): timezone.now(),
        }
        targets = {
            "10": [
                (timezone.now() - timedelta(hours=1), 1),
            ],
        }
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids=set())
        entry = self._find(result, CommentTargetType.CHART, chart_id=10)
        assert entry["state"] == "read"

    def test_mention_only_counts_when_unread(self):
        """A mentioned comment that's already read doesn't trigger 'mentioned'."""
        read_statuses = {(CommentTargetType.SUMMARY, None): timezone.now()}
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now() - timedelta(hours=1), 1),
            ],
        }
        # Comment 1 mentions the user but was created before read cursor
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids={1})
        entry = self._find(result, CommentTargetType.SUMMARY)
        assert entry["state"] == "read"

    def test_multiple_targets_independent(self):
        """Each target is computed independently."""
        past = timezone.now() - timedelta(hours=2)
        read_statuses = {
            (CommentTargetType.SUMMARY, None): timezone.now(),
            (CommentTargetType.CHART, 10): past,
        }
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now() - timedelta(hours=1), 1),
            ],
            "10": [
                (timezone.now() - timedelta(hours=1), 2),
            ],
        }
        result = CommentService._compute_target_states(targets, read_statuses, mentioned_ids=set())
        summary_entry = self._find(result, CommentTargetType.SUMMARY)
        chart_entry = self._find(result, CommentTargetType.CHART, chart_id=10)
        assert summary_entry["state"] == "read"
        assert chart_entry["state"] == "unread"

    def test_returns_list(self):
        """Result is a list, not a dict."""
        targets = {
            CommentTargetType.SUMMARY: [
                (timezone.now(), 1),
            ],
        }
        result = CommentService._compute_target_states(
            targets, read_statuses={}, mentioned_ids=set()
        )
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["target_type"] == CommentTargetType.SUMMARY
        assert result[0]["chart_id"] is None


# ================================================================================
# Tests: _get_read_statuses and _get_mentioned_comment_ids
# ================================================================================


class TestReadStatusesAndMentionedIds:
    """Tests for the DB-fetching helpers"""

    def test_get_read_statuses_returns_dict(self, snapshot, other_orguser):
        CommentReadStatus.objects.create(
            user=other_orguser,
            snapshot=snapshot,
            target_type=CommentTargetType.SUMMARY,
            chart_id=None,
            last_read_at=timezone.now(),
        )
        result = CommentService._get_read_statuses(other_orguser, snapshot)
        assert (CommentTargetType.SUMMARY, None) in result

    def test_get_read_statuses_empty(self, snapshot, other_orguser):
        result = CommentService._get_read_statuses(other_orguser, snapshot)
        assert result == {}

    def test_get_mentioned_comment_ids(self, snapshot, author_orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
        )
        result = CommentService._get_mentioned_comment_ids(snapshot, other_orguser.user.email)
        assert comment.id in result

    def test_get_mentioned_comment_ids_excludes_deleted(
        self, snapshot, author_orguser, other_orguser, org
    ):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
            is_deleted=True,
        )
        result = CommentService._get_mentioned_comment_ids(snapshot, other_orguser.user.email)
        assert len(result) == 0

    def test_get_mentioned_comment_ids_empty(self, snapshot, other_orguser):
        result = CommentService._get_mentioned_comment_ids(snapshot, other_orguser.user.email)
        assert result == set()


# ================================================================================
# Tests: list_comments (integration)
# ================================================================================


class TestGetCommentStates:
    """Integration tests for CommentService.get_comment_states — array response format"""

    def test_returns_list(self, snapshot, author_orguser, other_orguser, org):
        """get_comment_states returns a list, not a dict."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="A summary comment",
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        assert isinstance(result, list)

    def test_empty_snapshot_returns_empty_list(self, snapshot, other_orguser, org):
        """Snapshot with no comments returns []."""
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        assert result == []

    def test_summary_entry_has_correct_fields(self, snapshot, author_orguser, other_orguser, org):
        """Summary entry has target_type='summary' and chart_id=None."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary comment",
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next((e for e in result if e["target_type"] == CommentTargetType.SUMMARY), None)
        assert summary is not None
        assert summary["chart_id"] is None
        assert summary["count"] == 1
        assert summary["state"] in ("unread", "read", "mentioned")

    def test_chart_entry_has_correct_fields(self, snapshot, author_orguser, other_orguser, org):
        """Chart entry has target_type='chart' and an integer chart_id."""
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart comment",
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        chart_entry = next((e for e in result if e["target_type"] == CommentTargetType.CHART), None)
        assert chart_entry is not None
        assert chart_entry["chart_id"] == 10
        assert chart_entry["count"] == 1

    def test_mixed_targets_returns_multiple_entries(
        self, snapshot, author_orguser, other_orguser, org
    ):
        """Summary + two charts produce 3 entries in the list."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart 10",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=20,
            content="Chart 20",
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        assert len(result) == 3
        target_types = {(e["target_type"], e["chart_id"]) for e in result}
        assert (CommentTargetType.SUMMARY, None) in target_types
        assert (CommentTargetType.CHART, 10) in target_types
        assert (CommentTargetType.CHART, 20) in target_types

    def test_unread_state_for_never_read(self, snapshot, author_orguser, other_orguser, org):
        """User who never opened a thread sees 'unread' state."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="First comment",
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        assert summary["state"] == "unread"
        assert summary["unread_count"] == 1

    def test_read_state_after_mark_as_read(self, snapshot, author_orguser, other_orguser, org):
        """After marking as read, state becomes 'read'."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="A comment",
            author=author_orguser,
            org=org,
        )
        CommentService.mark_as_read(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
            target_type=CommentTargetType.SUMMARY,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        assert summary["state"] == "read"
        assert summary["unread_count"] == 0

    def test_mentioned_state(self, snapshot, author_orguser, other_orguser, org):
        """User mentioned in an unread comment sees 'mentioned' state."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        assert summary["state"] == "mentioned"

    def test_edited_comment_stays_read(self, snapshot, author_orguser, other_orguser, org):
        """Editing a comment (bumping updated_at) should NOT flip it back to unread.

        We use created_at for the unread check, not updated_at.
        """
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Original text",
            author=author_orguser,
            org=org,
        )
        # Mark as read
        CommentService.mark_as_read(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
            target_type=CommentTargetType.SUMMARY,
        )
        # Edit the comment — this bumps updated_at but NOT created_at
        comment.content = "Edited text"
        comment.save(update_fields=["content", "updated_at"])

        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        assert summary["state"] == "read", "Editing a comment should not make it unread again"
        assert summary["unread_count"] == 0

    def test_deleted_comments_excluded(self, snapshot, author_orguser, other_orguser, org):
        """Soft-deleted comments should not appear in counts."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Visible comment",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Deleted comment",
            author=author_orguser,
            org=org,
            is_deleted=True,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        assert summary["count"] == 1, "Deleted comments should not be counted"

    def test_read_status_does_not_cross_targets(self, snapshot, author_orguser, other_orguser, org):
        """Reading summary should NOT affect chart unread state, and vice versa."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary comment",
            author=author_orguser,
            org=org,
        )
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart comment",
            author=author_orguser,
            org=org,
        )
        # Only mark summary as read
        CommentService.mark_as_read(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
            target_type=CommentTargetType.SUMMARY,
        )
        result = CommentService.get_comment_states(
            snapshot_id=snapshot.id,
            org=org,
            orguser=other_orguser,
        )
        summary = next(e for e in result if e["target_type"] == CommentTargetType.SUMMARY)
        chart = next(e for e in result if e["target_type"] == CommentTargetType.CHART)
        assert summary["state"] == "read"
        assert chart["state"] == "unread", "Reading summary should not mark chart as read"


class TestListComments:
    """Integration tests for CommentService.list_comments"""

    @patch("ddpui.core.comments.mention_service.send_html_message")
    def test_returns_comments_with_is_new_and_mentions_map(
        self, mock_send, snapshot, author_orguser, other_orguser, org
    ):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
            author=author_orguser,
            org=org,
        )

        comments = CommentService.list_comments(
            snapshot_id=snapshot.id,
            org=org,
            target_type=CommentTargetType.SUMMARY,
            orguser=other_orguser,
        )

        assert len(comments) == 1
        assert hasattr(comments[0], "is_new")
        assert hasattr(comments[0], "_mentioned_users_map")
        assert comments[0].is_new is True
        assert other_orguser.user.email in comments[0]._mentioned_users_map

    def test_invalid_snapshot_raises(self, org, other_orguser):
        with pytest.raises(CommentValidationError, match="not found"):
            CommentService.list_comments(
                snapshot_id=99999,
                org=org,
                target_type=CommentTargetType.SUMMARY,
                orguser=other_orguser,
            )

    def test_wrong_org_raises(self, snapshot, other_orguser):
        other_org = Org.objects.create(
            name="Other Org", slug="other-org", airbyte_workspace_id="other-ws"
        )
        with pytest.raises(CommentValidationError, match="not found"):
            CommentService.list_comments(
                snapshot_id=snapshot.id,
                org=other_org,
                target_type=CommentTargetType.SUMMARY,
                orguser=other_orguser,
            )
        other_org.delete()
