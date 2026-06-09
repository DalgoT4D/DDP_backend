"""Tests for CommentService mutations — update_comment, delete_comment, get_mentionable_users"""

import os
import django
from datetime import date
from unittest.mock import patch

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
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.reports.comment_service import CommentService
from ddpui.core.reports.exceptions import (
    CommentNotFoundError,
    CommentPermissionError,
    CommentValidationError,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Mutation Test Org",
        slug="mut-svc-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def author_user():
    user = User.objects.create(
        username="mut_author",
        email="mut_author@test.com",
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
        username="mut_other",
        email="mut_other@test.com",
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
        title="Mutation Test Report",
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
# Tests: update_comment
# ================================================================================


class TestUpdateComment:
    """Tests for CommentService.update_comment"""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_success(self, mock_mentions, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Original",
            author=author_orguser,
            org=org,
        )
        updated = CommentService.update_comment(
            comment_id=comment.id,
            org=org,
            orguser=author_orguser,
            content="Updated",
        )
        assert updated.content == "Updated"
        comment.delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_clears_old_mentions(self, mock_mentions, snapshot, author_orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey @someone",
            mentioned_emails=["old@test.com"],
            author=author_orguser,
            org=org,
        )
        CommentService.update_comment(
            comment_id=comment.id,
            org=org,
            orguser=author_orguser,
            content="No mentions now",
            mentioned_emails=[],
        )
        comment.refresh_from_db()
        # mentioned_emails is cleared before process_mentions re-populates
        assert comment.mentioned_emails == []
        comment.delete()

    def test_non_author_raises(self, snapshot, author_orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours",
            author=author_orguser,
            org=org,
        )
        with pytest.raises(CommentPermissionError, match="only edit your own"):
            CommentService.update_comment(
                comment_id=comment.id,
                org=org,
                orguser=other_orguser,
                content="Trying to edit",
            )
        comment.delete()

    def test_deleted_comment_raises(self, snapshot, author_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="",
            is_deleted=True,
            author=author_orguser,
            org=org,
        )
        with pytest.raises(CommentValidationError, match="deleted comment"):
            CommentService.update_comment(
                comment_id=comment.id,
                org=org,
                orguser=author_orguser,
                content="Revive?",
            )
        comment.delete()

    def test_not_found_raises(self, org, author_orguser):
        with pytest.raises(CommentNotFoundError):
            CommentService.update_comment(
                comment_id=99999,
                org=org,
                orguser=author_orguser,
                content="Ghost",
            )


# ================================================================================
# Tests: delete_comment
# ================================================================================


class TestDeleteComment:
    """Tests for CommentService.delete_comment"""

    def test_hard_deletes_sole_comment(self, snapshot, author_orguser, org):
        """Only comment in thread, only author — hard-delete."""
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Delete me",
            mentioned_emails=["someone@test.com"],
            author=author_orguser,
            org=org,
        )
        comment_id = comment.id
        CommentService.delete_comment(
            comment_id=comment_id,
            org=org,
            orguser=author_orguser,
        )
        assert not Comment.objects.filter(id=comment_id).exists()

    def test_hard_deletes_multiple_own_comments(self, snapshot, author_orguser, org):
        """Multiple comments in thread but ALL by the same author — hard-delete."""
        c1 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="My first",
            author=author_orguser,
            org=org,
        )
        c2 = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="My second",
            author=author_orguser,
            org=org,
        )
        c2_id = c2.id
        CommentService.delete_comment(
            comment_id=c2_id,
            org=org,
            orguser=author_orguser,
        )
        assert not Comment.objects.filter(id=c2_id).exists()
        c1.delete()

    def test_soft_deletes_when_other_author_exists(
        self, snapshot, author_orguser, other_orguser, org
    ):
        """Another user has commented in the thread — soft-delete."""
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Other person's comment",
            author=other_orguser,
            org=org,
        )
        my_comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Delete me",
            mentioned_emails=["someone@test.com"],
            author=author_orguser,
            org=org,
        )
        CommentService.delete_comment(
            comment_id=my_comment.id,
            org=org,
            orguser=author_orguser,
        )
        my_comment.refresh_from_db()
        assert my_comment.is_deleted is True
        assert my_comment.content == ""
        assert my_comment.mentioned_emails == []

    def test_non_author_raises(self, snapshot, author_orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours",
            author=author_orguser,
            org=org,
        )
        with pytest.raises(CommentPermissionError, match="only delete your own"):
            CommentService.delete_comment(
                comment_id=comment.id,
                org=org,
                orguser=other_orguser,
            )
        comment.delete()

    def test_not_found_raises(self, org, author_orguser):
        with pytest.raises(CommentNotFoundError):
            CommentService.delete_comment(
                comment_id=99999,
                org=org,
                orguser=author_orguser,
            )


# ================================================================================
# Tests: get_mentionable_users
# ================================================================================


class TestGetMentionableUsers:
    """Tests for CommentService.get_mentionable_users"""

    def test_returns_ordered_by_email(self, org, author_orguser, other_orguser):
        users = CommentService.get_mentionable_users(org)
        emails = [u.user.email for u in users]
        assert emails == sorted(emails)
        assert author_orguser.user.email in emails
        assert other_orguser.user.email in emails

    def test_excludes_other_org(self, org, author_orguser):
        other_org = Org.objects.create(
            name="Other Org Ment", slug="oth-org-ment", airbyte_workspace_id="other-ws"
        )
        other_user = User.objects.create(
            username="mentexcl", email="mentexcl@test.com", password="pwd"
        )
        other_ou = OrgUser.objects.create(
            user=other_user,
            org=other_org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )

        users = CommentService.get_mentionable_users(org)
        emails = [u.user.email for u in users]
        assert other_user.email not in emails

        other_ou.delete()
        other_user.delete()
        other_org.delete()

    def test_empty_org(self, seed_db):
        empty_org = Org.objects.create(
            name="Empty Org", slug="empt-org-ment", airbyte_workspace_id="empty-ws"
        )
        users = CommentService.get_mentionable_users(empty_org)
        assert users == []
        empty_org.delete()
