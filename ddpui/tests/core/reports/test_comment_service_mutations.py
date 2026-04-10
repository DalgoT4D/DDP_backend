"""Tests for CommentService mutations — update_comment, delete_comment, get_mentionable_users"""

import os
import django
from unittest.mock import patch

import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.comment import Comment, CommentTargetType
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.reports.comment_service import CommentService
from ddpui.core.reports.exceptions import (
    CommentNotFoundError,
    CommentPermissionError,
    CommentValidationError,
)

pytestmark = pytest.mark.django_db


# ================================================================================
# Tests: update_comment
# ================================================================================


class TestUpdateComment:
    """Tests for CommentService.update_comment"""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_success(self, mock_mentions, snapshot, orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Original",
            author=orguser,
            org=org,
        )
        updated = CommentService.update_comment(
            comment_id=comment.id,
            org=org,
            orguser=orguser,
            content="Updated",
        )
        assert updated.content == "Updated"
        comment.delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_clears_old_mentions(self, mock_mentions, snapshot, orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Hey @someone",
            mentioned_emails=["old@test.com"],
            author=orguser,
            org=org,
        )
        CommentService.update_comment(
            comment_id=comment.id,
            org=org,
            orguser=orguser,
            content="No mentions now",
            mentioned_emails=[],
        )
        comment.refresh_from_db()
        # mentioned_emails is cleared before process_mentions re-populates
        assert comment.mentioned_emails == []
        comment.delete()

    def test_non_author_raises(self, snapshot, orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours",
            author=orguser,
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

    def test_deleted_comment_raises(self, snapshot, orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="",
            is_deleted=True,
            author=orguser,
            org=org,
        )
        with pytest.raises(CommentValidationError, match="deleted comment"):
            CommentService.update_comment(
                comment_id=comment.id,
                org=org,
                orguser=orguser,
                content="Revive?",
            )
        comment.delete()

    def test_not_found_raises(self, org, orguser):
        with pytest.raises(CommentNotFoundError):
            CommentService.update_comment(
                comment_id=99999,
                org=org,
                orguser=orguser,
                content="Ghost",
            )


# ================================================================================
# Tests: delete_comment
# ================================================================================


class TestDeleteComment:
    """Tests for CommentService.delete_comment"""

    def test_success(self, snapshot, orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Delete me",
            mentioned_emails=["someone@test.com"],
            author=orguser,
            org=org,
        )
        CommentService.delete_comment(
            comment_id=comment.id,
            org=org,
            orguser=orguser,
        )
        comment.refresh_from_db()
        assert comment.is_deleted is True
        assert comment.content == ""
        assert comment.mentioned_emails == []
        comment.delete()

    def test_non_author_raises(self, snapshot, orguser, other_orguser, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours",
            author=orguser,
            org=org,
        )
        with pytest.raises(CommentPermissionError, match="only delete your own"):
            CommentService.delete_comment(
                comment_id=comment.id,
                org=org,
                orguser=other_orguser,
            )
        comment.delete()

    def test_not_found_raises(self, org, orguser):
        with pytest.raises(CommentNotFoundError):
            CommentService.delete_comment(
                comment_id=99999,
                org=org,
                orguser=orguser,
            )


# ================================================================================
# Tests: get_mentionable_users
# ================================================================================


class TestGetMentionableUsers:
    """Tests for CommentService.get_mentionable_users"""

    def test_returns_ordered_by_email(self, org, orguser, other_orguser):
        users = CommentService.get_mentionable_users(org)
        emails = [u.user.email for u in users]
        assert emails == sorted(emails)
        assert orguser.user.email in emails
        assert other_orguser.user.email in emails

    def test_excludes_other_org(self, org, orguser):
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
