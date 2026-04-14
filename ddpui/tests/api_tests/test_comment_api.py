"""API Tests for Comment endpoints on Report Snapshots

Tests:
1. get_mentionable_users - returns_org_users, excludes_other_org, empty_when_solo
2. get_comment_states - empty_for_no_comments, unread_state, read_state, mentioned_state,
                        snapshot_not_found
3. mark_as_read - mark_summary, mark_chart, updates_existing
4. list_comments - list_summary, list_chart, empty_list, includes_is_new,
                   chart_requires_chart_id
5. create_comment - create_summary, create_chart, invalid_target_type, chart_without_chart_id,
                    chart_not_in_snapshot, with_mentions
6. update_comment - update_own, update_other_forbidden, comment_not_found
7. delete_comment - delete_own (soft delete), delete_other_forbidden, comment_not_found
"""

import os
import django
from datetime import date
from unittest.mock import patch

import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.report import ReportSnapshot
from ddpui.models.comment import Comment, CommentTargetType, CommentReadStatus
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.report_api import (
    get_mentionable_users,
    get_comment_states,
    mark_as_read,
    list_comments,
    create_comment,
    update_comment,
    delete_comment,
)
from ddpui.schemas.report_schema import CommentCreate, CommentUpdate, MarkReadRequest
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="cmtapiuser", email="cmtapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Comment API Test Org",
        slug="cmt-api-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org, seed_db):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def other_authuser():
    user = User.objects.create(
        username="cmtotherapiuser", email="cmtotherapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def other_orguser(other_authuser, org, seed_db):
    orguser = OrgUser.objects.create(
        user=other_authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def snapshot(org, orguser):
    snapshot = ReportSnapshot.objects.create(
        title="Comment API Report",
        date_column={},
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        frozen_dashboard={},
        frozen_chart_configs={"10": {"title": "Chart A"}, "20": {"title": "Chart B"}},
        created_by=orguser,
        org=org,
    )
    yield snapshot
    snapshot.delete()


# ================================================================================
# Test get_mentionable_users
# ================================================================================


class TestGetMentionableUsers:
    """Tests for get_mentionable_users endpoint"""

    def test_returns_org_users(self, orguser, other_orguser):
        request = mock_request(orguser)
        response = get_mentionable_users(request)
        assert response["success"] is True
        emails = [u.email for u in response["data"]]
        assert orguser.user.email in emails
        assert other_orguser.user.email in emails

    def test_excludes_other_org(self, orguser, org, seed_db):
        other_org = Org.objects.create(
            name="Other Org", slug="other-org-ment", airbyte_workspace_id="other-ws"
        )
        other_user = User.objects.create(
            username="othorguser", email="othorguser@test.com", password="pwd"
        )
        other_ou = OrgUser.objects.create(
            user=other_user,
            org=other_org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )

        request = mock_request(orguser)
        response = get_mentionable_users(request)
        emails = [u.email for u in response["data"]]
        assert other_user.email not in emails

        other_ou.delete()
        other_user.delete()
        other_org.delete()

    def test_empty_when_solo(self, seed_db):
        solo_org = Org.objects.create(
            name="Solo Org", slug="solo-org-m", airbyte_workspace_id="solo-ws"
        )
        solo_user = User.objects.create(
            username="solomuser", email="solomuser@test.com", password="pwd"
        )
        solo_ou = OrgUser.objects.create(
            user=solo_user,
            org=solo_org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )

        request = mock_request(solo_ou)
        response = get_mentionable_users(request)
        assert response["success"] is True
        assert len(response["data"]) == 1  # only the solo user

        solo_ou.delete()
        solo_user.delete()
        solo_org.delete()


# ================================================================================
# Test get_comment_states
# ================================================================================


class TestGetCommentStates:
    """Tests for get_comment_states endpoint"""

    def test_empty_for_no_comments(self, orguser, snapshot):
        request = mock_request(orguser)
        response = get_comment_states(request, snapshot.id)
        assert response["success"] is True
        assert response["data"]["states"] == []

    def test_unread_state(self, orguser, other_orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="A comment",
            author=other_orguser,
            org=org,
        )
        request = mock_request(orguser)
        response = get_comment_states(request, snapshot.id)
        states = response["data"]["states"]
        summary = next(s for s in states if s["target_type"] == "summary")
        assert summary["state"] == "unread"

    def test_read_state(self, orguser, other_orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Read me",
            author=other_orguser,
            org=org,
        )
        from ddpui.core.reports.comment_service import CommentService

        CommentService.mark_as_read(
            snapshot_id=snapshot.id,
            orguser=orguser,
            target_type=CommentTargetType.SUMMARY,
        )
        request = mock_request(orguser)
        response = get_comment_states(request, snapshot.id)
        states = response["data"]["states"]
        summary = next(s for s in states if s["target_type"] == "summary")
        assert summary["state"] == "read"

    def test_mentioned_state(self, orguser, other_orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content=f"Hey @{orguser.user.email}",
            mentioned_emails=[orguser.user.email],
            author=other_orguser,
            org=org,
        )
        request = mock_request(orguser)
        response = get_comment_states(request, snapshot.id)
        states = response["data"]["states"]
        summary = next(s for s in states if s["target_type"] == "summary")
        assert summary["state"] == "mentioned"

    def test_snapshot_not_found(self, orguser):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_comment_states(request, 99999)
        assert exc.value.status_code == 400


# ================================================================================
# Test mark_as_read
# ================================================================================


class TestMarkAsRead:
    """Tests for mark_as_read endpoint"""

    def test_mark_summary(self, orguser, snapshot):
        request = mock_request(orguser)
        payload = MarkReadRequest(target_type="summary")
        response = mark_as_read(request, snapshot.id, payload)
        assert response["success"] is True
        assert CommentReadStatus.objects.filter(
            user=orguser, snapshot=snapshot, target_type="summary"
        ).exists()

    def test_mark_chart(self, orguser, snapshot):
        request = mock_request(orguser)
        payload = MarkReadRequest(target_type="chart", chart_id=10)
        response = mark_as_read(request, snapshot.id, payload)
        assert response["success"] is True
        assert CommentReadStatus.objects.filter(
            user=orguser, snapshot=snapshot, target_type="chart", chart_id=10
        ).exists()

    def test_updates_existing(self, orguser, snapshot):
        request = mock_request(orguser)
        payload = MarkReadRequest(target_type="summary")
        mark_as_read(request, snapshot.id, payload)
        first_read = CommentReadStatus.objects.get(
            user=orguser, snapshot=snapshot, target_type="summary"
        )
        first_time = first_read.last_read_at

        # Mark again — should update, not duplicate
        mark_as_read(request, snapshot.id, payload)
        assert (
            CommentReadStatus.objects.filter(
                user=orguser, snapshot=snapshot, target_type="summary"
            ).count()
            == 1
        )
        first_read.refresh_from_db()
        assert first_read.last_read_at >= first_time


# ================================================================================
# Test list_comments
# ================================================================================


class TestListComments:
    """Tests for list_comments endpoint"""

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_list_summary(self, mock_send, orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Summary note",
            author=orguser,
            org=org,
        )
        request = mock_request(orguser)
        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["success"] is True
        assert len(response["data"]) == 1
        assert response["data"][0].content == "Summary note"

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_list_chart(self, mock_send, orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.CHART,
            snapshot=snapshot,
            snapshot_chart_id=10,
            content="Chart note",
            author=orguser,
            org=org,
        )
        request = mock_request(orguser)
        response = list_comments(request, snapshot.id, target_type="chart", chart_id=10)
        assert response["success"] is True
        assert len(response["data"]) == 1
        assert response["data"][0].chart_id == 10

    def test_empty_list(self, orguser, snapshot):
        request = mock_request(orguser)
        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["success"] is True
        assert len(response["data"]) == 0

    def test_includes_is_new(self, orguser, other_orguser, snapshot, org):
        Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="New for you",
            author=other_orguser,
            org=org,
        )
        request = mock_request(orguser)
        response = list_comments(request, snapshot.id, target_type="summary")
        assert response["data"][0].is_new is True

    def test_chart_requires_chart_id(self, orguser, snapshot):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_comments(request, snapshot.id, target_type="chart")
        assert exc.value.status_code == 400


# ================================================================================
# Test create_comment
# ================================================================================


class TestCreateComment:
    """Tests for create_comment endpoint"""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_create_summary(self, mock_mentions, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(target_type="summary", content="My summary comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        assert response["data"]["target_type"] == "summary"
        assert response["data"]["content"] == "My summary comment"
        assert response["data"]["author_email"] == orguser.user.email
        Comment.objects.filter(id=response["data"]["id"]).delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_create_chart(self, mock_mentions, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(target_type="chart", chart_id=10, content="Chart comment")
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        assert response["data"]["chart_id"] == 10
        Comment.objects.filter(id=response["data"]["id"]).delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_invalid_target_type(self, mock_mentions, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(target_type="invalid", content="Bad type")
        with pytest.raises(HttpError) as exc:
            create_comment(request, snapshot.id, payload)
        assert exc.value.status_code == 400

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_chart_without_chart_id(self, mock_mentions, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(target_type="chart", content="No chart id")
        with pytest.raises(HttpError) as exc:
            create_comment(request, snapshot.id, payload)
        assert exc.value.status_code == 400

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_chart_not_in_snapshot(self, mock_mentions, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(target_type="chart", chart_id=999, content="Ghost chart")
        with pytest.raises(HttpError) as exc:
            create_comment(request, snapshot.id, payload)
        assert exc.value.status_code == 400

    @patch("ddpui.core.reports.mention_service.send_html_message")
    def test_with_mentions(self, mock_send, orguser, other_orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentCreate(
            target_type="summary",
            content=f"Hey @{other_orguser.user.email}",
            mentioned_emails=[other_orguser.user.email],
        )
        response = create_comment(request, snapshot.id, payload)
        assert response["success"] is True
        comment = Comment.objects.get(id=response["data"]["id"])
        assert other_orguser.user.email in comment.mentioned_emails
        comment.delete()


# ================================================================================
# Test update_comment
# ================================================================================


class TestUpdateComment:
    """Tests for update_comment endpoint"""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_update_own(self, mock_mentions, orguser, snapshot, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Original",
            author=orguser,
            org=org,
        )
        request = mock_request(orguser)
        payload = CommentUpdate(content="Updated text")
        response = update_comment(request, snapshot.id, comment.id, payload)
        assert response["success"] is True
        assert response["data"]["content"] == "Updated text"
        comment.delete()

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_update_other_forbidden(self, mock_mentions, orguser, other_orguser, snapshot, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours",
            author=orguser,
            org=org,
        )
        request = mock_request(other_orguser)
        payload = CommentUpdate(content="Trying to edit")
        with pytest.raises(HttpError) as exc:
            update_comment(request, snapshot.id, comment.id, payload)
        assert exc.value.status_code == 403
        comment.delete()

    def test_comment_not_found(self, orguser, snapshot):
        request = mock_request(orguser)
        payload = CommentUpdate(content="Ghost")
        with pytest.raises(HttpError) as exc:
            update_comment(request, snapshot.id, 99999, payload)
        assert exc.value.status_code == 404


# ================================================================================
# Test delete_comment
# ================================================================================


class TestDeleteComment:
    """Tests for delete_comment endpoint"""

    @patch("ddpui.core.reports.mention_service.MentionService.process_mentions")
    def test_delete_own(self, mock_mentions, orguser, snapshot, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Delete me",
            author=orguser,
            org=org,
        )
        comment_id = comment.id
        request = mock_request(orguser)
        response = delete_comment(request, snapshot.id, comment_id)
        assert response["success"] is True
        # sole author in thread => hard-delete
        assert not Comment.objects.filter(id=comment_id).exists()

    def test_delete_other_forbidden(self, orguser, other_orguser, snapshot, org):
        comment = Comment.objects.create(
            target_type=CommentTargetType.SUMMARY,
            snapshot=snapshot,
            content="Not yours to delete",
            author=orguser,
            org=org,
        )
        request = mock_request(other_orguser)
        with pytest.raises(HttpError) as exc:
            delete_comment(request, snapshot.id, comment.id)
        assert exc.value.status_code == 403
        comment.delete()

    def test_comment_not_found(self, orguser, snapshot):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            delete_comment(request, snapshot.id, 99999)
        assert exc.value.status_code == 404
