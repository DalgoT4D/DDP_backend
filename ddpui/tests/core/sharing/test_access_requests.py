"""Task 15 (Milestone 9): request-access backend -- request -> owner
approves -> grant + notification.

Endpoints:
    POST /api/access/{rtype}/{resource_id}/requests/  -- ask (any org member)
    GET  /api/access/requests/                        -- inbox (incoming/outgoing)
    POST /api/access/requests/{id}/approve/            -- owner/admin decides
    POST /api/access/requests/{id}/decline/            -- owner/admin decides

Route functions are called directly via `mock_request(orguser)`, same
convention as `test_access_api.py` / `test_owner_transfer.py` -- this
exercises the real permission machinery (`request.permissions` built from
seeded RolePermission rows).
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from datetime import timedelta

import pytest
from django.test import Client
from django.utils import timezone as django_timezone
from ninja.errors import HttpError

from ddpui.auth import ANALYST_ROLE
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.models.access_request import AccessRequest
from ddpui.models.general_access import GeneralAudience
from ddpui.models.metric import Metric
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.org import Org
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import RolePermission
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.tests.core.sharing.test_access_api import (
    _dashboard,
    _grant,
    _make_orguser,
    admin,
    analyst,
    analyst2,
    member,
    org,
)

pytestmark = pytest.mark.django_db


# ================================================================================
# Helpers
# ================================================================================


def _create_request(caller, rtype, resource, permission="view", note=None):
    from ddpui.api.access_api import create_access_request
    from ddpui.schemas.access_schema import AccessRequestCreate

    payload = AccessRequestCreate(requested_permission=permission, note=note)
    return create_access_request(mock_request(caller), rtype, str(resource.pk), payload)


def _list_requests(caller):
    from ddpui.api.access_api import list_access_requests

    return list_access_requests(mock_request(caller))


def _approve(caller, request_id, permission=None):
    from ddpui.api.access_api import approve_access_request
    from ddpui.schemas.access_schema import AccessRequestDecision

    payload = AccessRequestDecision(permission=permission)
    return approve_access_request(mock_request(caller), request_id, payload)


def _decline(caller, request_id):
    from ddpui.api.access_api import decline_access_request

    return decline_access_request(mock_request(caller), request_id)


# ================================================================================
# POST /api/access/{rtype}/{resource_id}/requests/ -- create
# ================================================================================


class TestCreateAccessRequest:
    def test_member_without_access_creates_pending_request_and_notifies_owner(
        self, org, analyst, member
    ):
        dashboard = _dashboard(org, analyst)

        response = _create_request(member, "dashboard", dashboard, permission="view", note="pls")

        assert response["success"] is True
        data = response["data"]
        assert data["status"] == "pending"
        assert data["requested_permission"] == "view"
        assert data["note"] == "pls"
        assert data["requester"]["orguser_id"] == member.id

        access_request = AccessRequest.objects.get(id=data["id"])
        assert access_request.org_id == org.id
        assert access_request.resource_type == "dashboard"
        assert access_request.resource_id == str(dashboard.pk)
        assert access_request.expires_at > django_timezone.now() + timedelta(days=29)

        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient_id == analyst.id

    def test_new_request_notification_carries_actionable_metadata(self, org, analyst, member):
        """The owner's "new request" notification carries a structured
        payload (batch 2 / F6) so the Notifications page can render inline
        Approve/Deny instead of forcing a trip through the share modal."""
        dashboard = _dashboard(org, analyst)

        created = _create_request(member, "dashboard", dashboard, permission="edit", note="pls")

        notification = Notification.objects.get()
        assert notification.metadata == {
            "kind": "access_request",
            "request_id": created["data"]["id"],
            "resource_type": "dashboard",
            "resource_name": dashboard.title,
            "requester_email": member.user.email,
            "requested_permission": "edit",
        }

    def test_requester_with_existing_access_400(self, org, analyst, analyst2):
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="view")
        assert effective_permission(analyst2, "dashboard", dashboard) == "view"

        with pytest.raises(HttpError) as excinfo:
            _create_request(analyst2, "dashboard", dashboard)
        assert excinfo.value.status_code == 400
        assert AccessRequest.objects.count() == 0

    def test_duplicate_pending_request_refreshes_instead_of_stacking(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)

        first = _create_request(member, "dashboard", dashboard, permission="view", note="first ask")
        second = _create_request(
            member, "dashboard", dashboard, permission="edit", note="second ask"
        )

        assert first["data"]["id"] == second["data"]["id"]
        assert (
            AccessRequest.objects.filter(
                org=org, resource_type="dashboard", resource_id=str(dashboard.pk), requester=member
            ).count()
            == 1
        )
        access_request = AccessRequest.objects.get(id=second["data"]["id"])
        assert access_request.requested_permission == "edit"
        assert access_request.note == "second ask"
        # only the first ask notifies -- no re-notify on refresh
        assert Notification.objects.count() == 1

    def test_anonymous_401(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        client = Client()
        response = client.post(
            f"/api/access/dashboard/{dashboard.pk}/requests/",
            data={"requested_permission": "view"},
            content_type="application/json",
        )
        assert response.status_code == 401

    def test_reask_on_expired_but_unswept_pending_row_refreshes_not_stacks(
        self, org, analyst, member
    ):
        """A `pending` row can be past its `expires_at` for up to 24h before
        the daily cleanup task sweeps it to `expired` -- a re-ask in that
        window must still refresh the same row, not create a second pending
        row that would outlive the sweep."""
        dashboard = _dashboard(org, analyst)
        first = _create_request(member, "dashboard", dashboard, permission="view")
        stale = AccessRequest.objects.get(id=first["data"]["id"])
        stale.expires_at = django_timezone.now() - timedelta(hours=1)
        stale.save(update_fields=["expires_at"])

        second = _create_request(member, "dashboard", dashboard, permission="edit")

        assert second["data"]["id"] == first["data"]["id"]
        assert (
            AccessRequest.objects.filter(
                org=org, resource_type="dashboard", resource_id=str(dashboard.pk), requester=member
            ).count()
            == 1
        )
        refreshed = AccessRequest.objects.get(id=first["data"]["id"])
        assert refreshed.status == "pending"
        assert refreshed.requested_permission == "edit"
        assert refreshed.expires_at > django_timezone.now() + timedelta(days=29)


# ================================================================================
# GET /api/access/requests/ -- inbox
# ================================================================================


class TestListAccessRequests:
    def test_owner_sees_incoming_requester_sees_outgoing(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        _create_request(member, "dashboard", dashboard, permission="view")

        owner_view = _list_requests(analyst)["data"]
        assert len(owner_view["incoming"]) == 1
        assert owner_view["incoming"][0]["requester"]["orguser_id"] == member.id
        assert owner_view["outgoing"] == []

        requester_view = _list_requests(member)["data"]
        assert requester_view["incoming"] == []
        assert len(requester_view["outgoing"]) == 1
        assert requester_view["outgoing"][0]["resource_type"] == "dashboard"

    def test_admin_sees_every_pending_request_as_incoming(self, org, admin, analyst, member):
        dashboard = _dashboard(org, analyst)
        _create_request(member, "dashboard", dashboard, permission="view")

        admin_view = _list_requests(admin)["data"]
        assert len(admin_view["incoming"]) == 1

    def test_anonymous_401(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        _create_request(member, "dashboard", dashboard, permission="view")

        client = Client()
        response = client.get("/api/access/requests/")
        assert response.status_code == 401

    def test_editor_via_grant_does_not_see_request_as_incoming(
        self, org, analyst, analyst2, member
    ):
        """An Analyst with an explicit edit grant (not the owner) must not
        see the pending request in `incoming` -- deciding is owner/admin
        business, matching the approve/decline gate."""
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="edit")
        _create_request(member, "dashboard", dashboard, permission="view")

        editor_view = _list_requests(analyst2)["data"]
        assert editor_view["incoming"] == []

    def test_decided_requests_excluded_from_incoming_but_kept_in_outgoing(
        self, org, analyst, member
    ):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        _approve(analyst, created["data"]["id"])

        owner_view = _list_requests(analyst)["data"]
        assert owner_view["incoming"] == []

        requester_view = _list_requests(member)["data"]
        assert len(requester_view["outgoing"]) == 1
        assert requester_view["outgoing"][0]["status"] == "approved"


# ================================================================================
# POST /api/access/requests/{id}/approve/  and  /decline/
# ================================================================================


class TestApproveAccessRequest:
    def test_owner_approves_grant_exists_and_requester_notified(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        Notification.objects.all().delete()  # clear the "new request" notification

        response = _approve(analyst, created["data"]["id"])

        assert response["success"] is True
        assert response["data"]["status"] == "approved"
        share = ResourceShare.objects.get(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        )
        assert share.permission == "view"
        assert share.status == "active"
        assert effective_permission(member, "dashboard", dashboard) == "view"

        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient_id == member.id

    def test_decision_notification_has_no_actionable_payload(self, org, analyst, member):
        """Only the "new request" notification is actionable -- the
        requester's decision notification carries no `metadata` (there is
        nothing left to decide)."""
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        Notification.objects.all().delete()

        _approve(analyst, created["data"]["id"])

        notification = Notification.objects.get()
        assert notification.metadata is None

    def test_owner_downgrades_edit_request_to_view_on_approve(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="edit")

        _approve(analyst, created["data"]["id"], permission="view")

        share = ResourceShare.objects.get(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        )
        assert share.permission == "view"

    def test_owner_cannot_escalate_above_requested_permission(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")

        with pytest.raises(HttpError) as excinfo:
            _approve(analyst, created["data"]["id"], permission="edit")
        assert excinfo.value.status_code == 400
        assert not ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        ).exists()

    def test_double_decide_400(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        _approve(analyst, created["data"]["id"])

        with pytest.raises(HttpError) as excinfo:
            _approve(analyst, created["data"]["id"])
        assert excinfo.value.status_code == 400

    def test_editor_via_grant_cannot_approve(self, org, analyst, analyst2, member):
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="edit")
        created = _create_request(member, "dashboard", dashboard, permission="view")

        with pytest.raises(HttpError) as excinfo:
            _approve(analyst2, created["data"]["id"])
        assert excinfo.value.status_code == 403
        assert not ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        ).exists()

    def test_admin_can_approve(self, org, admin, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")

        response = _approve(admin, created["data"]["id"])
        assert response["success"] is True

    def test_member_owner_without_share_slug_can_approve(self, org, member):
        """Deciding a request gates on `require_owner_access` ONLY (no
        `require_share_permission`) -- a Member who owns a resource (e.g.
        via ownership transfer, Task 12) holds no `can_share_dashboards`
        slug but must still be able to decide requests on their own
        resource."""
        from ddpui.auth import MEMBER_ROLE

        dashboard = _dashboard(org, member)
        requester = _make_orguser(org, MEMBER_ROLE, "access-req-second-member")
        assert not RolePermission.objects.filter(
            role=member.new_role, permission__slug="can_share_dashboards"
        ).exists()

        created = _create_request(requester, "dashboard", dashboard, permission="view")
        response = _approve(member, created["data"]["id"])

        assert response["success"] is True
        assert ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=requester.id,
        ).exists()
        requester.delete()

    def test_member_owner_without_share_slug_can_decline(self, org, member):
        from ddpui.auth import MEMBER_ROLE

        dashboard = _dashboard(org, member)
        requester = _make_orguser(org, MEMBER_ROLE, "access-req-second-member-b")

        created = _create_request(requester, "dashboard", dashboard, permission="view")
        response = _decline(member, created["data"]["id"])

        assert response["success"] is True
        assert not ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=requester.id,
        ).exists()
        requester.delete()

    def test_cross_org_request_id_404(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")

        other_org = Org.objects.create(name="Access Req Other Org", slug="access-req-other-org")
        outsider = _make_orguser(other_org, ANALYST_ROLE, "access-req-outsider")

        with pytest.raises(HttpError) as excinfo:
            _approve(outsider, created["data"]["id"])
        assert excinfo.value.status_code == 404
        outsider.delete()
        other_org.delete()

    def test_anonymous_401(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")

        client = Client()
        response = client.post(f"/api/access/requests/{created['data']['id']}/approve/")
        assert response.status_code == 401

    def test_metric_request_approve_grants_access_bypassing_grants_false_flag(
        self, org, analyst, member
    ):
        """`metric` has `grants=False` (its public POST /grants/ 400s), but
        approving a request still inserts a ResourceShare row via the
        internal write path -- same pattern as ownership transfer (Task
        12)."""
        metric = Metric.objects.create(
            org=org,
            name="access-req-metric",
            schema_name="s",
            table_name="t",
            column="c",
            aggregation="sum",
            created_by=analyst,
            owner=analyst,
            general_audience=GeneralAudience.PRIVATE,
        )
        created = _create_request(member, "metric", metric, permission="view")

        response = _approve(analyst, created["data"]["id"])

        assert response["success"] is True
        share = ResourceShare.objects.get(
            org=org, resource_type="metric", resource_id=str(metric.pk), principal_id=member.id
        )
        assert share.permission == "view"
        assert effective_permission(member, "metric", metric) == "view"


class TestDeclineAccessRequest:
    def test_decline_no_grant_and_requester_notified(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        Notification.objects.all().delete()

        response = _decline(analyst, created["data"]["id"])

        assert response["success"] is True
        assert response["data"]["status"] == "declined"
        assert not ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        ).exists()
        assert effective_permission(member, "dashboard", dashboard) is None

        assert Notification.objects.count() == 1
        recipient = NotificationRecipient.objects.first()
        assert recipient.recipient_id == member.id

    def test_anonymous_401(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")

        client = Client()
        response = client.post(f"/api/access/requests/{created['data']['id']}/decline/")
        assert response.status_code == 401


# ================================================================================
# Expiry
# ================================================================================


class TestAccessRequestExpiry:
    def test_approve_on_expired_request_400(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        access_request = AccessRequest.objects.get(id=created["data"]["id"])
        access_request.expires_at = django_timezone.now() - timedelta(days=1)
        access_request.save(update_fields=["expires_at"])

        with pytest.raises(HttpError) as excinfo:
            _approve(analyst, access_request.id)
        assert excinfo.value.status_code == 400
        assert not ResourceShare.objects.filter(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=member.id,
        ).exists()

    def test_cleanup_task_marks_stale_pending_requests_expired(self, org, analyst, member):
        from ddpui.core.sharing.access_requests import expire_stale_requests

        dashboard = _dashboard(org, analyst)
        created = _create_request(member, "dashboard", dashboard, permission="view")
        stale = AccessRequest.objects.get(id=created["data"]["id"])
        stale.expires_at = django_timezone.now() - timedelta(days=1)
        stale.save(update_fields=["expires_at"])

        # a second, still-fresh pending request must be left untouched
        other_dashboard = _dashboard(org, analyst)
        other_dashboard.title = "Access Req Fresh Dashboard"
        other_dashboard.save(update_fields=["title"])
        fresh_request = _create_request(member, "dashboard", other_dashboard, permission="view")

        updated_count = expire_stale_requests()

        assert updated_count == 1
        stale.refresh_from_db()
        assert stale.status == "expired"
        fresh_row = AccessRequest.objects.get(id=fresh_request["data"]["id"])
        assert fresh_row.status == "pending"


# ================================================================================
# Notification deep links -- params must match what the webapp actually reads
# ================================================================================


class TestNotificationDeepLinks:
    """Pins the per-rtype deep-link shape (Task 15b). The query params are a
    contract with webapp_v2's pages: /alerts reads `?alertId=`, /metrics reads
    `?highlight=` (row highlight), /kpis reads `?open=` (opens the detail
    drawer); dashboards/reports route by path."""

    def test_deep_link_shape_per_rtype(self):
        from ddpui.core.sharing.access_requests import _build_resource_url, _frontend_url

        base = _frontend_url()
        assert _build_resource_url("dashboard", 7) == f"{base}/dashboards/7"
        assert _build_resource_url("report", 7) == f"{base}/reports/7"
        assert _build_resource_url("alert", 7) == f"{base}/alerts?alertId=7"
        assert _build_resource_url("metric", 7) == f"{base}/metrics?highlight=7"
        assert _build_resource_url("kpi", 7) == f"{base}/kpis?open=7"

    def test_metric_request_notification_carries_highlight_link(self, org, analyst, member):
        metric = Metric.objects.create(
            org=org,
            name="deep-link-metric",
            schema_name="s",
            table_name="t",
            column="c",
            aggregation="sum",
            created_by=analyst,
            owner=analyst,
            general_audience=GeneralAudience.PRIVATE,
        )
        _create_request(member, "metric", metric, permission="view")

        notification = Notification.objects.get()
        assert f"/metrics?highlight={metric.pk}" in notification.message
