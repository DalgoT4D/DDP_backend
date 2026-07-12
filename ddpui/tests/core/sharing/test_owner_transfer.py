"""Task 12: ownership transfer — `POST /api/access/{rtype}/{resource_id}/owner/`.

Gate: CURRENT owner (`ownership.can_delete_resource` semantics: owner FK,
`created_by` fallback) OR admin/super-admin, PLUS the rtype's share-permission
slug (transfer is a sharing mutation, same dynamic registry gate every other
`/api/access/*` mutation uses).

Effect: `resource.owner` flips to the new owner; the OLD owner receives an
explicit active Edit `ResourceShare` grant (uniform rule, even when the old
owner is also `created_by` and would already be admitted via
`accessible_filter`'s `created_by` clause). No reclaim -- the old owner simply
stops passing the owner gate.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
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


def _transfer(caller, rtype, resource, new_owner):
    from ddpui.api.access_api import transfer_owner
    from ddpui.schemas.access_schema import OwnerTransferRequest

    new_owner_id = new_owner.id if hasattr(new_owner, "id") else new_owner
    payload = OwnerTransferRequest(new_owner_orguser_id=new_owner_id)
    return transfer_owner(mock_request(caller), rtype, str(resource.pk), payload)


class TestOwnerTransfer:
    def test_owner_transfers_dashboard_owner_flips_and_old_owner_gets_edit_grant(
        self, org, analyst, analyst2
    ):
        dashboard = _dashboard(org, analyst)

        response = _transfer(analyst, "dashboard", dashboard, analyst2)

        assert response["success"] is True
        assert response["data"]["orguser_id"] == analyst2.id
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst2.id

        share = ResourceShare.objects.get(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_id=analyst.id,
        )
        assert share.permission == "edit"
        assert share.status == "active"

        # old owner still resolves to edit via the explicit grant
        assert effective_permission(analyst, "dashboard", dashboard) == "edit"

        # old owner can no longer transfer -- they fail the owner gate
        with pytest.raises(HttpError) as excinfo:
            _transfer(analyst, "dashboard", dashboard, analyst)
        assert excinfo.value.status_code == 403

        # new owner can transfer again (e.g. back, or onward)
        third = _make_orguser(org, ANALYST_ROLE, "owner-transfer-third")
        response = _transfer(analyst2, "dashboard", dashboard, third)
        assert response["data"]["orguser_id"] == third.id
        third.delete()

    def test_admin_non_owner_can_transfer(self, org, admin, analyst, analyst2):
        dashboard = _dashboard(org, analyst)

        response = _transfer(admin, "dashboard", dashboard, analyst2)

        assert response["success"] is True
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst2.id

    def test_editor_via_grant_cannot_transfer(self, org, analyst, analyst2, member):
        """An Analyst with an explicit edit grant (not the owner) resolves
        to "edit" via the resolver, but must still fail the owner gate --
        proving `require_owner_access` is stricter than `require_edit_access`."""
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="edit")
        assert effective_permission(analyst2, "dashboard", dashboard) == "edit"

        with pytest.raises(HttpError) as excinfo:
            _transfer(analyst2, "dashboard", dashboard, member)
        assert excinfo.value.status_code == 403
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst.id

    def test_cross_org_new_owner_404(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        other_org = Org.objects.create(name="Owner Transfer Other Org", slug="owner-xfer-other-org")
        outsider = _make_orguser(other_org, ANALYST_ROLE, "owner-xfer-outsider")

        with pytest.raises(HttpError) as excinfo:
            _transfer(analyst, "dashboard", dashboard, outsider)
        assert excinfo.value.status_code == 404
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst.id
        outsider.delete()
        other_org.delete()

    def test_inactive_orguser_new_owner_404(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        inactive = _make_orguser(org, ANALYST_ROLE, "owner-xfer-inactive")
        inactive.user.is_active = False
        inactive.user.save()

        with pytest.raises(HttpError) as excinfo:
            _transfer(analyst, "dashboard", dashboard, inactive)
        assert excinfo.value.status_code == 404
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst.id
        inactive.delete()

    def test_self_transfer_400(self, org, analyst):
        dashboard = _dashboard(org, analyst)

        with pytest.raises(HttpError) as excinfo:
            _transfer(analyst, "dashboard", dashboard, analyst)
        assert excinfo.value.status_code == 400
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst.id

    def test_transfer_of_resource_with_null_created_by_survives(self, org, analyst, analyst2):
        """SET_NULL survival: `created_by` is null (the creator was deleted)
        but `owner` is still set -- transfer must work off the owner FK."""
        dashboard = _dashboard(org, analyst)
        dashboard.created_by = None
        dashboard.save(update_fields=["created_by"])

        response = _transfer(analyst, "dashboard", dashboard, analyst2)
        assert response["success"] is True
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst2.id
        assert dashboard.created_by_id is None

    def test_transfer_of_fully_orphaned_resource_owner_and_created_by_null(
        self, org, admin, analyst
    ):
        """Both `owner` and `created_by` are null (fully orphaned row) --
        an admin can still assign an owner; there is no old owner to grant."""
        dashboard = _dashboard(org, analyst)
        dashboard.owner = None
        dashboard.created_by = None
        dashboard.save(update_fields=["owner", "created_by"])

        response = _transfer(admin, "dashboard", dashboard, analyst)
        assert response["success"] is True
        dashboard.refresh_from_db()
        assert dashboard.owner_id == analyst.id
        assert not ResourceShare.objects.filter(
            org=org, resource_type="dashboard", resource_id=str(dashboard.pk)
        ).exists()

    def test_member_new_owner_can_delete_and_resolver_edit(self, org, analyst, member):
        """Ownership is per-resource, not role-gated -- a Member new owner
        passes `can_delete_resource` and resolves to edit on this resource."""
        from ddpui.core.ownership import can_delete_resource

        dashboard = _dashboard(org, analyst)

        response = _transfer(analyst, "dashboard", dashboard, member)
        assert response["success"] is True
        dashboard.refresh_from_db()
        assert dashboard.owner_id == member.id

        assert can_delete_resource(member, dashboard) is True
        assert effective_permission(member, "dashboard", dashboard) == "edit"

    def test_metric_grantless_rtype_old_owner_still_gets_edit_grant_row(
        self, org, analyst, analyst2
    ):
        """`metric` has `grants=False` (the public POST /grants/ 400s for
        it), but that capability flag gates the grants ENDPOINT, not the
        ownership-transfer action. The old owner still receives an internal
        ResourceShare Edit row so their access stays consistent even though
        they could never have created that row via POST /grants/."""
        metric = Metric.objects.create(
            org=org,
            name="owner-xfer-metric",
            schema_name="s",
            table_name="t",
            column="c",
            aggregation="sum",
            created_by=analyst,
            owner=analyst,
        )

        response = _transfer(analyst, "metric", metric, analyst2)

        assert response["success"] is True
        metric.refresh_from_db()
        assert metric.owner_id == analyst2.id

        share = ResourceShare.objects.get(
            org=org, resource_type="metric", resource_id=str(metric.pk), principal_id=analyst.id
        )
        assert share.permission == "edit"
        assert share.status == "active"
        assert effective_permission(analyst, "metric", metric) == "edit"

    def test_get_access_overview_reflects_new_owner_immediately(self, org, analyst, analyst2):
        """GET /api/access/{rtype}/{id}/ reads the owner FK directly, so a
        transfer is reflected on the very next read -- no separate wiring."""
        from ddpui.api.access_api import get_access

        dashboard = _dashboard(org, analyst)
        _transfer(analyst, "dashboard", dashboard, analyst2)

        response = get_access(mock_request(analyst2), "dashboard", str(dashboard.pk))
        data = response["data"]
        assert data["owner"]["orguser_id"] == analyst2.id
        assert data["owner"]["email"] == analyst2.user.email
        assert data["viewer"] == {"effective_permission": "edit", "is_owner": True}
