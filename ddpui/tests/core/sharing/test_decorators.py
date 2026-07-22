"""v1.2 decorator gates: pool truth-table, the ② 404 wall, and ③'s
403-wording contract (the webapp matches on those strings)."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.auth import extract_resource, has_resource_permission
from ddpui.core.sharing.access_resolver import get_resource_permissions
from ddpui.models.general_access import AccessLevel
from ddpui.models.resource_share import ResourceShare
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_access_resolver import (
    _dashboard,
    _make_orguser,
    admin,
    analyst,
    member,
    org,
)

pytestmark = pytest.mark.django_db


def _grant(org_obj, orguser, resource, level, rtype="dashboard"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=orguser.id,
        permission=level,
        status="active",
    )


def _perms(orguser, resource, rtype="dashboard"):
    return get_resource_permissions(orguser, rtype, resource)


# ---------------------------------------------------------------- pool truth-table


def test_member_edit_grant_puts_edit_in_pool_only_on_that_dashboard(org, member, analyst):
    """The Ishan case (plan §1): Member + admin-granted Edit on dashboard A
    edits A; dashboard B stays view-less."""
    dash_a = _dashboard(org, owner=analyst)
    dash_b = _dashboard(org, owner=analyst)
    _grant(org, member, dash_a, "edit")

    assert "can_edit_dashboards" in _perms(member, dash_a)
    assert "can_edit_dashboards" not in _perms(member, dash_b)


def test_view_grant_contributes_view_not_edit(org, member, analyst):
    dash = _dashboard(org, owner=analyst)
    _grant(org, member, dash, "view")
    pool = _perms(member, dash)
    assert "can_view_dashboards" in pool
    assert "can_edit_dashboards" not in pool


def test_edit_grant_implies_view(org, member, analyst):
    dash = _dashboard(org, owner=analyst)
    _grant(org, member, dash, "edit")
    pool = _perms(member, dash)
    assert {"can_edit_dashboards", "can_view_dashboards"} <= pool


def test_member_floor_contributes_its_level(org, analyst, member):
    """Floors are a pool source too: member_level="edit" gives every Member
    edit on this dashboard, no grant row needed."""
    dash = _dashboard(org, owner=analyst, member_level=AccessLevel.EDIT)
    assert "can_edit_dashboards" in _perms(member, dash)
    dash_none = _dashboard(org, owner=analyst, member_level=AccessLevel.NONE)
    assert "can_edit_dashboards" not in _perms(member, dash_none)


def test_owner_and_admin_hold_view_and_edit(org, admin, member, analyst):
    dash = _dashboard(org, owner=member, analyst_level=AccessLevel.NONE)
    assert {"can_view_dashboards", "can_edit_dashboards"} <= _perms(member, dash)
    assert {"can_view_dashboards", "can_edit_dashboards"} <= _perms(admin, dash)


def test_member_edit_grant_still_capped_on_report_rtype(org, member, analyst):
    from ddpui.models.report import ReportSnapshot

    report = ReportSnapshot.objects.create(
        title="R",
        org=org,
        owner=analyst,
        created_by=analyst,
        analyst_level=AccessLevel.NONE,
        member_level=AccessLevel.NONE,
    )
    _grant(org, member, report, "edit", rtype="report")
    pool = get_resource_permissions(member, "report", report)
    # report has no view/edit slugs in seeds; the capped fallback maps to the
    # view slug, which is None for reports — so nothing enters the pool.
    assert pool == set()


def test_role_slugs_are_not_a_pool_source(org, member, analyst):
    """Role slugs answer ① only. If they pooled, every Member would hold
    can_view_dashboards on every dashboard, erasing floors and list scoping."""
    dash = _dashboard(org, owner=analyst, member_level=AccessLevel.NONE)
    assert _perms(member, dash) == set()


# ---------------------------------------------------------------- decorators


def _gated_endpoint():
    @extract_resource("dashboard")
    @has_resource_permission("can_edit_dashboards")
    def endpoint(request, dashboard_id: int):
        return request.resource

    return endpoint


def test_extract_resource_cross_org_is_404(org, seed_db, analyst):
    from ddpui.models.org import Org

    other_org = Org.objects.create(name="Other", slug="other-org", airbyte_workspace_id="wo")
    other_analyst = _make_orguser(other_org, ANALYST_ROLE, "otherana")
    dash = _dashboard(org, owner=analyst)

    endpoint = _gated_endpoint()
    with pytest.raises(HttpError) as excinfo:
        endpoint(mock_request(other_analyst), dashboard_id=dash.id)
    assert excinfo.value.status_code == 404
    assert str(excinfo.value.message) == "Dashboard not found"


def test_missing_resource_is_404(org, analyst):
    endpoint = _gated_endpoint()
    with pytest.raises(HttpError) as excinfo:
        endpoint(mock_request(analyst), dashboard_id=999999)
    assert excinfo.value.status_code == 404


def test_denied_edit_wording_contract(org, member, analyst):
    dash = _dashboard(org, owner=analyst)
    _grant(org, member, dash, "view")
    endpoint = _gated_endpoint()
    with pytest.raises(HttpError) as excinfo:
        endpoint(mock_request(member), dashboard_id=dash.id)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value.message) == "You do not have edit access to this dashboard"


def test_denied_view_wording_contract(org, member, analyst):
    dash = _dashboard(org, owner=analyst)

    @extract_resource("dashboard")
    @has_resource_permission("can_view_dashboards")
    def view_endpoint(request, dashboard_id: int):
        return request.resource

    with pytest.raises(HttpError) as excinfo:
        view_endpoint(mock_request(member), dashboard_id=dash.id)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value.message) == "You do not have access to this dashboard"


def test_member_with_edit_grant_passes_edit_gate(org, member, analyst):
    dash = _dashboard(org, owner=analyst)
    _grant(org, member, dash, "edit")
    endpoint = _gated_endpoint()
    assert endpoint(mock_request(member), dashboard_id=dash.id) == dash


def test_pool_attached_to_request(org, analyst, member):
    dash = _dashboard(org, owner=analyst)

    @extract_resource("dashboard")
    @has_resource_permission("can_view_dashboards")
    def endpoint(request, dashboard_id: int):
        return request.resource_permissions

    pool = endpoint(mock_request(analyst), dashboard_id=dash.id)
    assert "can_edit_dashboards" in pool  # owner contribution


def test_positional_call_binds_route_param(org, analyst):
    """Tests call endpoint functions positionally; ② must bind those too."""
    dash = _dashboard(org, owner=analyst)
    endpoint = _gated_endpoint()
    assert endpoint(mock_request(analyst), dash.id) == dash


def test_decorator_stack_costs_two_queries(org, member, analyst, django_assert_num_queries):
    """②+③ overhead is pinned: 1 resource fetch + 1 grants query (group ids
    ride as a subquery). A regression here means an N+1 crept in."""
    dash = _dashboard(org, owner=analyst)
    _grant(org, member, dash, "edit")
    endpoint = _gated_endpoint()
    request = mock_request(member)
    with django_assert_num_queries(2):
        endpoint(request, dashboard_id=dash.id)


def test_detail_response_carries_pool_level(org, member, analyst):
    """The webapp's edit affordance reads `user_permission` off the detail
    GET — "edit" for a Member holding an edit grant, "view" via floor only."""
    from ddpui.api.dashboard_native_api import get_dashboard

    granted = _dashboard(org, owner=analyst, created_by=analyst)
    _grant(org, member, granted, "edit")
    floor_only = _dashboard(org, owner=analyst, created_by=analyst, member_level=AccessLevel.VIEW)

    assert get_dashboard(mock_request(member), dashboard_id=granted.id).user_permission == "edit"
    assert get_dashboard(mock_request(member), dashboard_id=floor_only.id).user_permission == "view"


def test_unknown_slug_fails_at_decoration_time():
    with pytest.raises(ValueError):
        has_resource_permission("can_edit_dashbords")  # typo


def test_unknown_rtype_fails_at_decoration_time():
    with pytest.raises(ValueError):
        extract_resource("dashbord")
