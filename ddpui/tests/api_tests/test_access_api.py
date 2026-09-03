"""Tests for the resource-share (access) API — ownership transfer + candidates.

Spec: features/access-control/resource-sharing/v1/spec.md §"Ownership transfer"
(lines 265–274). Rules covered here:
- Only owner or Admin can initiate.
- Recipient must have effective Edit on the resource.
- Previous owner's direct shares are unchanged after transfer.
- Groups cannot be owners (indirect — /candidates only returns OrgUsers).
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.models.resource_share import (
    AccessLevel,
    AccessRequest,
    AccessRequestStatus,
    ResourceShare,
    ResourceSharePrincipalType,
    ResourceType,
)
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart
from ddpui.models.metric import Metric, KPI
from ddpui.core.access.access_control import (
    accessible_filter,
    get_user_access,
    get_user_access_map,
)
from ddpui.core.access.resource_share import sync_dashboard_cascade
from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.api.access_api import (
    add_resource_grants,
    create_access_request,
    list_access_requests,
    list_resource_grants,
    list_transfer_candidates,
    remove_resource_grant,
    respond_to_access_request,
    transfer_resource_ownership,
    update_general_access,
    update_resource_grant,
)
from ddpui.schemas.access.resource_share_schema import (
    AddGrantsPayload,
    GeneralAccessPayload,
    PrincipalGrantPayload,
    RequestAccessPayload,
    RespondToRequestPayload,
    TransferOwnershipPayload,
    UpdateGrantPayload,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def org():
    org = Org.objects.create(name="Access API Test Org", slug="access-test-org")
    yield org
    org.delete()


def _make_user(email: str, org: Org, role_slug: str) -> OrgUser:
    u = User.objects.create(username=email, email=email, password="pw")
    return OrgUser.objects.create(
        user=u, org=org, new_role=Role.objects.filter(slug=role_slug).first()
    )


@pytest.fixture
def admin(org, seed_db):
    ou = _make_user("admin@t.com", org, ADMIN_ROLE)
    yield ou
    ou.user.delete()  # cascades to OrgUser


@pytest.fixture
def owner_analyst(org, seed_db):
    """Analyst who owns the fixture dashboard."""
    ou = _make_user("owner@t.com", org, ANALYST_ROLE)
    yield ou
    ou.user.delete()


@pytest.fixture
def other_analyst(org, seed_db):
    """Another Analyst. With default Analyst floor = Edit, has implicit Edit
    on any non-private resource — qualifies as a transfer target."""
    ou = _make_user("analyst@t.com", org, ANALYST_ROLE)
    yield ou
    ou.user.delete()


@pytest.fixture
def member(org, seed_db):
    """Member — default Member floor = View, so no implicit Edit."""
    ou = _make_user("member@t.com", org, MEMBER_ROLE)
    yield ou
    ou.user.delete()


@pytest.fixture
def dashboard(org, owner_analyst):
    """A dashboard owned by owner_analyst."""
    d = Dashboard.objects.create(title="Test Dashboard", org=org, created_by=owner_analyst)
    yield d
    d.delete()


def _grant(org, dashboard, orguser, level):
    """Direct user share on the dashboard."""
    return ResourceShare.objects.create(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=orguser.id,
        access_level=level,
    )


# ---------------------------------------------------------------------------
# Transfer ownership — happy paths
# ---------------------------------------------------------------------------


def test_owner_can_transfer_to_analyst_with_floor_edit(dashboard, owner_analyst, other_analyst):
    """Analyst floor=Edit gives the recipient implicit Edit — transfer succeeds."""
    request = mock_request(owner_analyst)
    transfer_resource_ownership(
        request,
        "dashboard",
        str(dashboard.id),
        TransferOwnershipPayload(to_orguser_id=other_analyst.id),
    )
    dashboard.refresh_from_db()
    assert dashboard.created_by_id == other_analyst.id


def test_admin_can_transfer_ownership_even_when_not_owner(dashboard, admin, other_analyst):
    """Admin who did not create the resource can still transfer it."""
    request = mock_request(admin)
    transfer_resource_ownership(
        request,
        "dashboard",
        str(dashboard.id),
        TransferOwnershipPayload(to_orguser_id=other_analyst.id),
    )
    dashboard.refresh_from_db()
    assert dashboard.created_by_id == other_analyst.id


def test_owner_can_transfer_to_member_with_direct_edit_share(org, dashboard, owner_analyst, member):
    """Member (floor=View) with a direct Edit share qualifies (spec line 271)."""
    _grant(org, dashboard, member, AccessLevel.EDIT)
    request = mock_request(owner_analyst)
    transfer_resource_ownership(
        request, "dashboard", str(dashboard.id), TransferOwnershipPayload(to_orguser_id=member.id)
    )
    dashboard.refresh_from_db()
    assert dashboard.created_by_id == member.id


# ---------------------------------------------------------------------------
# Transfer ownership — guardrails
# ---------------------------------------------------------------------------


def test_non_owner_non_admin_cannot_transfer(dashboard, other_analyst, member):
    """Analyst with Edit floor (not owner, not admin) is blocked (spec line 270)."""
    request = mock_request(other_analyst)
    with pytest.raises(HttpError) as exc:
        transfer_resource_ownership(
            request,
            "dashboard",
            str(dashboard.id),
            TransferOwnershipPayload(to_orguser_id=member.id),
        )
    assert "owner or an admin" in str(exc.value)


def test_transfer_to_recipient_without_edit_fails(dashboard, owner_analyst, member):
    """Member has floor=View and no direct share → not eligible (spec line 271)."""
    request = mock_request(owner_analyst)
    with pytest.raises(HttpError) as exc:
        transfer_resource_ownership(
            request,
            "dashboard",
            str(dashboard.id),
            TransferOwnershipPayload(to_orguser_id=member.id),
        )
    assert "Edit" in str(exc.value)


def test_transfer_to_nonexistent_user_fails(dashboard, owner_analyst):
    """Recipient must exist in the same org."""
    request = mock_request(owner_analyst)
    with pytest.raises(HttpError) as exc:
        transfer_resource_ownership(
            request, "dashboard", str(dashboard.id), TransferOwnershipPayload(to_orguser_id=999999)
        )
    assert "recipient" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Transfer ownership — post-transfer state
# ---------------------------------------------------------------------------


def test_previous_owner_existing_share_upgraded_to_edit_after_transfer(
    org, dashboard, owner_analyst, other_analyst
):
    """Previous owner with an existing direct share has it upgraded to Edit."""
    prev_owner_share = _grant(org, dashboard, owner_analyst, AccessLevel.VIEW)
    request = mock_request(owner_analyst)
    transfer_resource_ownership(
        request,
        "dashboard",
        str(dashboard.id),
        TransferOwnershipPayload(to_orguser_id=other_analyst.id),
    )
    prev_owner_share.refresh_from_db()
    assert prev_owner_share.access_level == AccessLevel.EDIT
    assert prev_owner_share.principal_id == owner_analyst.id


def test_previous_owner_gets_new_edit_share_when_no_direct_share_exists(
    org, dashboard, owner_analyst, other_analyst
):
    """Previous owner with no direct share gets a new Edit share after transfer."""
    request = mock_request(owner_analyst)
    transfer_resource_ownership(
        request,
        "dashboard",
        str(dashboard.id),
        TransferOwnershipPayload(to_orguser_id=other_analyst.id),
    )
    share = ResourceShare.objects.filter(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=owner_analyst.id,
        parent=None,
    ).first()
    assert share is not None
    assert share.access_level == AccessLevel.EDIT


def test_transfer_updates_created_by(dashboard, owner_analyst, other_analyst):
    """The recipient is the new owner after transfer."""
    request = mock_request(owner_analyst)
    transfer_resource_ownership(
        request,
        "dashboard",
        str(dashboard.id),
        TransferOwnershipPayload(to_orguser_id=other_analyst.id),
    )
    dashboard.refresh_from_db()
    assert dashboard.created_by_id == other_analyst.id
    # Sanity: previous owner is no longer the owner.
    assert dashboard.created_by_id != owner_analyst.id


# ---------------------------------------------------------------------------
# Candidates endpoint
# ---------------------------------------------------------------------------


def test_candidates_403_for_non_owner_non_admin(dashboard, other_analyst):
    """/candidates is restricted to creator/admin — matches transfer gate."""
    request = mock_request(other_analyst)
    with pytest.raises(HttpError) as exc:
        list_transfer_candidates(request, "dashboard", str(dashboard.id))
    assert exc.value.status_code == 403


def test_candidates_returns_all_org_users_with_access_levels(
    org, dashboard, owner_analyst, other_analyst, member, admin
):
    """Every OrgUser is returned; access_level reflects the resource-specific
    effective level (floor, direct grant, admin override)."""
    request = mock_request(owner_analyst)
    result = list_transfer_candidates(request, "dashboard", str(dashboard.id))
    by_id = {c.orguser_id: c for c in result}

    # Owner marked as such, has Edit
    assert by_id[owner_analyst.id].is_owner is True
    assert by_id[owner_analyst.id].access_level == AccessLevel.EDIT
    # Admin has implicit Edit
    assert by_id[admin.id].access_level == AccessLevel.EDIT
    # Analyst gets Edit via floor (default Analyst floor = Edit)
    assert by_id[other_analyst.id].access_level == AccessLevel.EDIT
    # Member has no share and floor=View → View
    assert by_id[member.id].access_level == AccessLevel.VIEW


def test_candidates_direct_edit_share_promotes_member(org, dashboard, owner_analyst, member):
    """A Member with a direct Edit share should show as Edit in candidates
    (making them a valid transfer target — spec line 271)."""
    _grant(org, dashboard, member, AccessLevel.EDIT)
    request = mock_request(owner_analyst)
    result = list_transfer_candidates(request, "dashboard", str(dashboard.id))
    member_row = next(c for c in result if c.orguser_id == member.id)
    assert member_row.access_level == AccessLevel.EDIT


def test_candidates_private_resource_bypasses_floor(org, dashboard, owner_analyst, other_analyst):
    """On a private resource, floor is ignored — Analysts without a direct
    share drop to no_access. Only explicit grantees keep Edit/View."""
    dashboard.is_private = True
    dashboard.save(update_fields=["is_private"])
    request = mock_request(owner_analyst)
    result = list_transfer_candidates(request, "dashboard", str(dashboard.id))
    other = next(c for c in result if c.orguser_id == other_analyst.id)
    assert other.access_level == AccessLevel.NO_ACCESS


# ---------------------------------------------------------------------------
# General access endpoint — spec §"Private toggle" + spec §"Public sharing"
# ---------------------------------------------------------------------------


def _set_mode(caller, rtype, resource_id, mode):
    return update_general_access(
        mock_request(caller), rtype, str(resource_id), GeneralAccessPayload(mode=mode)
    )


def test_internal_to_private_sets_is_private(dashboard, owner_analyst):
    """Mode=private → is_private flips on."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "private")
    dashboard.refresh_from_db()
    assert dashboard.is_private is True
    assert dashboard.is_public is False


def test_internal_to_public_generates_token_and_timestamps(dashboard, owner_analyst):
    """Mode=public → is_public on, token generated, public_shared_at set."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    assert dashboard.is_public is True
    assert dashboard.is_private is False
    assert dashboard.public_share_token
    assert dashboard.public_shared_at is not None
    assert dashboard.public_disabled_at is None


def test_public_to_private_keeps_token_dormant(dashboard, owner_analyst):
    """Going private disables the public link (is_public=False) but preserves the
    token so re-enabling public later reuses the same URL. The public endpoint
    gates on is_public, not token existence."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    original_token = dashboard.public_share_token
    assert original_token  # sanity

    _set_mode(owner_analyst, "dashboard", dashboard.id, "private")
    dashboard.refresh_from_db()
    assert dashboard.is_private is True
    assert dashboard.is_public is False
    assert dashboard.public_share_token == original_token  # dormant, preserved


def test_public_to_internal_keeps_token_dormant(dashboard, owner_analyst):
    """Turning public off (to everyone) preserves the token as dormant so
    the owner can re-enable later without a new URL. public_disabled_at set."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    original_token = dashboard.public_share_token

    _set_mode(owner_analyst, "dashboard", dashboard.id, "internal")
    dashboard.refresh_from_db()
    assert dashboard.is_public is False
    assert dashboard.public_share_token == original_token  # dormant, not cleared
    assert dashboard.public_disabled_at is not None


def test_public_url_survives_private_round_trip(dashboard, owner_analyst):
    """After public → private → public, the same public_share_token comes back so
    the previously-shared URL still works."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    original_token = dashboard.public_share_token
    assert original_token

    _set_mode(owner_analyst, "dashboard", dashboard.id, "private")
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    assert dashboard.is_public is True
    assert dashboard.public_share_token == original_token  # same URL


def test_private_to_internal_does_not_reenable_public_spec_line_263(dashboard, owner_analyst):
    """Spec line 263: 'turning [Private] off does not restore the public
    link — the owner must re-enable it manually.' Token stays dormant across
    the round-trip so a later re-enable reuses the same URL."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    original_token = dashboard.public_share_token
    _set_mode(owner_analyst, "dashboard", dashboard.id, "private")
    _set_mode(owner_analyst, "dashboard", dashboard.id, "internal")
    dashboard.refresh_from_db()
    assert dashboard.is_public is False  # still off, not silently re-enabled
    assert dashboard.public_share_token == original_token  # preserved for later re-enable


def test_public_on_chart_fails_400(org, owner_analyst):
    """Charts don't support public sharing (no is_public field) → 400."""
    chart = Chart.objects.create(
        title="c",
        org=org,
        created_by=owner_analyst,
        chart_type="bar",
        schema_name="s",
        table_name="t",
        computation_type="raw",
        extra_config={},
    )
    with pytest.raises(HttpError) as exc:
        _set_mode(owner_analyst, "chart", chart.id, "public")
    assert exc.value.status_code == 400
    chart.delete()


def test_public_blocked_when_org_disallows(org, dashboard, owner_analyst):
    """Spec §"Access Defaults": admin toggle off → cannot enable Public."""
    OrgPreferences.objects.create(org=org, allow_public_sharing=False)
    with pytest.raises(HttpError) as exc:
        _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    assert exc.value.status_code == 403


def test_non_edit_holder_cannot_change_mode(dashboard, member):
    """Requires Edit access to change mode (spec: 'Requires ownership or Edit')."""
    with pytest.raises(HttpError) as exc:
        _set_mode(member, "dashboard", dashboard.id, "private")
    assert exc.value.status_code == 403


def test_idempotent_no_state_change(dashboard, owner_analyst):
    """Setting the same mode twice: second call is a no-op on flags/tokens."""
    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    first_token = dashboard.public_share_token
    first_shared_at = dashboard.public_shared_at

    _set_mode(owner_analyst, "dashboard", dashboard.id, "public")
    dashboard.refresh_from_db()
    # Token doesn't churn; public_shared_at bumps (acceptable — records the action)
    assert dashboard.public_share_token == first_token
    assert dashboard.is_public is True


# ---------------------------------------------------------------------------
# GET /grants — owner field + caller_is_owner + auth gate
# ---------------------------------------------------------------------------


def test_grants_response_populates_owner_field(dashboard, owner_analyst):
    """Owner is surfaced separately from the shares list (no share row exists)."""
    request = mock_request(owner_analyst)
    result = list_resource_grants(request, "dashboard", str(dashboard.id))
    assert result.owner is not None
    assert result.owner.email == owner_analyst.user.email
    assert result.owner.role_name == owner_analyst.new_role.name


def test_grants_response_owner_null_on_orphan(org, owner_analyst):
    """Orphan resource (created_by=None) yields owner=null."""
    orphan = Dashboard.objects.create(title="Orphan", org=org, created_by=None)
    request = mock_request(owner_analyst)
    result = list_resource_grants(request, "dashboard", str(orphan.id))
    assert result.owner is None
    orphan.delete()


def test_grants_caller_is_owner_true_for_owner(dashboard, owner_analyst):
    request = mock_request(owner_analyst)
    result = list_resource_grants(request, "dashboard", str(dashboard.id))
    assert result.caller_is_owner is True


def test_grants_caller_is_owner_false_for_admin_not_owner(dashboard, admin):
    """Admin can view grants even without being the owner — caller_is_owner=False."""
    request = mock_request(admin)
    result = list_resource_grants(request, "dashboard", str(dashboard.id))
    assert result.caller_is_owner is False


def test_grants_403_for_view_only_holder(dashboard, member):
    """Member with floor=View cannot view the sharing list."""
    request = mock_request(member)
    with pytest.raises(HttpError) as exc:
        list_resource_grants(request, "dashboard", str(dashboard.id))
    assert exc.value.status_code == 403


def test_grants_includes_general_access_state(dashboard, owner_analyst):
    """List response embeds general_access — populates the share-modal section."""
    request = mock_request(owner_analyst)
    result = list_resource_grants(request, "dashboard", str(dashboard.id))
    assert result.general_access.mode == "internal"  # newly-created default
    assert result.general_access.supports_public is True  # dashboards support public


# ---------------------------------------------------------------------------
# POST /grants — add + auth
# ---------------------------------------------------------------------------


def test_add_user_grant_creates_share_row(dashboard, owner_analyst, member):
    """Adding a user principal at Edit creates a ResourceShare row."""
    payload = AddGrantsPayload(
        principals=[
            PrincipalGrantPayload(
                principal_type="user", principal_id=member.id, access_level="edit"
            )
        ]
    )
    add_resource_grants(mock_request(owner_analyst), "dashboard", str(dashboard.id), payload)
    row = ResourceShare.objects.filter(
        org=dashboard.org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=member.id,
    ).first()
    assert row is not None
    assert row.access_level == AccessLevel.EDIT


def test_edit_holder_can_reshare_spec_line_261(org, dashboard, other_analyst, member):
    """Spec line 261: Edit-holders can re-share (not just owner/admin).
    other_analyst has Edit via Analyst floor — should be able to add grants."""
    payload = AddGrantsPayload(
        principals=[
            PrincipalGrantPayload(
                principal_type="user", principal_id=member.id, access_level="view"
            )
        ]
    )
    add_resource_grants(mock_request(other_analyst), "dashboard", str(dashboard.id), payload)
    assert ResourceShare.objects.filter(
        principal_id=member.id, resource_id=str(dashboard.id)
    ).exists()


def test_view_holder_cannot_add_grants(dashboard, member, owner_analyst):
    """Spec line 261: View-holders cannot share. Member has floor=View."""
    payload = AddGrantsPayload(
        principals=[
            PrincipalGrantPayload(
                principal_type="user", principal_id=owner_analyst.id, access_level="view"
            )
        ]
    )
    with pytest.raises(HttpError) as exc:
        add_resource_grants(mock_request(member), "dashboard", str(dashboard.id), payload)
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /grants/{share_id} — update level + auth
# ---------------------------------------------------------------------------


def test_update_grant_changes_access_level(org, dashboard, owner_analyst, member):
    row = _grant(org, dashboard, member, AccessLevel.VIEW)
    update_resource_grant(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        row.id,
        UpdateGrantPayload(access_level="edit"),
    )
    row.refresh_from_db()
    assert row.access_level == AccessLevel.EDIT


def test_view_holder_cannot_update_grant(org, dashboard, member):
    """Setup — add a share row for the owner (arbitrary), then have Member try to change it."""
    other = _make_user("target@t.com", dashboard.org, MEMBER_ROLE)
    row = _grant(org, dashboard, other, AccessLevel.VIEW)
    try:
        with pytest.raises(HttpError) as exc:
            update_resource_grant(
                mock_request(member),
                "dashboard",
                str(dashboard.id),
                row.id,
                UpdateGrantPayload(access_level="edit"),
            )
        assert exc.value.status_code == 403
    finally:
        other.user.delete()


# ---------------------------------------------------------------------------
# DELETE /grants/{share_id} — remove + auth
# ---------------------------------------------------------------------------


def test_remove_grant_deletes_share_row(org, dashboard, owner_analyst, member):
    row = _grant(org, dashboard, member, AccessLevel.EDIT)
    remove_resource_grant(mock_request(owner_analyst), "dashboard", str(dashboard.id), row.id)
    assert not ResourceShare.objects.filter(id=row.id).exists()


def test_view_holder_cannot_remove_grant(org, dashboard, member):
    other = _make_user("target2@t.com", dashboard.org, MEMBER_ROLE)
    row = _grant(org, dashboard, other, AccessLevel.VIEW)
    try:
        with pytest.raises(HttpError) as exc:
            remove_resource_grant(mock_request(member), "dashboard", str(dashboard.id), row.id)
        assert exc.value.status_code == 403
    finally:
        other.user.delete()


# ---------------------------------------------------------------------------
# Story 3: Cascade — spec test-spec.md §"Story 3"
# ---------------------------------------------------------------------------


def _chart(org, owner, title="Chart"):
    return Chart.objects.create(
        title=title,
        org=org,
        created_by=owner,
        chart_type="bar",
        schema_name="s",
        table_name="t",
        computation_type="raw",
        extra_config={},
    )


def _kpi(org, owner, title="KPI"):
    metric = Metric.objects.create(
        name=f"{title}-metric",
        org=org,
        schema_name="s",
        table_name="t",
        column="c",
        aggregation="sum",
        created_by=owner,
    )
    return KPI.objects.create(
        name=title,
        org=org,
        metric=metric,
        target_value=100,
        direction="above",
        time_grain="month",
        time_dimension_column="d",
        green_threshold_pct=90,
        amber_threshold_pct=70,
        created_by=owner,
    )


def _dashboard_with_inner(org, owner, chart_id=None, kpi_id=None):
    """Dashboard whose tabs reference the given chart and/or KPI. Cascade
    materialization walks the tabs JSON, so we need a real components block."""
    components = {}
    if chart_id:
        components["c1"] = {"type": "chart", "config": {"chartId": chart_id}}
    if kpi_id:
        components["k1"] = {"type": "kpi", "config": {"kpiId": kpi_id}}
    tabs = [{"id": "t1", "components": components}] if components else []
    return Dashboard.objects.create(title="D", org=org, created_by=owner, tabs=tabs)


def _share_dashboard(caller, dashboard, target, level="edit"):
    add_resource_grants(
        mock_request(caller),
        "dashboard",
        str(dashboard.id),
        AddGrantsPayload(
            principals=[
                PrincipalGrantPayload(
                    principal_type="user", principal_id=target.id, access_level=level
                )
            ]
        ),
    )


# ---- Materialization --------------------------------------------


def test_dashboard_share_materializes_cascade_rows(org, owner_analyst, member):
    """Sharing a dashboard creates cascade rows for its inner chart + KPI."""
    chart = _chart(org, owner_analyst)
    kpi = _kpi(org, owner_analyst)
    metric = kpi.metric
    try:
        d = _dashboard_with_inner(org, owner_analyst, chart.id, kpi.id)
        _share_dashboard(owner_analyst, d, member, "edit")

        child_chart = ResourceShare.objects.filter(
            resource_type=ResourceType.CHART, resource_id=str(chart.id), principal_id=member.id
        ).first()
        child_kpi = ResourceShare.objects.filter(
            resource_type=ResourceType.KPI, resource_id=str(kpi.id), principal_id=member.id
        ).first()
        assert child_chart is not None and child_chart.parent_id is not None
        assert child_chart.access_level == AccessLevel.EDIT
        assert child_kpi is not None and child_kpi.access_level == AccessLevel.EDIT
    finally:
        kpi.delete()
        metric.delete()


def test_dashboard_share_level_update_propagates_to_children(org, owner_analyst, member):
    """PATCH dashboard share level → all cascade children updated."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    parent = ResourceShare.objects.get(
        resource_type=ResourceType.DASHBOARD, resource_id=str(d.id), principal_id=member.id
    )
    update_resource_grant(
        mock_request(owner_analyst),
        "dashboard",
        str(d.id),
        parent.id,
        UpdateGrantPayload(access_level="edit"),
    )
    child = ResourceShare.objects.get(
        resource_type=ResourceType.CHART, resource_id=str(chart.id), principal_id=member.id
    )
    assert child.access_level == AccessLevel.EDIT


def test_deleting_parent_share_deletes_cascade_children(org, owner_analyst, member):
    """ON DELETE CASCADE on ``parent`` FK: removing the dashboard share
    removes all its cascade rows."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "edit")
    parent = ResourceShare.objects.get(
        resource_type=ResourceType.DASHBOARD, resource_id=str(d.id), principal_id=member.id
    )
    remove_resource_grant(mock_request(owner_analyst), "dashboard", str(d.id), parent.id)
    assert not ResourceShare.objects.filter(
        resource_type=ResourceType.CHART, resource_id=str(chart.id), principal_id=member.id
    ).exists()


def test_chart_removed_from_tabs_deletes_cascade_row(org, owner_analyst, member):
    """Re-syncing after removing a chart from the dashboard drops its cascade rows."""
    chart_a = _chart(org, owner_analyst, "A")
    chart_b = _chart(org, owner_analyst, "B")
    d = Dashboard.objects.create(
        title="D",
        org=org,
        created_by=owner_analyst,
        tabs=[
            {
                "id": "t1",
                "components": {
                    "c1": {"type": "chart", "config": {"chartId": chart_a.id}},
                    "c2": {"type": "chart", "config": {"chartId": chart_b.id}},
                },
            }
        ],
    )
    _share_dashboard(owner_analyst, d, member, "view")
    # Remove chart_b from tabs then re-sync.
    d.tabs = [
        {"id": "t1", "components": {"c1": {"type": "chart", "config": {"chartId": chart_a.id}}}}
    ]
    d.save(update_fields=["tabs"])
    sync_dashboard_cascade(d)

    assert not ResourceShare.objects.filter(
        resource_type=ResourceType.CHART, resource_id=str(chart_b.id), principal_id=member.id
    ).exists()
    assert ResourceShare.objects.filter(
        resource_type=ResourceType.CHART, resource_id=str(chart_a.id), principal_id=member.id
    ).exists()


def test_direct_grant_survives_when_chart_removed_from_dashboard(org, owner_analyst, member):
    """Cascade re-sync only touches rows with parent set — direct grants on
    the chart are unaffected."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)

    ResourceShare.objects.create(
        org=org,
        resource_type=ResourceType.CHART,
        resource_id=str(chart.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=member.id,
        access_level=AccessLevel.EDIT,
    )
    _share_dashboard(owner_analyst, d, member, "view")

    d.tabs = []
    d.save(update_fields=["tabs"])
    sync_dashboard_cascade(d)

    direct = ResourceShare.objects.filter(
        resource_type=ResourceType.CHART,
        resource_id=str(chart.id),
        principal_id=member.id,
        parent__isnull=True,
    ).first()
    assert direct is not None
    assert direct.access_level == AccessLevel.EDIT


# ---- Enforcement ---------------------------------


def test_dashboard_edit_share_gives_edit_on_inner_chart(org, owner_analyst, member):
    """Cascade Edit → chart's effective access = Edit."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "edit")
    assert get_user_access(member, "chart", chart.id) == AccessLevel.EDIT


def test_dashboard_view_share_gives_view_on_inner_chart(org, owner_analyst, member):
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    assert get_user_access(member, "chart", chart.id) == AccessLevel.VIEW


def test_no_access_floor_plus_cascade_visible(org, owner_analyst, member):
    """Member with no-access floor still sees cascade-shared chart."""
    OrgPreferences.objects.create(
        org=org,
        default_member_level=AccessLevel.NO_ACCESS,
        default_analyst_level=AccessLevel.EDIT,
    )
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    assert get_user_access(member, "chart", chart.id) == AccessLevel.VIEW


def test_deleting_dashboard_share_removes_chart_access(org, owner_analyst, member):
    """After deleting the parent dashboard share, the chart is inaccessible
    (no floor, no direct grant)."""
    OrgPreferences.objects.create(
        org=org,
        default_member_level=AccessLevel.NO_ACCESS,
        default_analyst_level=AccessLevel.EDIT,
    )
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "edit")
    parent = ResourceShare.objects.get(
        resource_type=ResourceType.DASHBOARD, resource_id=str(d.id), principal_id=member.id
    )
    remove_resource_grant(mock_request(owner_analyst), "dashboard", str(d.id), parent.id)
    assert get_user_access(member, "chart", chart.id) == AccessLevel.NO_ACCESS


def test_kpi_cascade_same_as_chart(org, owner_analyst, member):
    """KPI cascade rule is identical to chart cascade."""
    kpi = _kpi(org, owner_analyst)
    metric = kpi.metric
    try:
        d = _dashboard_with_inner(org, owner_analyst, kpi_id=kpi.id)
        _share_dashboard(owner_analyst, d, member, "edit")
        assert get_user_access(member, "kpi", kpi.id) == AccessLevel.EDIT
    finally:
        kpi.delete()
        metric.delete()


# ---- Read-layer ------------------------------------------------------


def test_cascade_only_row_shows_share_id_null_and_source(org, owner_analyst, member):
    """User with cascade-only access → share_id=None, cascade_sources populated."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    result = list_resource_grants(mock_request(owner_analyst), "chart", str(chart.id))
    row = next(s for s in result.shares if s.principal_id == member.id)
    assert row.share_id is None
    assert len(row.cascade_sources) == 1
    assert row.cascade_sources[0].dashboard_id == d.id


# ---- Multi-dashboard (Story 4) ---------------------------------------------


def test_chart_in_two_dashboards_effective_access_is_max(org, owner_analyst, member):
    """Chart in Dashboard A (Edit) + Dashboard B (View) → effective Edit (max)."""
    chart = _chart(org, owner_analyst)
    d_a = _dashboard_with_inner(org, owner_analyst, chart.id)
    d_b = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d_a, member, "edit")
    _share_dashboard(owner_analyst, d_b, member, "view")
    assert get_user_access(member, "chart", chart.id) == AccessLevel.EDIT


def test_chart_in_two_dashboards_survives_one_share_deletion(org, owner_analyst, member):
    """Delete Dashboard A's Edit share → chart still accessible via Dashboard B's View."""
    OrgPreferences.objects.create(
        org=org,
        default_member_level=AccessLevel.NO_ACCESS,
        default_analyst_level=AccessLevel.EDIT,
    )
    chart = _chart(org, owner_analyst)
    d_a = _dashboard_with_inner(org, owner_analyst, chart.id)
    d_b = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d_a, member, "edit")
    _share_dashboard(owner_analyst, d_b, member, "view")
    parent_a = ResourceShare.objects.get(
        resource_type=ResourceType.DASHBOARD, resource_id=str(d_a.id), principal_id=member.id
    )
    remove_resource_grant(mock_request(owner_analyst), "dashboard", str(d_a.id), parent_a.id)
    assert get_user_access(member, "chart", chart.id) == AccessLevel.VIEW


# ---- Cascade rows are read-only --------------------------------------


def test_patch_directly_on_cascade_row_rejected(org, owner_analyst, member):
    """Spec: cascade rows can't be updated directly — must change parent share."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    cascade_row = ResourceShare.objects.get(
        resource_type=ResourceType.CHART, resource_id=str(chart.id), principal_id=member.id
    )
    with pytest.raises(HttpError) as exc:
        update_resource_grant(
            mock_request(owner_analyst),
            "chart",
            str(chart.id),
            cascade_row.id,
            UpdateGrantPayload(access_level="edit"),
        )
    assert exc.value.status_code == 400
    assert "cascade" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Story 14: Orphan cleanup on resource / group delete — spec test-spec.md §"Story 14"
# ---------------------------------------------------------------------------


def _create_share(org, rtype, resource_id, target):
    return ResourceShare.objects.create(
        org=org,
        resource_type=rtype,
        resource_id=str(resource_id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=target.id,
        access_level=AccessLevel.VIEW,
    )


def _create_request(org, rtype, resource_id, requester):
    return AccessRequest.objects.create(
        org=org,
        resource_type=rtype,
        resource_id=str(resource_id),
        requester=requester,
        requested_level=AccessLevel.VIEW,
        status=AccessRequestStatus.PENDING,
    )


def test_dashboard_delete_removes_all_grants_and_requests(org, owner_analyst, member):
    """Spec: on dashboard delete, ResourceShare + AccessRequest rows are cleaned."""
    from ddpui.api.dashboard_native_api import delete_dashboard

    Dashboard.objects.create(
        title="Keep", org=org, created_by=owner_analyst
    )  # last-dashboard guard
    d = Dashboard.objects.create(title="Doomed", org=org, created_by=owner_analyst)
    _create_share(org, ResourceType.DASHBOARD, d.id, member)
    _create_request(org, ResourceType.DASHBOARD, d.id, member)

    delete_dashboard(mock_request(owner_analyst), dashboard_id=d.id)

    assert not ResourceShare.objects.filter(
        org=org, resource_type=ResourceType.DASHBOARD, resource_id=str(d.id)
    ).exists()
    assert not AccessRequest.objects.filter(
        org=org, resource_type=ResourceType.DASHBOARD, resource_id=str(d.id)
    ).exists()


def test_chart_delete_removes_all_grants(org, owner_analyst, member):
    """Spec: chart delete → ResourceShare + AccessRequest cleaned."""
    from ddpui.api.charts_api import delete_chart

    chart = _chart(org, owner_analyst)
    _create_share(org, ResourceType.CHART, chart.id, member)
    _create_request(org, ResourceType.CHART, chart.id, member)

    delete_chart(mock_request(owner_analyst), chart_id=chart.id)

    assert not ResourceShare.objects.filter(
        org=org, resource_type=ResourceType.CHART, resource_id=str(chart.id)
    ).exists()
    assert not AccessRequest.objects.filter(
        org=org, resource_type=ResourceType.CHART, resource_id=str(chart.id)
    ).exists()


def test_report_delete_removes_all_grants_and_requests(org, owner_analyst, member):
    """Spec: report delete → ResourceShare + AccessRequest cleaned."""
    from ddpui.api.report_api import delete_snapshot

    snap = ReportSnapshot.objects.create(
        title="Snap",
        org=org,
        created_by=owner_analyst,
        period_end="2025-01-01",
    )
    _create_share(org, ResourceType.REPORT, snap.id, member)
    _create_request(org, ResourceType.REPORT, snap.id, member)

    delete_snapshot(mock_request(owner_analyst), snapshot_id=snap.id)

    assert not ResourceShare.objects.filter(
        org=org, resource_type=ResourceType.REPORT, resource_id=str(snap.id)
    ).exists()
    assert not AccessRequest.objects.filter(
        org=org, resource_type=ResourceType.REPORT, resource_id=str(snap.id)
    ).exists()


def test_kpi_delete_removes_all_grants(org, owner_analyst, member):
    """Spec: KPI delete → ResourceShare + AccessRequest cleaned."""
    from ddpui.api.kpi_api import delete_kpi

    kpi = _kpi(org, owner_analyst)
    metric = kpi.metric
    try:
        _create_share(org, ResourceType.KPI, kpi.id, member)
        _create_request(org, ResourceType.KPI, kpi.id, member)

        delete_kpi(mock_request(owner_analyst), kpi_id=kpi.id)

        assert not ResourceShare.objects.filter(
            org=org, resource_type=ResourceType.KPI, resource_id=str(kpi.id)
        ).exists()
        assert not AccessRequest.objects.filter(
            org=org, resource_type=ResourceType.KPI, resource_id=str(kpi.id)
        ).exists()
    finally:
        # KPI already deleted by the endpoint; Metric still needs cleanup so
        # its Org can be torn down.
        metric.delete()


def test_group_delete_removes_group_share_rows(org, owner_analyst, member):
    """Spec: group delete → any ResourceShare rows keyed on that group are removed."""
    from ddpui.models.org_user import OrgUserGroup
    from ddpui.api.user_org_api import delete_user_group

    d = Dashboard.objects.create(title="D-for-group", org=org, created_by=owner_analyst)
    group = OrgUserGroup.objects.create(org=org, name="Field Staff", created_by=owner_analyst)
    ResourceShare.objects.create(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(d.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
        access_level=AccessLevel.VIEW,
    )
    delete_user_group(mock_request(owner_analyst), group_id=group.id)

    assert not ResourceShare.objects.filter(
        principal_type=ResourceSharePrincipalType.GROUP, principal_id=group.id
    ).exists()
    d.delete()


def test_group_member_removed_loses_group_access(org, owner_analyst, member):
    """Spec: removing a user from a group revokes their group-derived access
    on the next call to ``get_user_access``."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember
    from ddpui.api.user_org_api import remove_user_group_member

    # Force no-access floor so any effective access must come from the group.
    OrgPreferences.objects.create(
        org=org,
        default_member_level=AccessLevel.NO_ACCESS,
        default_analyst_level=AccessLevel.NO_ACCESS,
    )
    d = Dashboard.objects.create(title="D-for-group-member", org=org, created_by=owner_analyst)
    group = OrgUserGroup.objects.create(org=org, name="Team", created_by=owner_analyst)
    membership = OrgUserGroupMember.objects.create(group=group, orguser=member)
    ResourceShare.objects.create(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(d.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
        access_level=AccessLevel.EDIT,
    )
    # sanity: access derived from group
    assert get_user_access(member, "dashboard", d.id) == AccessLevel.EDIT

    remove_user_group_member(
        mock_request(owner_analyst), group_id=group.id, member_id=membership.id
    )

    # After removal, no group access + floor is no-access → no_access
    assert get_user_access(member, "dashboard", d.id) == AccessLevel.NO_ACCESS
    d.delete()


# ---------------------------------------------------------------------------
# Story 9: Request access — spec test-spec.md §"Story 9"
# ---------------------------------------------------------------------------


@pytest.fixture
def no_access_member(org, seed_db):
    """A Member with No-Access floor so they can request access to a resource."""
    OrgPreferences.objects.filter(org=org).delete()
    OrgPreferences.objects.create(
        org=org,
        default_member_level=AccessLevel.NO_ACCESS,
        default_analyst_level=AccessLevel.EDIT,
    )
    ou = _make_user("noaccess@t.com", org, MEMBER_ROLE)
    yield ou
    ou.user.delete()


# ---- Create ---------------------------------------------------------------


def test_no_access_user_can_request_view(dashboard, no_access_member):
    """Member with no current access can create a View request."""
    result = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view", note="please"),
    )
    assert result.requested_level == "view"
    assert result.status == AccessRequestStatus.PENDING


def test_view_holder_can_request_edit_upgrade(dashboard, member):
    """View-holder (floor=view) can request an Edit upgrade → 201."""
    result = create_access_request(
        mock_request(member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="edit"),
    )
    assert result.requested_level == "edit"
    assert result.status == AccessRequestStatus.PENDING


def test_edit_holder_cannot_request_lower_or_equal(dashboard, other_analyst):
    """Edit-holder requesting view (downgrade) or edit (same) → 409."""
    with pytest.raises(HttpError) as exc:
        create_access_request(
            mock_request(other_analyst),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="view"),
        )
    assert exc.value.status_code == 409


def test_view_holder_requesting_view_rejected(dashboard, member):
    """View-holder requesting View (same level) → 409."""
    with pytest.raises(HttpError) as exc:
        create_access_request(
            mock_request(member),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="view"),
        )
    assert exc.value.status_code == 409


def test_duplicate_pending_request_rejected(dashboard, no_access_member):
    """A second pending request for the same resource + user is rejected (409)."""
    create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    with pytest.raises(HttpError) as exc:
        create_access_request(
            mock_request(no_access_member),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="edit"),
        )
    assert exc.value.status_code == 409


def test_request_on_missing_resource_returns_404(no_access_member):
    """Spec: nonexistent resource → 404 (frontend never shows the request screen)."""
    with pytest.raises(HttpError) as exc:
        create_access_request(
            mock_request(no_access_member),
            "dashboard",
            "999999",
            RequestAccessPayload(requested_level="view"),
        )
    assert exc.value.status_code == 404


# ---- List -----------------------------------------------------------------


def test_owner_lists_pending_requests(dashboard, owner_analyst, no_access_member):
    """Owner sees the pending request."""
    create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    result = list_access_requests(mock_request(owner_analyst), "dashboard", str(dashboard.id))
    assert len(result) == 1
    assert result[0].requester_id == no_access_member.id


def test_view_only_holder_cannot_list_requests(dashboard, member):
    """Member with floor=View cannot view pending requests → 403."""
    with pytest.raises(HttpError) as exc:
        list_access_requests(mock_request(member), "dashboard", str(dashboard.id))
    assert exc.value.status_code == 403


def test_empty_list_when_no_pending(dashboard, owner_analyst):
    result = list_access_requests(mock_request(owner_analyst), "dashboard", str(dashboard.id))
    assert result == []


# ---- Respond --------------------------------------------------------------


def test_approve_creates_grant(dashboard, owner_analyst, no_access_member):
    """Approve → direct share row created at requested level; request marked approved."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="edit"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="approved"),
    )
    share = ResourceShare.objects.filter(
        org=dashboard.org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_id=no_access_member.id,
    ).first()
    assert share is not None
    assert share.access_level == AccessLevel.EDIT
    updated = AccessRequest.objects.get(id=req.id)
    assert updated.status == AccessRequestStatus.APPROVED


def test_approve_can_downgrade_to_view(dashboard, owner_analyst, no_access_member):
    """Owner can approve at a lower level than requested."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="edit"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="approved", granted_level="view"),
    )
    share = ResourceShare.objects.get(
        principal_id=no_access_member.id, resource_id=str(dashboard.id)
    )
    assert share.access_level == AccessLevel.VIEW


def test_decline_creates_no_grant(dashboard, owner_analyst, no_access_member):
    """Decline → no grant; request marked declined."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="declined"),
    )
    assert not ResourceShare.objects.filter(
        principal_id=no_access_member.id, resource_id=str(dashboard.id)
    ).exists()
    updated = AccessRequest.objects.get(id=req.id)
    assert updated.status == AccessRequestStatus.DECLINED


def test_view_holder_cannot_respond(dashboard, owner_analyst, no_access_member, member):
    """Only Edit-holders / owner / admin can respond."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    with pytest.raises(HttpError) as exc:
        respond_to_access_request(
            mock_request(member),
            "dashboard",
            str(dashboard.id),
            req.id,
            RespondToRequestPayload(decision="approved"),
        )
    assert exc.value.status_code == 403


def test_respond_to_nonexistent_request(dashboard, owner_analyst):
    with pytest.raises(HttpError) as exc:
        respond_to_access_request(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            999999,
            RespondToRequestPayload(decision="approved"),
        )
    assert exc.value.status_code == 404


def test_respond_to_already_decided_request(dashboard, owner_analyst, no_access_member):
    """Cannot re-respond to an already-decided request."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="approved"),
    )
    with pytest.raises(HttpError) as exc:
        respond_to_access_request(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            req.id,
            RespondToRequestPayload(decision="declined"),
        )
    assert exc.value.status_code == 409


def test_approve_upgrade_merges_into_existing_direct_share(
    org, dashboard, owner_analyst, no_access_member
):
    """View-holder with an existing direct share → approve Edit upgrade →
    the same row is bumped to Edit; no duplicate row is created."""
    existing = _grant(org, dashboard, no_access_member, AccessLevel.VIEW)
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="edit"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="approved"),
    )

    direct_rows = ResourceShare.objects.filter(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=no_access_member.id,
        parent__isnull=True,
    )
    assert direct_rows.count() == 1
    updated = direct_rows.first()
    assert updated.id == existing.id
    assert updated.access_level == AccessLevel.EDIT


def test_approve_upgrade_via_group_creates_new_direct_row(
    org, dashboard, owner_analyst, no_access_member
):
    """View came from a group grant only (no direct row) → approve Edit →
    new direct Edit row for the user; group grant is untouched."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember

    group = OrgUserGroup.objects.create(org=org, name="Group", created_by=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=no_access_member)
    group_share = ResourceShare.objects.create(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
        access_level=AccessLevel.VIEW,
    )

    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="edit"),
    )
    respond_to_access_request(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        req.id,
        RespondToRequestPayload(decision="approved"),
    )

    # New direct Edit row for the user
    direct_row = ResourceShare.objects.get(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=no_access_member.id,
        parent__isnull=True,
    )
    assert direct_row.access_level == AccessLevel.EDIT

    # Group grant untouched
    group_share.refresh_from_db()
    assert group_share.access_level == AccessLevel.VIEW


# ---- Notifications --------------------------------------------------------


def _patch_notification():
    """Patch ``create_notification`` in both notification-trigger modules used by
    ``access_api`` (access-request + share-grant paths). Returns a single MagicMock
    whose ``call_args`` / ``called`` / ``side_effect`` cover both — since a given
    test only fires one path, the aggregation is unambiguous."""
    from contextlib import contextmanager
    from unittest.mock import MagicMock, patch

    @contextmanager
    def _both():
        combined = MagicMock()
        with patch(
            "ddpui.core.notifications.triggers.access.create_notification",
            side_effect=combined,
        ), patch(
            "ddpui.core.notifications.triggers.share.create_notification",
            side_effect=combined,
        ):
            yield combined

    return _both()


def test_owner_notified_on_new_request(dashboard, owner_analyst, no_access_member):
    """Owner is always in the recipient set."""
    with _patch_notification() as mock_notify:
        create_access_request(
            mock_request(no_access_member),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="view", note="please"),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert owner_analyst.id in payload.recipients


def test_admins_also_notified_on_new_request(dashboard, owner_analyst, no_access_member, admin):
    """Org admins are included alongside the owner as governance backup."""
    with _patch_notification() as mock_notify:
        create_access_request(
            mock_request(no_access_member),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="view"),
        )
        payload = mock_notify.call_args[0][0]
        assert admin.id in payload.recipients
        assert owner_analyst.id in payload.recipients


def test_requester_notified_on_approve(dashboard, owner_analyst, no_access_member):
    """Approve → requester gets a notification."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    with _patch_notification() as mock_notify:
        respond_to_access_request(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            req.id,
            RespondToRequestPayload(decision="approved"),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert no_access_member.id in payload.recipients
        assert "approved" in payload.message.lower()


def test_requester_notified_on_decline(dashboard, owner_analyst, no_access_member):
    """Decline → requester gets a notification saying declined."""
    req = create_access_request(
        mock_request(no_access_member),
        "dashboard",
        str(dashboard.id),
        RequestAccessPayload(requested_level="view"),
    )
    with _patch_notification() as mock_notify:
        respond_to_access_request(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            req.id,
            RespondToRequestPayload(decision="declined"),
        )
        assert mock_notify.called
        message = mock_notify.call_args[0][0].message.lower()
        assert "declined" in message
        assert "view" in message  # requested level named in decline body


def test_orphan_resource_still_notifies_admins(org, no_access_member, admin):
    """On an orphan resource (created_by=None), the owner leg is skipped, but
    admins still receive the notification — governance backup path."""
    orphan = Dashboard.objects.create(title="Orphan", org=org, created_by=None)
    try:
        with _patch_notification() as mock_notify:
            create_access_request(
                mock_request(no_access_member),
                "dashboard",
                str(orphan.id),
                RequestAccessPayload(requested_level="view"),
            )
            assert mock_notify.called
            payload = mock_notify.call_args[0][0]
            assert admin.id in payload.recipients
        assert AccessRequest.objects.filter(
            resource_id=str(orphan.id), requester=no_access_member
        ).exists()
    finally:
        orphan.delete()


def test_orphan_resource_with_no_admins_is_silent(org, no_access_member):
    """No owner + no admins → nothing to notify (silent no-op), but the request
    row still lands."""
    orphan = Dashboard.objects.create(title="Orphan", org=org, created_by=None)
    try:
        with _patch_notification() as mock_notify:
            create_access_request(
                mock_request(no_access_member),
                "dashboard",
                str(orphan.id),
                RequestAccessPayload(requested_level="view"),
            )
            assert not mock_notify.called
        assert AccessRequest.objects.filter(
            resource_id=str(orphan.id), requester=no_access_member
        ).exists()
    finally:
        orphan.delete()


def test_notification_failure_does_not_fail_api_call(dashboard, owner_analyst, no_access_member):
    """Spec: `create_notification` failure must not fail the endpoint."""
    with _patch_notification() as mock_notify:
        mock_notify.side_effect = Exception("delivery broken")
        # Should NOT raise — the endpoint swallows notification errors.
        result = create_access_request(
            mock_request(no_access_member),
            "dashboard",
            str(dashboard.id),
            RequestAccessPayload(requested_level="view"),
        )
        assert result.status == AccessRequestStatus.PENDING
    # AccessRequest still landed.
    assert AccessRequest.objects.filter(
        resource_id=str(dashboard.id), requester=no_access_member
    ).exists()


# ---------------------------------------------------------------------------
# Share notifications on direct grants
# spec test-spec.md §"Story 9 · notifications on Share" — see triggers/share.notify_share_recipients
# ---------------------------------------------------------------------------


def _add_grants_payload(principals=None, pending_grants=None, invite_role_uuid=None):
    return AddGrantsPayload(
        principals=principals or [],
        pending_grants=pending_grants or [],
        invite_role_uuid=invite_role_uuid,
    )


def test_direct_user_grant_fires_share_notification(dashboard, owner_analyst, member):
    """Adding a new user chip at View → notification to that user."""
    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=member.id,
                        access_level="view",
                    )
                ],
            ),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert list(payload.recipients) == [member.id]
        assert "shared" in payload.message.lower()
        assert "view" in payload.message.lower()


def test_group_grant_notifies_every_current_member(
    org, dashboard, owner_analyst, member, no_access_member
):
    """Group grant expands to every current OrgUserGroupMember with an orguser."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember

    group = OrgUserGroup.objects.create(org=org, name="Team", created_by=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=member)
    OrgUserGroupMember.objects.create(group=group, orguser=no_access_member)

    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="group",
                        principal_id=group.id,
                        access_level="view",
                    )
                ],
            ),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        recipients = set(payload.recipients)
        assert member.id in recipients
        assert no_access_member.id in recipients


def test_view_to_edit_upgrade_fires_upgrade_notification(org, dashboard, owner_analyst, member):
    """Pre-existing direct View → re-share at Edit → 'upgraded' notification."""
    _grant(org, dashboard, member, AccessLevel.VIEW)
    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=member.id,
                        access_level="edit",
                    )
                ],
            ),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert list(payload.recipients) == [member.id]
        assert "upgraded" in payload.message.lower()


def test_noop_re_save_does_not_notify(org, dashboard, owner_analyst, member):
    """Same-level re-save fires no notification."""
    _grant(org, dashboard, member, AccessLevel.VIEW)
    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=member.id,
                        access_level="view",
                    )
                ],
            ),
        )
        assert not mock_notify.called


def test_downgrade_does_not_notify(org, dashboard, owner_analyst, member):
    """Edit → View downgrade fires no notification."""
    _grant(org, dashboard, member, AccessLevel.EDIT)
    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=member.id,
                        access_level="view",
                    )
                ],
            ),
        )
        assert not mock_notify.called


def test_invitation_grants_do_not_fire_share_notification(org, dashboard, owner_analyst, seed_db):
    """Pending-email invitation rows go through the platform invite-email flow,
    not the share-notification path."""
    from ddpui.schemas.access.resource_share_schema import PendingGrantPayload

    member_role = Role.objects.filter(slug=MEMBER_ROLE).first()
    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                pending_grants=[PendingGrantPayload(email="invitee@t.com", access_level="view")],
                invite_role_uuid=str(member_role.uuid),
            ),
        )
        assert not mock_notify.called


def test_pending_email_belonging_to_existing_dalgo_user_creates_direct_share(
    org, dashboard, owner_analyst, seed_db
):
    """Regression: when the pending email is an existing Dalgo user (with a
    User row) but not in THIS org, invite_user_v1 creates an OrgUser directly
    instead of an Invitation. The share must land as a direct-user grant, not
    an invitation-pending row — and the endpoint must NOT raise 'could not
    resolve invitation'."""
    from ddpui.schemas.access.resource_share_schema import PendingGrantPayload
    from ddpui.models.resource_share import ResourceShare, ResourceSharePrincipalType
    from django.contrib.auth.models import User

    # An existing Dalgo user in a DIFFERENT org (has a User row, no OrgUser here).
    other_org = Org.objects.create(slug="other-org")
    User.objects.create(username="existing@dalgo.test", email="existing@dalgo.test", password="pw")
    member_role = Role.objects.filter(slug=MEMBER_ROLE).first()

    try:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                pending_grants=[
                    PendingGrantPayload(email="existing@dalgo.test", access_level="view")
                ],
                invite_role_uuid=str(member_role.uuid),
            ),
        )
    finally:
        other_org.delete()

    # A direct-user share must exist for the newly-created OrgUser.
    orguser = OrgUser.objects.get(org=org, user__email="existing@dalgo.test")
    share = ResourceShare.objects.filter(
        org=org,
        resource_type="dashboard",
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.USER,
        principal_id=orguser.id,
    ).first()
    assert share is not None
    assert share.access_level == AccessLevel.VIEW
    # And NOT an invitation-linked row for that email.
    assert not ResourceShare.objects.filter(
        org=org,
        resource_type="dashboard",
        resource_id=str(dashboard.id),
        invitation__invited_email__iexact="existing@dalgo.test",
    ).exists()


def test_dedup_when_user_is_direct_and_group_grantee(org, dashboard, owner_analyst, member):
    """User granted directly + present in a granted group at the same level → one notification, once."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember

    group = OrgUserGroup.objects.create(org=org, name="Team", created_by=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=member)

    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=member.id,
                        access_level="view",
                    ),
                    PrincipalGrantPayload(
                        principal_type="group",
                        principal_id=group.id,
                        access_level="view",
                    ),
                ],
            ),
        )
        # Same (class, level) bucket → one create_notification call, recipient once.
        assert mock_notify.call_count == 1
        payload = mock_notify.call_args[0][0]
        assert payload.recipients.count(member.id) == 1


def test_sender_not_own_share_notification_recipient(org, dashboard, owner_analyst, member):
    """Sender is a member of a granted group → filtered out of the recipient list."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember

    group = OrgUserGroup.objects.create(org=org, name="Team", created_by=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=member)
    OrgUserGroupMember.objects.create(group=group, orguser=owner_analyst)

    with _patch_notification() as mock_notify:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            _add_grants_payload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="group",
                        principal_id=group.id,
                        access_level="view",
                    )
                ],
            ),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert owner_analyst.id not in payload.recipients
        assert member.id in payload.recipients


def test_row_update_view_to_edit_fires_upgrade_notification(org, dashboard, owner_analyst, member):
    """PATCH /grants/{share_id} that raises the level → 'upgraded' notification."""
    row = _grant(org, dashboard, member, AccessLevel.VIEW)
    with _patch_notification() as mock_notify:
        update_resource_grant(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            row.id,
            UpdateGrantPayload(access_level="edit"),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert member.id in payload.recipients
        assert "upgraded" in payload.message.lower()
        assert "edit" in payload.message.lower()


def test_row_update_edit_to_view_fires_downgrade_notification(
    org, dashboard, owner_analyst, member
):
    """Row-level downgrade also notifies (deliberate divergence from bulk-add)."""
    row = _grant(org, dashboard, member, AccessLevel.EDIT)
    with _patch_notification() as mock_notify:
        update_resource_grant(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            row.id,
            UpdateGrantPayload(access_level="view"),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert member.id in payload.recipients
        assert "downgraded" in payload.message.lower()
        assert "view" in payload.message.lower()


def test_row_update_same_level_is_silent(org, dashboard, owner_analyst, member):
    """No-op saves (level unchanged) don't notify."""
    row = _grant(org, dashboard, member, AccessLevel.VIEW)
    with _patch_notification() as mock_notify:
        update_resource_grant(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            row.id,
            UpdateGrantPayload(access_level="view"),
        )
        assert not mock_notify.called


def test_row_update_group_upgrade_fans_out_to_members(org, dashboard, owner_analyst, member):
    """Group-share upgrade via PATCH → every current group member gets notified."""
    from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember
    from ddpui.models.resource_share import ResourceSharePrincipalType

    group = OrgUserGroup.objects.create(org=org, name="Team-Upgrade", created_by=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=member)
    row = ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
        access_level=AccessLevel.VIEW,
    )
    with _patch_notification() as mock_notify:
        update_resource_grant(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            row.id,
            UpdateGrantPayload(access_level="edit"),
        )
        assert mock_notify.called
        payload = mock_notify.call_args[0][0]
        assert member.id in payload.recipients


# ---------------------------------------------------------------------------
# Story 6: Private toggle — enforcement
# spec test-spec.md §"Story 6" + spec.md §"Private toggle" (floor bypass)
# ---------------------------------------------------------------------------


@pytest.fixture
def private_dashboard(org, owner_analyst):
    d = Dashboard.objects.create(
        title="Private", org=org, created_by=owner_analyst, is_private=True
    )
    yield d
    d.delete()


def test_private_plus_view_floor_no_access(private_dashboard, other_analyst):
    """Private + Analyst floor=View → floor bypassed → no_access.
    (Default Analyst floor is Edit, so force View here for the test.)"""
    OrgPreferences.objects.filter(org=private_dashboard.org).delete()
    OrgPreferences.objects.create(
        org=private_dashboard.org,
        default_analyst_level=AccessLevel.VIEW,
        default_member_level=AccessLevel.VIEW,
    )
    assert (
        get_user_access(other_analyst, "dashboard", private_dashboard.id) == AccessLevel.NO_ACCESS
    )


def test_private_plus_edit_floor_no_access(private_dashboard, other_analyst):
    """Private + Analyst floor=Edit → floor still bypassed → no_access."""
    assert (
        get_user_access(other_analyst, "dashboard", private_dashboard.id) == AccessLevel.NO_ACCESS
    )


def test_private_plus_direct_edit_grant_gives_edit(org, private_dashboard, member):
    """Private + explicit user grant → grant applies (Edit)."""
    _grant(org, private_dashboard, member, AccessLevel.EDIT)
    assert get_user_access(member, "dashboard", private_dashboard.id) == AccessLevel.EDIT


def test_private_plus_direct_view_grant_gives_view(org, private_dashboard, member):
    """Private + explicit user grant → View grant applies as View."""
    _grant(org, private_dashboard, member, AccessLevel.VIEW)
    assert get_user_access(member, "dashboard", private_dashboard.id) == AccessLevel.VIEW


def test_private_plus_cascade_edit_grant_gives_edit(org, owner_analyst, member):
    """Private inner chart + parent dashboard shared at Edit → cascade Edit
    survives the private toggle on the child chart."""
    chart = _chart(org, owner_analyst)
    chart.is_private = True
    chart.save(update_fields=["is_private"])
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "edit")
    assert get_user_access(member, "chart", chart.id) == AccessLevel.EDIT


def test_private_plus_owner_edit(private_dashboard, owner_analyst):
    """Owner always sees their own private resource at Edit."""
    assert get_user_access(owner_analyst, "dashboard", private_dashboard.id) == AccessLevel.EDIT


def test_private_plus_admin_edit(private_dashboard, admin):
    """Admin always sees any private resource at Edit."""
    assert get_user_access(admin, "dashboard", private_dashboard.id) == AccessLevel.EDIT


def test_accessible_filter_excludes_private_when_only_floor(private_dashboard, other_analyst):
    """accessible_filter on the list endpoint: floor-only access excludes
    private resources — they don't appear in the list."""
    OrgPreferences.objects.filter(org=private_dashboard.org).delete()
    OrgPreferences.objects.create(
        org=private_dashboard.org,
        default_analyst_level=AccessLevel.VIEW,
        default_member_level=AccessLevel.VIEW,
    )
    q = accessible_filter(other_analyst, "dashboard")
    ids = list(
        Dashboard.objects.filter(org=private_dashboard.org).filter(q).values_list("id", flat=True)
    )
    assert private_dashboard.id not in ids


def test_accessible_filter_includes_private_with_direct_grant(org, private_dashboard, member):
    """accessible_filter: private + explicit grantee → included."""
    _grant(org, private_dashboard, member, AccessLevel.VIEW)
    q = accessible_filter(member, "dashboard")
    ids = list(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
    assert private_dashboard.id in ids


def test_accessible_filter_includes_private_via_cascade(org, owner_analyst, member):
    """accessible_filter for charts: private chart + cascade grant → included."""
    chart = _chart(org, owner_analyst)
    chart.is_private = True
    chart.save(update_fields=["is_private"])
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, member, "view")
    q = accessible_filter(member, "chart")
    ids = list(Chart.objects.filter(org=org).filter(q).values_list("id", flat=True))
    assert chart.id in ids


def test_accessible_filter_includes_private_when_owner(private_dashboard, owner_analyst):
    """accessible_filter: owner sees their own private resource in the list."""
    q = accessible_filter(owner_analyst, "dashboard")
    ids = list(
        Dashboard.objects.filter(org=private_dashboard.org).filter(q).values_list("id", flat=True)
    )
    assert private_dashboard.id in ids


def test_get_user_access_map_private_no_grant_is_none(private_dashboard, other_analyst):
    """get_user_access_map (list serialization) returns None for a private
    resource the caller has no access to — so the list drops it."""
    OrgPreferences.objects.filter(org=private_dashboard.org).delete()
    OrgPreferences.objects.create(
        org=private_dashboard.org,
        default_analyst_level=AccessLevel.VIEW,
        default_member_level=AccessLevel.VIEW,
    )
    result = get_user_access_map(other_analyst, "dashboard", [private_dashboard])
    assert result.get(private_dashboard.id) is None


# ---------------------------------------------------------------------------
# Story 1: Floor settings — hierarchy validation + engine behavior
# spec test-spec.md §"Story 1"
# ---------------------------------------------------------------------------


from ddpui.api.org_preferences_api import update_access_defaults, get_org_preferences
from ddpui.schemas.org_preferences_schema import UpdateAccessDefaultsSchema


def _set_floors(caller, analyst, member, allow_public=True):
    return update_access_defaults(
        mock_request(caller),
        UpdateAccessDefaultsSchema(
            default_analyst_level=analyst,
            default_member_level=member,
            allow_public_sharing=allow_public,
        ),
    )


# ---- Floor hierarchy validation ---------------------------------


def test_valid_member_view_analyst_edit(org, admin):
    """Member=View, Analyst=Edit — valid; Member ≤ Analyst."""
    result = _set_floors(admin, "edit", "view")
    assert result["success"] is True


def test_valid_both_no_access(org, admin):
    """Both No Access is valid — equal floors allowed."""
    result = _set_floors(admin, "no_access", "no_access")
    assert result["success"] is True


def test_valid_both_view(org, admin):
    result = _set_floors(admin, "view", "view")
    assert result["success"] is True


def test_valid_both_edit(org, admin):
    result = _set_floors(admin, "edit", "edit")
    assert result["success"] is True


def test_invalid_member_view_analyst_no_access(org, admin):
    """Member=View > Analyst=No Access → 400 (member cannot exceed analyst)."""
    with pytest.raises(HttpError) as exc:
        _set_floors(admin, "no_access", "view")
    assert exc.value.status_code == 400


def test_invalid_member_edit_analyst_view(org, admin):
    with pytest.raises(HttpError) as exc:
        _set_floors(admin, "view", "edit")
    assert exc.value.status_code == 400


def test_invalid_member_edit_analyst_no_access(org, admin):
    with pytest.raises(HttpError) as exc:
        _set_floors(admin, "no_access", "edit")
    assert exc.value.status_code == 400


# ---- Floor applied to resource access ----------------------------


def test_analyst_gets_edit_from_default_floor(dashboard, other_analyst):
    """Default Analyst floor=Edit → analyst without a grant reads Edit."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.EDIT,
        default_member_level=AccessLevel.VIEW,
    )
    assert get_user_access(other_analyst, "dashboard", dashboard.id) == AccessLevel.EDIT


def test_member_gets_view_from_default_floor(dashboard, member):
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.EDIT,
        default_member_level=AccessLevel.VIEW,
    )
    assert get_user_access(member, "dashboard", dashboard.id) == AccessLevel.VIEW


def test_member_no_access_floor_returns_no_access(dashboard, member):
    """Explicit no_access floor for Members → no_access (not None)."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.EDIT,
        default_member_level=AccessLevel.NO_ACCESS,
    )
    assert get_user_access(member, "dashboard", dashboard.id) == AccessLevel.NO_ACCESS


def test_analyst_no_access_floor_returns_no_access(dashboard, other_analyst):
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.NO_ACCESS,
        default_member_level=AccessLevel.NO_ACCESS,
    )
    assert get_user_access(other_analyst, "dashboard", dashboard.id) == AccessLevel.NO_ACCESS


def test_admin_always_gets_edit_regardless_of_floor(dashboard, admin):
    """Admin bypasses the floor — always Edit."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.NO_ACCESS,
        default_member_level=AccessLevel.NO_ACCESS,
    )
    assert get_user_access(admin, "dashboard", dashboard.id) == AccessLevel.EDIT


def test_missing_orgpreferences_defaults_to_model_defaults(dashboard, member):
    """No OrgPreferences row → falls back to model defaults (Analyst=Edit,
    Member=View). Was a bug — fixed to use in-memory instance defaults."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    # Member should get View (model default) when no row exists.
    assert get_user_access(member, "dashboard", dashboard.id) == AccessLevel.VIEW


def test_creator_always_gets_edit_regardless_of_floor(dashboard, owner_analyst):
    """Resource creator always reads Edit — even with no_access floor."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.NO_ACCESS,
        default_member_level=AccessLevel.NO_ACCESS,
    )
    assert get_user_access(owner_analyst, "dashboard", dashboard.id) == AccessLevel.EDIT


def test_floor_change_takes_immediate_effect(dashboard, member):
    """After changing the floor, the next get_user_access call reflects it."""
    OrgPreferences.objects.filter(org=dashboard.org).delete()
    OrgPreferences.objects.create(
        org=dashboard.org,
        default_analyst_level=AccessLevel.EDIT,
        default_member_level=AccessLevel.VIEW,
    )
    assert get_user_access(member, "dashboard", dashboard.id) == AccessLevel.VIEW
    # Change floor.
    prefs = OrgPreferences.objects.get(org=dashboard.org)
    prefs.default_member_level = AccessLevel.NO_ACCESS
    prefs.save()
    assert get_user_access(member, "dashboard", dashboard.id) == AccessLevel.NO_ACCESS


def test_missing_resource_returns_none_not_no_access(other_analyst):
    """Missing resource → None (distinct from no_access — used by 404 vs 403)."""
    result = get_user_access(other_analyst, "dashboard", 999999)
    assert result is None


# ---------------------------------------------------------------------------
# Story 16: Edge cases — spec test-spec.md §"Story 16"
# ---------------------------------------------------------------------------


from ddpui.api.charts_api import delete_chart as _delete_chart_api


def test_cascade_edit_does_not_confer_delete(org, owner_analyst, other_analyst):
    """Spec §"Cascade" line 306: 'Edit cascade does not confer delete.'
    A non-owner-non-admin Analyst has Edit (via floor + can_delete_charts perm)
    but is still blocked from deleting because owner/admin gate lives in
    ``ChartService.delete_chart``."""
    chart = _chart(org, owner_analyst)
    with pytest.raises(HttpError) as exc:
        _delete_chart_api(mock_request(other_analyst), chart_id=chart.id)
    # 403 from ChartService; caveat: has_permission decorator converts internal
    # 403s → HttpError(404, "unauthorized") on missing perms — but Analysts
    # have can_delete_charts, so we hit the service-layer 403 as intended.
    assert exc.value.status_code == 403
    assert "owner or an admin" in str(exc.value).lower()


def test_cascade_edit_confers_reshare_rights(org, owner_analyst, no_access_member, member):
    """Spec: derived Edit (cascade only) still allows sharing to others."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, no_access_member, "edit")
    # no_access_member has cascade-Edit — should be able to share the chart.
    add_resource_grants(
        mock_request(no_access_member),
        "chart",
        str(chart.id),
        AddGrantsPayload(
            principals=[
                PrincipalGrantPayload(
                    principal_type="user", principal_id=member.id, access_level="view"
                )
            ]
        ),
    )
    assert ResourceShare.objects.filter(
        resource_type=ResourceType.CHART,
        resource_id=str(chart.id),
        principal_id=member.id,
        parent__isnull=True,
    ).exists()


def test_floor_view_returns_all_non_private_resources(org, owner_analyst, member):
    """Member with floor=View sees every non-private resource in the list."""
    d1 = Dashboard.objects.create(title="D1", org=org, created_by=owner_analyst)
    d2 = Dashboard.objects.create(title="D2", org=org, created_by=owner_analyst)
    try:
        OrgPreferences.objects.filter(org=org).delete()
        OrgPreferences.objects.create(
            org=org,
            default_analyst_level=AccessLevel.EDIT,
            default_member_level=AccessLevel.VIEW,
        )
        q = accessible_filter(member, "dashboard")
        ids = set(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
        assert d1.id in ids and d2.id in ids
    finally:
        d1.delete()
        d2.delete()


def test_private_resource_excluded_from_floor_only_list(org, owner_analyst, member):
    """Mix of private + public resources → floor-only user sees only non-private."""
    d_public = Dashboard.objects.create(title="Pub", org=org, created_by=owner_analyst)
    d_private = Dashboard.objects.create(
        title="Priv", org=org, created_by=owner_analyst, is_private=True
    )
    try:
        OrgPreferences.objects.filter(org=org).delete()
        OrgPreferences.objects.create(
            org=org,
            default_analyst_level=AccessLevel.EDIT,
            default_member_level=AccessLevel.VIEW,
        )
        q = accessible_filter(member, "dashboard")
        ids = set(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
        assert d_public.id in ids
        assert d_private.id not in ids
    finally:
        d_public.delete()
        d_private.delete()


def test_admin_sees_all_resources_including_private(org, owner_analyst, admin):
    """Admin sees every resource — private and non-private."""
    d_public = Dashboard.objects.create(title="P", org=org, created_by=owner_analyst)
    d_private = Dashboard.objects.create(
        title="Priv", org=org, created_by=owner_analyst, is_private=True
    )
    try:
        q = accessible_filter(admin, "dashboard")
        ids = set(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
        assert d_public.id in ids and d_private.id in ids
    finally:
        d_public.delete()
        d_private.delete()


def test_accessible_filter_handles_orphan_created_by(org, member):
    """Orphan resource (created_by=None) must not blow up the filter."""
    orphan = Dashboard.objects.create(title="Orphan", org=org, created_by=None)
    try:
        q = accessible_filter(member, "dashboard")
        # Just calling the filter without exception is the assertion.
        list(Dashboard.objects.filter(org=org).filter(q))
    finally:
        orphan.delete()


# ---------------------------------------------------------------------------
# Story 12: Groups — grants + management
# ---------------------------------------------------------------------------


from ddpui.models.org_user import OrgUserGroup, OrgUserGroupMember
from ddpui.api.user_org_api import (
    CreateGroupPayload,
    UpdateGroupPayload,
    create_user_group,
    list_user_groups,
    rename_user_group,
)


def _group(org, owner, name="G"):
    return OrgUserGroup.objects.create(org=org, name=name, created_by=owner)


def _group_share(org, resource_type, resource_id, group, level):
    return ResourceShare.objects.create(
        org=org,
        resource_type=resource_type,
        resource_id=str(resource_id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
        access_level=level,
    )


# ---- Grant behavior ------------------------------------------------------


def test_user_in_group_edit_grant_no_access_floor_gets_edit(org, owner_analyst, no_access_member):
    """Group grant + no-access floor → member gets group's Edit level."""
    group = _group(org, owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=no_access_member)
    d = Dashboard.objects.create(title="D-B05", org=org, created_by=owner_analyst)
    try:
        _group_share(org, ResourceType.DASHBOARD, d.id, group, AccessLevel.EDIT)
        assert get_user_access(no_access_member, "dashboard", d.id) == AccessLevel.EDIT
    finally:
        d.delete()


def test_user_in_two_groups_gets_max_level(org, owner_analyst, no_access_member):
    """User in Group A (Edit) and Group B (View) on same resource → max = Edit."""
    g_edit = _group(org, owner_analyst, "GE")
    g_view = _group(org, owner_analyst, "GV")
    OrgUserGroupMember.objects.create(group=g_edit, orguser=no_access_member)
    OrgUserGroupMember.objects.create(group=g_view, orguser=no_access_member)
    d = Dashboard.objects.create(title="D-B06", org=org, created_by=owner_analyst)
    try:
        _group_share(org, ResourceType.DASHBOARD, d.id, g_edit, AccessLevel.EDIT)
        _group_share(org, ResourceType.DASHBOARD, d.id, g_view, AccessLevel.VIEW)
        assert get_user_access(no_access_member, "dashboard", d.id) == AccessLevel.EDIT
    finally:
        d.delete()


def test_group_dashboard_share_cascades_to_members(org, owner_analyst, no_access_member):
    """Group dashboard share → cascade child rows created for the group →
    group member gets chart access via that cascade row."""
    group = _group(org, owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=no_access_member)
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    add_resource_grants(
        mock_request(owner_analyst),
        "dashboard",
        str(d.id),
        AddGrantsPayload(
            principals=[
                PrincipalGrantPayload(
                    principal_type="group", principal_id=group.id, access_level="edit"
                )
            ]
        ),
    )
    assert get_user_access(no_access_member, "chart", chart.id) == AccessLevel.EDIT


def test_owner_adds_grant_for_group(org, owner_analyst, dashboard):
    """POST /grants with principal_type=group creates a group share row."""
    group = _group(org, owner_analyst)
    add_resource_grants(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        AddGrantsPayload(
            principals=[
                PrincipalGrantPayload(
                    principal_type="group", principal_id=group.id, access_level="view"
                )
            ]
        ),
    )
    assert ResourceShare.objects.filter(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
    ).exists()


def test_direct_share_for_owner_raises_400(org, owner_analyst, dashboard):
    """Sharing a resource directly with its owner must be rejected with 400."""
    with pytest.raises(HttpError) as exc:
        add_resource_grants(
            mock_request(owner_analyst),
            "dashboard",
            str(dashboard.id),
            AddGrantsPayload(
                principals=[
                    PrincipalGrantPayload(
                        principal_type="user",
                        principal_id=owner_analyst.id,
                        access_level="view",
                    )
                ]
            ),
        )
    assert exc.value.status_code == 400
    assert "owner" in str(exc.value.message).lower()


def test_group_share_with_owner_as_member_succeeds_with_warning(
    org, owner_analyst, dashboard, member
):
    """Sharing via a group that includes the owner succeeds; response carries a warning."""
    group = _group(org, owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=owner_analyst)
    OrgUserGroupMember.objects.create(group=group, orguser=member)

    result = add_resource_grants(
        mock_request(owner_analyst),
        "dashboard",
        str(dashboard.id),
        AddGrantsPayload(
            principals=[
                PrincipalGrantPayload(
                    principal_type="group", principal_id=group.id, access_level="edit"
                )
            ]
        ),
    )
    # Share row is created.
    assert ResourceShare.objects.filter(
        org=org,
        resource_type=ResourceType.DASHBOARD,
        resource_id=str(dashboard.id),
        principal_type=ResourceSharePrincipalType.GROUP,
        principal_id=group.id,
    ).exists()
    # Response carries at least one advisory warning mentioning the owner.
    assert len(result.warnings) > 0
    assert any("owner" in w.lower() for w in result.warnings)


# ---- Group management authz --------------------------------


def test_member_cannot_create_group(org, member):
    """Members lack ``can_create_user_group`` → create returns 404 (has_permission
    decorator quirk: internal 403 → 404 UNAUTHORIZED)."""
    with pytest.raises(HttpError) as exc:
        create_user_group(
            mock_request(member),
            CreateGroupPayload(name="X", orguser_ids=[]),
        )
    assert exc.value.status_code in (403, 404)


def test_non_creator_analyst_cannot_rename_another_analysts_group(
    org, owner_analyst, other_analyst
):
    """Only the group's creator or an Admin can rename. Other Analysts
    (with ``can_edit_user_group`` on their role) must be blocked."""
    group = _group(org, owner_analyst, "AnalystOwnedGroup")
    with pytest.raises(HttpError) as exc:
        rename_user_group(
            mock_request(other_analyst),
            group_id=group.id,
            payload=UpdateGroupPayload(name="Renamed"),
        )
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# Story 5: Member + No-Access floor — empty-state + grant/ownership visibility
# spec test-spec.md §"Story 5"
# ---------------------------------------------------------------------------


def test_edit_grant_on_no_access_floor_returns_edit(org, owner_analyst, no_access_member):
    """Explicit Edit grant survives no-access floor → user reads Edit."""
    d = Dashboard.objects.create(title="D-B01", org=org, created_by=owner_analyst)
    try:
        _grant(org, d, no_access_member, AccessLevel.EDIT)
        assert get_user_access(no_access_member, "dashboard", d.id) == AccessLevel.EDIT
    finally:
        d.delete()


def test_view_grant_on_no_access_floor_returns_view(org, owner_analyst, no_access_member):
    d = Dashboard.objects.create(title="D-B03", org=org, created_by=owner_analyst)
    try:
        _grant(org, d, no_access_member, AccessLevel.VIEW)
        assert get_user_access(no_access_member, "dashboard", d.id) == AccessLevel.VIEW
    finally:
        d.delete()


def test_no_floor_no_grants_no_ownership_empty_list(org, owner_analyst, no_access_member):
    """Member with no-access floor, no grants, not owner → sees zero dashboards."""
    Dashboard.objects.create(title="Someone else's", org=org, created_by=owner_analyst)
    try:
        q = accessible_filter(no_access_member, "dashboard")
        result = Dashboard.objects.filter(org=org).filter(q)
        assert result.count() == 0
    finally:
        Dashboard.objects.filter(org=org, title="Someone else's").delete()


def test_no_floor_direct_grant_shows_only_granted(org, owner_analyst, no_access_member):
    """Member with no-access floor + one direct grant → sees only that resource."""
    d_grant = Dashboard.objects.create(title="Granted", org=org, created_by=owner_analyst)
    d_other = Dashboard.objects.create(title="Other", org=org, created_by=owner_analyst)
    try:
        _grant(org, d_grant, no_access_member, AccessLevel.VIEW)
        q = accessible_filter(no_access_member, "dashboard")
        ids = set(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
        assert d_grant.id in ids
        assert d_other.id not in ids
    finally:
        d_grant.delete()
        d_other.delete()


def test_no_floor_owner_sees_own_resources(org, no_access_member):
    """No-access floor + user is creator → own resource visible."""
    # no_access_member creates their own dashboard.
    mine = Dashboard.objects.create(title="Mine", org=org, created_by=no_access_member)
    try:
        q = accessible_filter(no_access_member, "dashboard")
        ids = set(Dashboard.objects.filter(org=org).filter(q).values_list("id", flat=True))
        assert mine.id in ids
    finally:
        mine.delete()


def test_no_floor_cascade_grant_shows_chart(org, owner_analyst, no_access_member):
    """No-access floor + cascade grant on chart → chart in accessible list."""
    chart = _chart(org, owner_analyst)
    d = _dashboard_with_inner(org, owner_analyst, chart.id)
    _share_dashboard(owner_analyst, d, no_access_member, "view")
    q = accessible_filter(no_access_member, "chart")
    ids = set(Chart.objects.filter(org=org).filter(q).values_list("id", flat=True))
    assert chart.id in ids


# ---------------------------------------------------------------------------
# Story 10: External user invite → acceptance → grant promotion
# spec test-spec.md §"Story 10"
# ---------------------------------------------------------------------------


from uuid import uuid4
from ddpui.models.org_user import Invitation
from ddpui.models.org_user import AcceptInvitationSchema
from ddpui.core.orguserfunctions import accept_invitation_v1


def _invitation(org, invited_by, email, role_slug=MEMBER_ROLE):
    from django.utils import timezone as _tz

    return Invitation.objects.create(
        invited_email=email,
        invited_by=invited_by,
        invited_on=_tz.now(),
        invite_code=str(uuid4()),
        invited_new_role=Role.objects.filter(slug=role_slug).first(),
    )


def _pending_share(org, resource_type, resource_id, invitation, level):
    return ResourceShare.objects.create(
        org=org,
        resource_type=resource_type,
        resource_id=str(resource_id),
        principal_type=ResourceSharePrincipalType.USER,  # will be user post-promotion
        principal_id=None,
        invitation=invitation,
        access_level=level,
    )


def test_pending_resource_share_promoted_on_invite_accept(org, owner_analyst, dashboard):
    """Pending ResourceShare (invitation_id set) — after user accepts invite,
    the row's principal_id becomes the new orguser and invitation becomes NULL."""
    invite = _invitation(org, owner_analyst, "newbie@t.com")
    pending = _pending_share(org, ResourceType.DASHBOARD, dashboard.id, invite, AccessLevel.VIEW)

    accept_invitation_v1(AcceptInvitationSchema(invite_code=invite.invite_code, password="pw"))

    pending.refresh_from_db()
    assert pending.principal_type == ResourceSharePrincipalType.USER
    new_orguser = OrgUser.objects.get(user__email="newbie@t.com", org=org)
    assert pending.principal_id == new_orguser.id
    assert pending.invitation_id is None
    # Cleanup
    new_orguser.user.delete()


def test_pending_group_membership_promoted_on_invite_accept(org, owner_analyst):
    """Pending OrgUserGroupMember (invitation_id set) — after acceptance,
    orguser is set and invitation is nulled."""
    group = OrgUserGroup.objects.create(org=org, name="Grp10", created_by=owner_analyst)
    invite = _invitation(org, owner_analyst, "newmember@t.com")
    membership = OrgUserGroupMember.objects.create(group=group, invitation=invite)

    accept_invitation_v1(AcceptInvitationSchema(invite_code=invite.invite_code, password="pw"))

    membership.refresh_from_db()
    new_orguser = OrgUser.objects.get(user__email="newmember@t.com", org=org)
    assert membership.orguser_id == new_orguser.id
    # invitation FK is SET_NULL on delete → still there here, but the FK stops
    # being reachable after invitation.delete() at the end of the flow.
    # Cleanup
    new_orguser.user.delete()
    group.delete()


def test_promoted_share_gives_effective_access(org, owner_analyst, dashboard):
    """After acceptance, the promoted share confers effective access via
    get_user_access on the shared resource."""
    invite = _invitation(org, owner_analyst, "accessuser@t.com")
    _pending_share(org, ResourceType.DASHBOARD, dashboard.id, invite, AccessLevel.EDIT)

    accept_invitation_v1(AcceptInvitationSchema(invite_code=invite.invite_code, password="pw"))

    new_orguser = OrgUser.objects.get(user__email="accessuser@t.com", org=org)
    assert get_user_access(new_orguser, "dashboard", dashboard.id) == AccessLevel.EDIT
    new_orguser.user.delete()


def test_pending_invite_appears_in_grants_list_with_pending_status(org, owner_analyst, dashboard):
    """Before acceptance, list_grants shows the invitation as a `pending` row."""
    invite = _invitation(org, owner_analyst, "pending@t.com")
    _pending_share(org, ResourceType.DASHBOARD, dashboard.id, invite, AccessLevel.VIEW)

    result = list_resource_grants(mock_request(owner_analyst), "dashboard", str(dashboard.id))
    pending_rows = [s for s in result.shares if s.status == "pending"]
    assert len(pending_rows) == 1
    assert pending_rows[0].email == "pending@t.com"
    # Cleanup
    invite.delete()


@pytest.mark.xfail(
    reason=(
        "BUG: list_user_groups is gated on can_view_user_groups (Members lack it) "
        "so Members get 404 UNAUTHORIZED, but spec line 206 says 'Member sees only "
        "their own groups' — implying Members CAN see groups they're a member of. "
        "Fix options: (a) grant Members can_view_user_groups + add visibility filter, "
        "(b) accept spec is out of date and update spec."
    ),
    strict=True,
)
def test_group_visibility_by_role(org, owner_analyst, other_analyst, member):
    """Analyst sees own + member-of groups; Member sees only groups they're a
    member of."""
    my_group = _group(org, owner_analyst, "OwnedByOwner")
    other_group = _group(org, other_analyst, "OwnedByOther")
    OrgUserGroupMember.objects.create(group=other_group, orguser=member)
    try:
        result_owner = list_user_groups(mock_request(owner_analyst))
        ids_owner = {g.id for g in result_owner}
        assert my_group.id in ids_owner

        result_member = list_user_groups(mock_request(member))
        ids_member = {g.id for g in result_member}
        assert other_group.id in ids_member
    finally:
        my_group.delete()
        other_group.delete()
