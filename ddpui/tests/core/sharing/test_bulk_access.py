"""Task 17: POST /api/access/bulk/ — one action applied across a selection
of resources (mixed rtypes allowed).

Bulk semantics are apply-where-possible: every item is independently
gated (registry -> share slug -> org-scoped fetch -> resolver edit ->
capability flag) and failures become `skipped` entries with a reason code,
never a whole-request 4xx. Only request-shape problems (empty selection,
selection over the cap, bad action, missing action payload, bad action
payload) fail the whole request.

Route functions are called directly via `mock_request(orguser)`, same as
test_access_api.py.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import Mock, patch
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.models.alert import Alert, AlertType
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(name="Bulk Access Org", slug="bulk-access-org")
    yield org
    # KPI.metric is PROTECT — delete KPIs before the Metric/Org CASCADE runs.
    KPI.objects.filter(org=org).delete()
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "bulkaccess-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "bulkaccess-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def analyst2(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "bulkaccess-analyst2")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "bulkaccess-member")
    yield ou
    ou.delete()


def _dashboard(org_obj, owner, audience=GeneralAudience.PRIVATE, level=GeneralLevel.VIEW):
    return Dashboard.objects.create(
        title="Bulk Test Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        general_audience=audience,
        general_level=level,
    )


def _report(org_obj, owner, audience=GeneralAudience.PRIVATE, level=GeneralLevel.VIEW):
    return ReportSnapshot.objects.create(
        title="Bulk Test Report",
        org=org_obj,
        owner=owner,
        created_by=owner,
        general_audience=audience,
        general_level=level,
    )


def _metric(org_obj, owner, audience=GeneralAudience.PRIVATE, level=GeneralLevel.VIEW):
    return Metric.objects.create(
        org=org_obj,
        name="bulk-metric",
        schema_name="s",
        table_name="t",
        column="c",
        aggregation="sum",
        created_by=owner,
        owner=owner,
        general_audience=audience,
        general_level=level,
    )


def _alert(org_obj, owner, audience=GeneralAudience.PRIVATE, level=GeneralLevel.VIEW):
    return Alert.objects.create(
        org=org_obj,
        name="bulk-alert",
        alert_type=AlertType.STANDALONE,
        standalone_config={
            "schema_name": "public",
            "table_name": "t",
            "column": "amount",
            "aggregation": "sum",
        },
        condition={"operator": "gt", "value": 0},
        schedule_cron="0 9 * * *",
        message_template="test",
        owner=owner,
        created_by=owner,
        general_audience=audience,
        general_level=level,
    )


def _grant(org_obj, rtype, resource, principal_orguser, permission="view", status="active"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal_orguser.id,
        permission=permission,
        status=status,
    )


def _bulk(caller, items, action, **kwargs):
    from ddpui.api.access_api import bulk_access
    from ddpui.schemas.access_schema import BulkAccessRequest, BulkItemRef

    payload = BulkAccessRequest(
        items=[BulkItemRef(rtype=r, id=str(i)) for r, i in items],
        action=action,
        **kwargs,
    )
    return bulk_access(mock_request(caller), payload)


def _grant_payload(principal=None, permission="view", email=None, principal_type="user"):
    from ddpui.schemas.access_schema import GrantCreate

    return GrantCreate(
        principal_type=principal_type,
        principal_id=principal.id if principal is not None else None,
        email=email,
        permission=permission,
    )


# ================================================================================
# add_grant across a mixed selection
# ================================================================================


class TestBulkAddGrant:
    def test_mixed_selection_applies_where_possible_with_distinct_skip_reasons(
        self, org, analyst, analyst2, member
    ):
        """2 editable dashboards -> applied; a view-only report ->
        edit_access_denied; a metric (grants=False) -> grants_not_supported."""
        dash1 = _dashboard(org, analyst2)
        dash2 = _dashboard(org, analyst2)
        report = _report(org, analyst)
        _grant(org, "report", report, analyst2, permission="view")
        # analyst2 resolves to edit on the metric via general access, so it
        # passes the edit gate and is skipped by the capability flag instead
        metric = _metric(org, analyst, GeneralAudience.ANALYSTS_PLUS, GeneralLevel.EDIT)

        response = _bulk(
            analyst2,
            [
                ("dashboard", dash1.pk),
                ("dashboard", dash2.pk),
                ("report", report.pk),
                ("metric", metric.pk),
            ],
            "add_grant",
            add_grant=_grant_payload(principal=member, permission="view"),
        )

        assert response["success"] is True
        data = response["data"]
        assert data["applied"] == [
            {"rtype": "dashboard", "id": str(dash1.pk)},
            {"rtype": "dashboard", "id": str(dash2.pk)},
        ]
        skip_reasons = {(s["rtype"], s["id"]): s["reason"] for s in data["skipped"]}
        assert skip_reasons == {
            ("report", str(report.pk)): "edit_access_denied",
            ("metric", str(metric.pk)): "grants_not_supported",
        }
        assert data["applied_count"] == 2
        assert data["skipped_count"] == 2
        assert data["requires_confirmation"] == []

        # the grants really exist for the applied items, and only those
        for dash in (dash1, dash2):
            assert ResourceShare.objects.filter(
                org=org,
                resource_type="dashboard",
                resource_id=str(dash.pk),
                principal_id=member.id,
                status="active",
                permission="view",
            ).exists()
        assert not ResourceShare.objects.filter(
            resource_type="metric", resource_id=str(metric.pk)
        ).exists()

    def test_unknown_email_invites_once_and_creates_pending_grant_per_resource(self, org, analyst):
        """Sharing 3 resources with an unknown email sends exactly ONE
        invitation (one Invitation row, one email) but writes 3 pending
        grant rows — one per resource."""
        dash1 = _dashboard(org, analyst)
        dash2 = _dashboard(org, analyst)
        report = _report(org, analyst)

        with patch("ddpui.utils.awsses.send_invite_user_email") as mock_send:
            response = _bulk(
                analyst,
                [("dashboard", dash1.pk), ("dashboard", dash2.pk), ("report", report.pk)],
                "add_grant",
                add_grant=_grant_payload(email="bulk-future@test.com", permission="view"),
            )

        data = response["data"]
        assert data["applied_count"] == 3
        assert data["skipped"] == []

        assert Invitation.objects.filter(invited_email="bulk-future@test.com").count() == 1
        assert mock_send.call_count == 1
        assert (
            Invitation.objects.get(invited_email="bulk-future@test.com").invited_new_role.slug
            == MEMBER_ROLE
        )

        pending = ResourceShare.objects.filter(
            org=org, pending_email="bulk-future@test.com", status="pending", principal_id=None
        )
        assert {(s.resource_type, s.resource_id) for s in pending} == {
            ("dashboard", str(dash1.pk)),
            ("dashboard", str(dash2.pk)),
            ("report", str(report.pk)),
        }


# ================================================================================
# set_general with the aggregated narrow prompt
# ================================================================================


def _general_payload(audience, level="view", remove_grant_ids=None):
    from ddpui.schemas.access_schema import GeneralAccessUpdate

    return GeneralAccessUpdate(audience=audience, level=level, remove_grant_ids=remove_grant_ids)


class TestBulkSetGeneral:
    def test_narrowing_confirms_only_resources_with_grants_and_resend_commits(
        self, org, analyst, member
    ):
        """Narrowing 3 dashboards to private where only one has an active
        grant: the other two apply immediately on the FIRST call; the
        granted one comes back in requires_confirmation untouched. The
        re-send (remove_grant_ids present) commits it and deletes the
        grant."""
        dash1 = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        dash2 = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        dash3 = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        share = _grant(org, "dashboard", dash2, member)

        items = [("dashboard", dash1.pk), ("dashboard", dash2.pk), ("dashboard", dash3.pk)]
        response = _bulk(
            analyst, items, "set_general", set_general=_general_payload("private", "view")
        )

        data = response["data"]
        assert data["applied"] == [
            {"rtype": "dashboard", "id": str(dash1.pk)},
            {"rtype": "dashboard", "id": str(dash3.pk)},
        ]
        assert data["applied_count"] == 2
        assert data["skipped_count"] == 0
        assert len(data["requires_confirmation"]) == 1
        confirmation = data["requires_confirmation"][0]
        assert confirmation["rtype"] == "dashboard"
        assert confirmation["id"] == str(dash2.pk)
        assert [g["id"] for g in confirmation["persisting_grants"]] == [share.id]

        dash1.refresh_from_db()
        dash2.refresh_from_db()
        dash3.refresh_from_db()
        assert dash1.general_audience == GeneralAudience.PRIVATE
        assert dash3.general_audience == GeneralAudience.PRIVATE
        assert dash2.general_audience == GeneralAudience.ALL_USERS  # untouched
        assert ResourceShare.objects.filter(id=share.id).exists()

        # re-send just the undecided resource with the grant marked for removal
        response = _bulk(
            analyst,
            [("dashboard", dash2.pk)],
            "set_general",
            set_general=_general_payload("private", "view", remove_grant_ids=[share.id]),
        )
        data = response["data"]
        assert data["applied"] == [{"rtype": "dashboard", "id": str(dash2.pk)}]
        assert data["requires_confirmation"] == []
        dash2.refresh_from_db()
        assert dash2.general_audience == GeneralAudience.PRIVATE
        assert not ResourceShare.objects.filter(id=share.id).exists()

    def test_resend_with_empty_remove_list_commits_keeping_grants(self, org, analyst, member):
        dash = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        share = _grant(org, "dashboard", dash, member)

        response = _bulk(
            analyst,
            [("dashboard", dash.pk)],
            "set_general",
            set_general=_general_payload("admins", "view", remove_grant_ids=[]),
        )
        assert response["data"]["applied_count"] == 1
        dash.refresh_from_db()
        assert dash.general_audience == GeneralAudience.ADMINS
        assert ResourceShare.objects.filter(id=share.id).exists()  # deliberately kept

    def test_remove_grant_ids_outside_the_selection_404_whole_request(self, org, analyst, member):
        """A grant id belonging to a resource NOT in the (gate-surviving)
        selection is a client bug — 404 for the whole request, nothing
        committed. Mirrors the single-item endpoint."""
        dash = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        other_dash = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        foreign_share = _grant(org, "dashboard", other_dash, member)

        with pytest.raises(HttpError) as excinfo:
            _bulk(
                analyst,
                [("dashboard", dash.pk)],
                "set_general",
                set_general=_general_payload(
                    "private", "view", remove_grant_ids=[foreign_share.id]
                ),
            )
        assert excinfo.value.status_code == 404
        dash.refresh_from_db()
        assert dash.general_audience == GeneralAudience.ALL_USERS  # nothing committed
        assert ResourceShare.objects.filter(id=foreign_share.id).exists()


# ================================================================================
# toggle_public
# ================================================================================


def _toggle_payload(is_public):
    from ddpui.schemas.access_schema import BulkPublicToggle

    return BulkPublicToggle(is_public=is_public)


class TestBulkTogglePublic:
    def test_enable_applies_to_public_linkable_rtypes_and_skips_alert(self, org, analyst):
        """Enabling across dashboard + report + alert: the two public_link
        rtypes get a token and is_public=True; the alert (public_link=False)
        is skipped with its own reason."""
        dash = _dashboard(org, analyst)
        report = _report(org, analyst)
        alert = _alert(org, analyst)

        response = _bulk(
            analyst,
            [("dashboard", dash.pk), ("report", report.pk), ("alert", alert.pk)],
            "toggle_public",
            toggle_public=_toggle_payload(True),
        )

        data = response["data"]
        assert data["applied"] == [
            {"rtype": "dashboard", "id": str(dash.pk)},
            {"rtype": "report", "id": str(report.pk)},
        ]
        assert data["skipped"] == [
            {"rtype": "alert", "id": str(alert.pk), "reason": "public_link_not_supported"}
        ]

        dash.refresh_from_db()
        report.refresh_from_db()
        assert dash.is_public is True and dash.public_share_token
        assert dash.public_shared_at is not None and dash.public_disabled_at is None
        assert report.is_public is True and report.public_share_token

    def test_enable_blocked_by_kill_switch_skips_but_disable_still_allowed(self, org, analyst):
        from ddpui.models.org_preferences import OrgPreferences

        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        dash_off = _dashboard(org, analyst)
        dash_on = _dashboard(org, analyst)
        dash_on.is_public = True
        dash_on.public_share_token = "existing-token"
        dash_on.save()

        # enabling while the switch is off: skipped, nothing changed
        response = _bulk(
            analyst,
            [("dashboard", dash_off.pk)],
            "toggle_public",
            toggle_public=_toggle_payload(True),
        )
        assert response["data"]["applied"] == []
        assert response["data"]["skipped"] == [
            {"rtype": "dashboard", "id": str(dash_off.pk), "reason": "public_sharing_disabled"}
        ]
        dash_off.refresh_from_db()
        assert dash_off.is_public is False

        # disabling is ALWAYS allowed, even with the switch off
        response = _bulk(
            analyst,
            [("dashboard", dash_on.pk)],
            "toggle_public",
            toggle_public=_toggle_payload(False),
        )
        assert response["data"]["applied"] == [{"rtype": "dashboard", "id": str(dash_on.pk)}]
        dash_on.refresh_from_db()
        assert dash_on.is_public is False
        assert dash_on.public_share_token == "existing-token"  # kept for audit
        assert dash_on.public_disabled_at is not None


# ================================================================================
# Per-item gates and request-shape validation
# ================================================================================


class TestBulkGates:
    def test_cross_org_and_unknown_ids_skip_as_not_found(self, org, admin, member):
        """A cross-org resource id must be indistinguishable from a
        nonexistent one — both skip with `not_found`, never a leak. Unknown
        rtypes too."""
        other_org = Org.objects.create(name="Bulk Other Org", slug="bulk-other-org")
        other_admin = _make_orguser(other_org, ADMIN_ROLE, "bulkaccess-other-admin")
        foreign_dash = _dashboard(other_org, other_admin)
        mine = _dashboard(org, admin)

        response = _bulk(
            admin,
            [
                ("dashboard", mine.pk),
                ("dashboard", foreign_dash.pk),
                ("dashboard", 999999),
                ("chart", 1),
            ],
            "add_grant",
            add_grant=_grant_payload(principal=member, permission="view"),
        )

        data = response["data"]
        assert data["applied"] == [{"rtype": "dashboard", "id": str(mine.pk)}]
        skip_reasons = {(s["rtype"], s["id"]): s["reason"] for s in data["skipped"]}
        assert skip_reasons == {
            ("dashboard", str(foreign_dash.pk)): "not_found",
            ("dashboard", "999999"): "not_found",
            ("chart", "1"): "not_found",
        }
        # nothing was written for the foreign resource
        assert not ResourceShare.objects.filter(
            resource_type="dashboard", resource_id=str(foreign_dash.pk)
        ).exists()

    def test_member_without_share_slugs_gets_everything_skipped(self, org, analyst, member):
        """Design choice (documented): a caller with no can_share_* slugs
        gets every item skipped with `share_permission_denied` — bulk is
        apply-where-possible, never an all-or-nothing 403."""
        dash = _dashboard(org, analyst, GeneralAudience.ALL_USERS, GeneralLevel.EDIT)

        response = _bulk(
            member,
            [("dashboard", dash.pk)],
            "add_grant",
            add_grant=_grant_payload(principal=analyst, permission="view"),
        )
        assert response["data"]["applied"] == []
        assert response["data"]["skipped"] == [
            {"rtype": "dashboard", "id": str(dash.pk), "reason": "share_permission_denied"}
        ]

    def test_duplicate_items_are_deduplicated(self, org, analyst, member):
        dash = _dashboard(org, analyst)
        response = _bulk(
            analyst,
            [("dashboard", dash.pk), ("dashboard", dash.pk)],
            "add_grant",
            add_grant=_grant_payload(principal=member, permission="view"),
        )
        assert response["data"]["applied_count"] == 1
        assert response["data"]["applied"] == [{"rtype": "dashboard", "id": str(dash.pk)}]

    def test_empty_selection_400(self, org, analyst, member):
        with pytest.raises(HttpError) as excinfo:
            _bulk(analyst, [], "add_grant", add_grant=_grant_payload(principal=member))
        assert excinfo.value.status_code == 400

    def test_selection_over_cap_400(self, org, analyst, member):
        from ddpui.core.sharing.sharing_actions import BULK_MAX_ITEMS

        items = [("dashboard", i) for i in range(BULK_MAX_ITEMS + 1)]
        with pytest.raises(HttpError) as excinfo:
            _bulk(analyst, items, "add_grant", add_grant=_grant_payload(principal=member))
        assert excinfo.value.status_code == 400

    def test_unknown_action_400(self, org, analyst):
        dash = _dashboard(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            _bulk(analyst, [("dashboard", dash.pk)], "delete_everything")
        assert excinfo.value.status_code == 400

    def test_missing_action_payload_400(self, org, analyst):
        dash = _dashboard(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            _bulk(analyst, [("dashboard", dash.pk)], "add_grant")  # no add_grant payload
        assert excinfo.value.status_code == 400

    def test_malformed_grant_payload_400_whole_request(self, org, analyst):
        """A payload-shape error (invalid permission) is a client bug and
        fails the whole request — not N per-item skips."""
        dash = _dashboard(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            _bulk(
                analyst,
                [("dashboard", dash.pk)],
                "add_grant",
                add_grant=_grant_payload(email="x@test.com", permission="own"),
            )
        assert excinfo.value.status_code == 400


# ================================================================================
# Per-item atomicity: no selection-wide transaction
# ================================================================================


class TestBulkAtomicity:
    def test_one_failing_item_does_not_roll_back_the_others(self, org, analyst, member):
        """Pin: the bulk loop is NOT wrapped in a selection-wide
        transaction. A per-item failure mid-loop (here: forced on the 2nd
        of 3 dashboards) leaves the 1st item's committed write intact and
        still processes the 3rd."""
        from ddpui.core.sharing import sharing_actions
        from ddpui.core.sharing.exceptions import SharingValidationError

        dash1 = _dashboard(org, analyst)
        dash2 = _dashboard(org, analyst)
        dash3 = _dashboard(org, analyst)

        real_upsert = sharing_actions.upsert_grant

        def failing_on_dash2(grantor, rtype, resource, payload):
            if resource.pk == dash2.pk:
                raise SharingValidationError("forced per-item failure")
            return real_upsert(grantor, rtype, resource, payload)

        with patch.object(sharing_actions, "upsert_grant", side_effect=failing_on_dash2):
            response = _bulk(
                analyst,
                [("dashboard", dash1.pk), ("dashboard", dash2.pk), ("dashboard", dash3.pk)],
                "add_grant",
                add_grant=_grant_payload(principal=member, permission="view"),
            )

        data = response["data"]
        assert data["applied"] == [
            {"rtype": "dashboard", "id": str(dash1.pk)},
            {"rtype": "dashboard", "id": str(dash3.pk)},
        ]
        assert data["skipped"] == [
            {"rtype": "dashboard", "id": str(dash2.pk), "reason": "validation_error"}
        ]
        # the rows written before and after the failure both persist
        for dash in (dash1, dash3):
            assert ResourceShare.objects.filter(
                resource_type="dashboard", resource_id=str(dash.pk), principal_id=member.id
            ).exists()
        assert not ResourceShare.objects.filter(
            resource_type="dashboard", resource_id=str(dash2.pk)
        ).exists()
