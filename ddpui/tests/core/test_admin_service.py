"""
Tests for the admin service layer (ddpui/core/admin/admin_service.py).

The admin portal has no session of its own — it authenticates through the shared
POST /api/v2/login/ and each route is gated by @platform_admin_required. What is left
here is the org / invitation / removal-impact business logic.
"""

import os
from uuid import uuid4

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.core.admin import admin_service
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import Invitation, OrgUser
from ddpui.schemas.admin_schema import AdminUpdateOrgSchema, AdminCreateNotificationSchema

pytestmark = pytest.mark.django_db


# --------------------------------------------------------------------------- #
# update_org
# --------------------------------------------------------------------------- #


def test_update_org_leaves_omitted_fields_untouched():
    """
    Partial update: a field passed as None is NOT cleared, it is left alone. The API
    sends None for every field the admin did not edit, so treating None as "set to null"
    would wipe viz_url every time someone renamed an org.
    """
    org = Org.objects.create(name="Before", slug="before-slug", viz_url="https://viz.example.com/")

    admin_service.update_org(org, AdminUpdateOrgSchema(name="After"))

    org.refresh_from_db()
    assert org.name == "After"
    assert org.viz_url == "https://viz.example.com/"  # NOT cleared
    assert org.slug == "before-slug"  # slug is never touched


def test_update_org_without_org_plans_row_does_not_raise():
    """
    base_plan lives on OrgPlans, and an org can exist without one (nothing guarantees the
    row). Setting a plan on such an org is a no-op rather than a crash.
    """
    org = Org.objects.create(name="Planless", slug="planless")
    assert not OrgPlans.objects.filter(org=org).exists()

    result = admin_service.update_org(org, AdminUpdateOrgSchema(base_plan="Dalgo"))

    assert result.id == org.id
    assert not OrgPlans.objects.filter(org=org).exists()  # still none — silently skipped


# --------------------------------------------------------------------------- #
# invitation lookup + scoping
# --------------------------------------------------------------------------- #


def _make_invitation(org, email, inviter_org=None):
    """an Invitation into `org`, sent by someone who may belong to a different org"""
    inviter_user = User.objects.create(username=f"inviter-{email}", email=f"inviter-{email}")
    inviter = OrgUser.objects.create(user=inviter_user, org=inviter_org or org)
    return Invitation.objects.create(
        invited_email=email,
        invited_by=inviter,
        invited_in_org=org,
        invited_on=timezone.now(),
        invite_code=str(uuid4()),
    )


def test_get_pending_invitation_normalizes_the_email():
    """
    Invitation emails are matched case-insensitively and whitespace-trimmed, so the
    lookup after an invite finds the row the invite just wrote regardless of how the
    admin typed the address.
    """
    org = Org.objects.create(name="Akshara", slug="akshara-norm")
    invitation = _make_invitation(org, "Priya@Akshara.ORG")

    found = admin_service.get_pending_invitation(org, "  priya@akshara.org  ")

    assert found is not None
    assert found.id == invitation.id


def test_list_org_invitations_is_scoped_by_target_org():
    """
    The Users tab lists invitations by TARGET org (invited_in_org), not by the inviter's
    org. So a cross-org invite a platform admin sent into Akshara shows on Akshara's tab,
    and Bhumi's invites never leak into it.
    """
    akshara = Org.objects.create(name="Akshara", slug="akshara-scope")
    bhumi = Org.objects.create(name="Bhumi", slug="bhumi-scope")
    # the inviter belongs to Bhumi but the invite targets Akshara — the cross-org case
    _make_invitation(akshara, "into-akshara@x.org", inviter_org=bhumi)
    _make_invitation(bhumi, "into-bhumi@x.org", inviter_org=bhumi)

    emails = {inv.invited_email for inv in admin_service.list_org_invitations(akshara)}

    assert emails == {"into-akshara@x.org"}  # Bhumi's invite is not listed here


# --------------------------------------------------------------------------- #
# notifications -- preview / create / history
# --------------------------------------------------------------------------- #


def test_preview_notification_recipients_whole_platform_counts_everyone():
    """org_ids omitted (whole platform) counts every OrgUser, merged across every
    org -- not a per-org breakdown (plan.md §4.3)"""
    org = Org.objects.create(name="Whole Platform Org", slug="whole-platform-org")
    user = User.objects.create(username="wp@x.org", email="wp@x.org")
    OrgUser.objects.create(user=user, org=org)

    count = admin_service.preview_notification_recipients(None)

    assert count >= 1


def test_preview_notification_recipients_merges_multiple_orgs():
    """org_ids with several orgs merges their recipients into one combined count"""
    org_a = Org.objects.create(name="Org A", slug="preview-org-a")
    org_b = Org.objects.create(name="Org B", slug="preview-org-b")
    OrgUser.objects.create(user=User.objects.create(username="a@x.org", email="a@x.org"), org=org_a)
    OrgUser.objects.create(user=User.objects.create(username="b@x.org", email="b@x.org"), org=org_b)

    count = admin_service.preview_notification_recipients([org_a.id, org_b.id])

    assert count == 2


def test_preview_notification_recipients_bogus_org_id_contributes_zero():
    """a bogus org_id inside a selection contributes zero rather than erroring
    (plan.md §4.3, §5)"""
    count = admin_service.preview_notification_recipients([9999999])

    assert count == 0


def test_create_admin_notification_blocks_zero_recipient_audience():
    """a bogus-only org_ids selection has zero recipients, so nothing is created"""
    error, notification = admin_service.create_admin_notification(
        "meera@example.com",
        AdminCreateNotificationSchema(message="hello", email_subject="subject", org_ids=[9999999]),
    )

    assert error is not None
    assert notification is None


def test_create_admin_notification_persists_audience_channels_and_server_author():
    """author comes from the platform admin's own email, never the client; audience
    and channel choice persist onto the created Notification (plan.md §4.1, §4.3)"""
    org = Org.objects.create(name="Create Org", slug="create-notif-org")
    OrgUser.objects.create(user=User.objects.create(username="c@x.org", email="c@x.org"), org=org)

    error, notification = admin_service.create_admin_notification(
        "meera@example.com",
        AdminCreateNotificationSchema(
            message="hello",
            email_subject="subject",
            org_ids=[org.id],
            send_in_app=True,
            send_email=False,
        ),
    )

    assert error is None
    assert notification.author == "meera@example.com"
    assert notification.target_org_ids == [org.id]
    assert notification.send_in_app is True
    assert notification.send_email is False
    assert notification.sent_time is not None


def test_get_admin_notification_history_resolves_org_names_and_recipient_count():
    """history resolves target_org_ids to org names and reports the true
    recipient count, regardless of which channels were chosen (plan.md §4.3)"""
    org = Org.objects.create(name="History Org", slug="history-org")
    OrgUser.objects.create(user=User.objects.create(username="h@x.org", email="h@x.org"), org=org)

    _, notification = admin_service.create_admin_notification(
        "meera@example.com",
        AdminCreateNotificationSchema(message="hist", email_subject="subject", org_ids=[org.id]),
    )

    history = admin_service.get_admin_notification_history()
    entry = next(item for item in history if item.id == notification.id)

    assert entry.target_org_names == ["History Org"]
    assert entry.recipient_count == 1


def test_removal_impact_is_zero_for_a_user_with_no_content():
    """
    A user who created nothing orphans nothing. The confirm dialog reads these counts,
    so zeros are what let it skip the warning instead of showing an empty one.
    """
    org = Org.objects.create(name="Empty", slug="empty-org")
    user = User.objects.create(username="nobody@x.org", email="nobody@x.org")
    orguser = OrgUser.objects.create(user=user, org=org)

    assert admin_service.removal_impact(orguser) == (0, 0, 0)
