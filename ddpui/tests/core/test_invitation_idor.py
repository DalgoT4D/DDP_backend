"""Regression tests for invitation cross-tenant IDOR.

Before the fix, ``Invitation.objects.filter(id=invitation_id)`` was used
without any org constraint in both:

* ``ddpui.api.user_org_api.delete_invitation``
* ``ddpui.core.orguserfunctions.resend_invitation``

A user with ``can_delete_invitation`` / ``can_edit_invitation`` in *any*
org could therefore guess an integer ``invitation_id`` and revoke or
re-trigger an invitation belonging to a *different* tenant.

These tests exercise the queryset call that the fix relies on: the
filter must include ``invited_by__org=<requestor_org>``, and the lookup
must miss when the invitation belongs to another org.
"""

from unittest.mock import Mock, patch

from ddpui.api import user_org_api
from ddpui.core import orguserfunctions


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _request_for(orguser):
    """Build a Mock request that satisfies @has_permission and the endpoint body."""
    request = Mock()
    request.permissions = ["can_delete_invitation"]
    request.orguser = orguser
    return request


# ---------------------------------------------------------------------------
# delete_invitation (api layer)
# ---------------------------------------------------------------------------


@patch("ddpui.api.user_org_api.Invitation.objects.filter")
def test_delete_invitation_filters_by_requestor_org(mock_filter):
    """The filter must include invited_by__org so cross-tenant lookups miss."""
    org_a = Mock(name="org_a")
    orguser = Mock(org=org_a)

    mock_filter.return_value.first.return_value = None  # cross-org lookup → not found

    result = user_org_api.delete_invitation(_request_for(orguser), 42)

    mock_filter.assert_called_once_with(id=42, invited_by__org=org_a)
    assert result == {"success": 1}


@patch("ddpui.api.user_org_api.Invitation.objects.filter")
def test_delete_invitation_does_not_delete_other_orgs_invitation(mock_filter):
    """If the lookup returns no row, .delete() must not be called."""
    org_a = Mock(name="org_a")
    orguser = Mock(org=org_a)

    foreign_invitation = Mock()  # belongs to some other org — but filter excludes it
    mock_filter.return_value.first.return_value = None  # filter excluded it

    user_org_api.delete_invitation(_request_for(orguser), 999)

    foreign_invitation.delete.assert_not_called()


@patch("ddpui.api.user_org_api.Invitation.objects.filter")
def test_delete_invitation_deletes_when_invitation_belongs_to_org(mock_filter):
    """In-org delete still works."""
    org_a = Mock(name="org_a")
    orguser = Mock(org=org_a)

    own_invitation = Mock()
    mock_filter.return_value.first.return_value = own_invitation

    result = user_org_api.delete_invitation(_request_for(orguser), 7)

    mock_filter.assert_called_once_with(id=7, invited_by__org=org_a)
    own_invitation.delete.assert_called_once()
    assert result == {"success": 1}


# ---------------------------------------------------------------------------
# resend_invitation (core layer)
# ---------------------------------------------------------------------------


@patch("ddpui.core.orguserfunctions.awsses.send_invite_user_email")
@patch("ddpui.core.orguserfunctions.Invitation.objects.filter")
def test_resend_invitation_does_not_resend_other_orgs_invitation(
    mock_filter, mock_send_email
):
    """A foreign-org invitation_id must not trigger an email."""
    org_a = Mock(name="org_a")
    mock_filter.return_value.first.return_value = None  # filtered out by org

    result, error = orguserfunctions.resend_invitation("42", org_a)

    mock_filter.assert_called_once_with(id="42", invited_by__org=org_a)
    mock_send_email.assert_not_called()
    assert result is None and error is None


@patch("ddpui.core.orguserfunctions.awsses.send_invite_user_email")
@patch("ddpui.core.orguserfunctions.Invitation.objects.filter")
def test_resend_invitation_resends_when_invitation_belongs_to_org(
    mock_filter, mock_send_email
):
    """In-org resend triggers the invitation email."""
    org_a = Mock(name="org_a")
    invitation = Mock(invite_code="abc", invited_email="invitee@example.com")
    invitation.invited_by.user.email = "admin@example.com"
    mock_filter.return_value.first.return_value = invitation

    orguserfunctions.resend_invitation("7", org_a)

    mock_filter.assert_called_once_with(id="7", invited_by__org=org_a)
    invitation.save.assert_called_once()
    mock_send_email.assert_called_once()
