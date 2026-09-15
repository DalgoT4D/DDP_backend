"""Tests for the user-account transactional email triggers.

Notification-family template tests (report share golden, mention golden, alert
email, generic notification, cross-shell chrome) live in ``test_templates.py``
in the same directory. Trial + biz-dev email helpers still live in
``ddpui.utils.awsses`` and are tested in ``tests/utils/test_awsses.py``.
"""

import os
from unittest.mock import patch

from ddpui.core.notifications.triggers.user import (
    send_added_to_org,
    send_invite_user,
    send_password_reset,
    send_signup,
)


def test_send_password_reset():
    """Plain-text password reset — recipient has no Dalgo account row yet."""
    with patch(
        "ddpui.core.notifications.triggers.user.send_text_message"
    ) as mock_send_text_message:
        send_password_reset("to_email", "reset_url")
        message = """Hello,

We received a request to reset your Dalgo password.

Please click this link to begin: reset_url.

If you did not request a password reset you may safely ignore this email.

"""
        mock_send_text_message.assert_called_once_with(
            "to_email", "You've requested a password reset", message
        )


def test_send_signup():
    """Plain-text signup verification — pre-account send."""
    with patch(
        "ddpui.core.notifications.triggers.user.send_text_message"
    ) as mock_send_text_message:
        send_signup("to_email", "verification_url")
        message = """Hello,

Welcome to Dalgo! Please verify your email address by clicking the link below

verification_url

    """
        mock_send_text_message.assert_called_once_with("to_email", "Welcome to Dalgo", message)


def test_send_invite_user_default_wording():
    """Fresh invite (no group context) uses the plain 'invited to Dalgo' wording,
    the trailing invite_url becomes the CTA, and the CTA label is 'Accept Invitation'."""
    with patch("ddpui.core.notifications.triggers.user.send_html_message") as mock_send_html:
        send_invite_user("to_email", "inviter@x.org", "https://dalgo/accept/abc")

    to_email, subject, plain, html_body = mock_send_html.call_args[0]
    assert to_email == "to_email"
    assert subject == "You have been invited to Dalgo by inviter@x.org"
    assert "Accept the invite and set your password to get started." in plain
    assert "https://dalgo/accept/abc" in plain
    assert "You have been invited to Dalgo by inviter@x.org" in html_body
    assert "https://dalgo/accept/abc" in html_body
    assert "Accept Invitation" in html_body


def test_send_invite_user_group_wording():
    """Group-flow invite names the group in the headline + body."""
    with patch("ddpui.core.notifications.triggers.user.send_html_message") as mock_send_html:
        send_invite_user(
            "to_email",
            "inviter@x.org",
            "https://dalgo/accept/abc",
            group_name="Funders",
        )

    _, subject, plain, html_body = mock_send_html.call_args[0]
    assert subject == "You have been added to the Funders group by inviter@x.org"
    assert "Accept the invite and set your password to explore your workspace." in plain
    assert "You have been added to the Funders group by inviter@x.org" in html_body
    assert "Accept Invitation" in html_body


def test_send_added_to_org_default_wording():
    """Existing Dalgo user added to a new org (no group) — CTA 'Explore Workspace'
    points at FRONTEND_URL."""
    with patch(
        "ddpui.core.notifications.triggers.user.send_html_message"
    ) as mock_send_html, patch.dict(os.environ, {"FRONTEND_URL": "https://test-frontend.com"}):
        send_added_to_org("to_email", "adder@x.org", "AcmeOrg")

    _, subject, plain, html_body = mock_send_html.call_args[0]
    assert subject == "You have been added to AcmeOrg by adder@x.org"
    assert "You now have access to this Dalgo workspace." in plain
    assert "https://test-frontend.com" in plain
    assert "You have been added to AcmeOrg by adder@x.org" in html_body
    assert "Explore Workspace" in html_body


def test_send_added_to_org_group_wording():
    """Existing user added via group create/edit — names the group + inherit-access copy."""
    with patch(
        "ddpui.core.notifications.triggers.user.send_html_message"
    ) as mock_send_html, patch.dict(os.environ, {"FRONTEND_URL": "https://test-frontend.com"}):
        send_added_to_org("to_email", "adder@x.org", "AcmeOrg", group_name="Funders")

    _, subject, plain, html_body = mock_send_html.call_args[0]
    assert subject == "You have been added to the Funders group by adder@x.org"
    assert (
        "You now automatically inherit access to all resources shared with "
        "the Funders group." in plain
    )
    assert "You have been added to the Funders group by adder@x.org" in html_body
    assert "Explore Workspace" in html_body
