import os
from unittest.mock import patch

from ddpui.utils.awsses import (
    send_password_reset_email,
    send_signup_email,
    send_invite_user_email,
    send_youve_been_added_email,
)


def test_send_password_reset_email():
    """tests send_password_reset_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message:
        send_password_reset_email("to_email", "reset_url")
        message = """Hello,

We received a request to reset your Dalgo password.

Please click this link to begin: reset_url.

If you did not request a password reset you may safely ignore this email.

"""
        mock_send_text_message.assert_called_once_with(
            "to_email", "You've requested a password reset", message
        )


def test_send_signup_email():
    """tests send_signup_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message:
        send_signup_email("to_email", "verification_url")
        message = """Hello,

Welcome to Dalgo! Please verify your email address by clicking the link below

verification_url

    """
        mock_send_text_message.assert_called_once_with("to_email", "Welcome to Dalgo", message)


def test_send_invite_user_email():
    """tests send_invite_user_email sends a branded HTML invitation with a
    plain-text alternative, via send_html_message (not send_text_message)"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send_html_message:
        send_invite_user_email(
            "to_email",
            "invited_by_email",
            "invite_url",
            org_name="Test Org",
            role_name="Analyst",
            date_str="Jul 16, 2026",
        )
        mock_send_html_message.assert_called_once()
        args, _ = mock_send_html_message.call_args
        to_email, subject, plain_text, html_body = args

        assert to_email == "to_email"
        assert subject == "invited_by_email has invited you to join Dalgo"

        # plain-text alternative carries the same facts
        assert "invited_by_email" in plain_text
        assert "Test Org" in plain_text
        assert "Analyst" in plain_text
        assert "invite_url" in plain_text

        # HTML body carries role, org name, invite link and the CTA copy
        assert "invited_by_email" in html_body
        assert "Test Org" in html_body
        assert "Analyst" in html_body
        assert "invite_url" in html_body
        assert "Accept Invitation" in html_body


def test_send_youve_been_added_email():
    """tests send_youve_been_added_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message, patch.dict(
        os.environ, {"FRONTEND_URL": "https://test-frontend.com"}
    ):
        send_youve_been_added_email("to_email", "added_by", "org_name")
        message = """Hello,

You've been added to org_name by added_by.

Open your dashboard at https://test-frontend.com
    """
        mock_send_text_message.assert_called_once_with("to_email", "Added to Dalgo Org", message)
