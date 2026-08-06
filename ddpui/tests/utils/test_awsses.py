import os
from unittest.mock import patch

import pytest

from ddpui.utils.awsses import (
    send_password_reset_email,
    send_signup_email,
    send_invite_user_email,
    send_youve_been_added_email,
    send_text_message,
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
    """tests send_invite_user_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message:
        send_invite_user_email("to_email", "invited_by_email", "invite_url")
        message = """Hello,

Welcome to Dalgo.

You have been invited by invited_by_email

Click here to accept: invite_url

    """
        mock_send_text_message.assert_called_once_with("to_email", "Welcome to Dalgo", message)


# ---- local-dev SES fallback (settings.DEBUG=True AND no SES creds) -------------


def test_send_text_message_dev_fallback_logs_instead_of_raising():
    """
    LOCAL-DEV FALLBACK: with DEBUG=True and SES credentials absent, send_text_message
    logs the email and returns None WITHOUT touching the real SES client — so invite /
    signup / reset flows complete locally with no SES setup and never raise.
    """
    with patch("ddpui.utils.awsses.settings.DEBUG", True), patch(
        "ddpui.utils.awsses._ses_available", return_value=False
    ), patch("ddpui.utils.awsses._get_ses_client") as mock_get_ses_client, patch(
        "ddpui.utils.awsses.logger"
    ) as mock_logger:
        result = send_text_message("to@example.com", "Subject", "Body")

    assert result is None
    mock_get_ses_client.assert_not_called()  # the real SES path was never entered
    mock_logger.info.assert_called_once()  # the email was logged instead


def test_send_text_message_does_not_mask_misconfig_outside_dev():
    """
    NEVER-MASK GUARANTEE: with DEBUG=False (staging/prod), even when SES credentials
    are absent the fallback does NOT engage — send_text_message goes to the real SES
    client, so a genuine misconfiguration raises loudly instead of being swallowed.
    """
    with patch("ddpui.utils.awsses.settings.DEBUG", False), patch(
        "ddpui.utils.awsses._ses_available", return_value=False
    ), patch("ddpui.utils.awsses._get_ses_client") as mock_get_ses_client:
        mock_get_ses_client.side_effect = ValueError("Missing SES AWS credentials")
        with pytest.raises(ValueError, match="Missing SES AWS credentials"):
            send_text_message("to@example.com", "Subject", "Body")

    mock_get_ses_client.assert_called_once()  # the real SES path WAS entered (not masked)


def test_send_text_message_uses_real_ses_when_configured_in_dev():
    """
    DEV WITH REAL CREDS: with DEBUG=True but SES credentials present, the fallback is
    skipped and the real SES client is used — a dev who configures SES still exercises
    the real send path.
    """
    with patch("ddpui.utils.awsses.settings.DEBUG", True), patch(
        "ddpui.utils.awsses._ses_available", return_value=True
    ), patch("ddpui.utils.awsses._get_ses_client") as mock_get_ses_client, patch(
        "ddpui.utils.awsses.os.getenv", return_value="sender@dalgo.org"
    ):
        send_text_message("to@example.com", "Subject", "Body")

    mock_get_ses_client.assert_called_once()
    mock_get_ses_client.return_value.send_email.assert_called_once()


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
