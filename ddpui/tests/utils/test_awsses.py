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
    """tests send_invite_user_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message:
        send_invite_user_email("to_email", "invited_by_email", "invite_url")
        message = """Hello,

Welcome to Dalgo.

You have been invited by invited_by_email

Click here to accept: invite_url

    """
        mock_send_text_message.assert_called_once_with("to_email", "Welcome to Dalgo", message)


def test_send_youve_been_added_email():
    """tests send_youve_been_added_email"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message:
        send_youve_been_added_email("to_email", "added_by", "org_name")
        url = os.getenv("FRONTEND_URL")
        message = f"""Hello,

You've been added to org_name by added_by.

Open your dashboard at {url}
    """
        mock_send_text_message.assert_called_once_with("to_email", "Welcome to Dalgo", message)
