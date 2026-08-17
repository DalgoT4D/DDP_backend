import os
from unittest.mock import patch

from ddpui.utils.awsses import (
    send_password_reset_email,
    send_signup_email,
    send_invite_user_email,
    send_youve_been_added_email,
    send_trial_verification_email,
    send_trial_welcome_email,
    send_trial_day3_not_started_email,
    send_trial_day3_in_progress_email,
    send_trial_completion_email,
    send_trial_midpoint_email,
    send_trial_pre_end_email,
    biz_dev_recipients,
    send_biz_dev_notification,
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
    with patch("ddpui.utils.awsses.send_text_message") as mock_send_text_message, patch.dict(
        os.environ, {"FRONTEND_URL": "https://test-frontend.com"}
    ):
        send_youve_been_added_email("to_email", "added_by", "org_name")
        message = """Hello,

You've been added to org_name by added_by.

Open your dashboard at https://test-frontend.com
    """
        mock_send_text_message.assert_called_once_with("to_email", "Added to Dalgo Org", message)


def test_send_trial_verification_email():
    """tests send_trial_verification_email — sends branded HTML with plain-text fallback"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send_html_message:
        send_trial_verification_email("to_email", "verify_url")
        mock_send_html_message.assert_called_once()
        args, _ = mock_send_html_message.call_args
        assert args[0] == "to_email"
        assert "verify_url" in args[2]  # plain-text body
        assert "verify_url" in args[3]  # html body
        assert "#00897B" in args[3]  # Dalgo shell


def test_send_trial_welcome_email():
    """tests send_trial_welcome_email — sends branded HTML with plain-text fallback"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send_html_message:
        send_trial_welcome_email("to_email", "login_url")
        mock_send_html_message.assert_called_once()
        args, _ = mock_send_html_message.call_args
        assert args[0] == "to_email"
        assert "login_url" in args[2]  # plain-text body
        assert "login_url" in args[3]  # html body
        assert "#00897B" in args[3]  # Dalgo shell


def test_send_trial_day3_not_started_email():
    """sends the html+text pair with the day-3 not-started subject"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_day3_not_started_email("to@x.org", "https://app", "https://cal")
        assert mock_send.call_count == 1
        to_email, subject, text_body, html_body = mock_send.call_args[0]
        assert to_email == "to@x.org"
        assert subject == "Ready to see Dalgo in action?"
        assert "Ready to see Dalgo in action?" in text_body
        assert "OPEN WORKSPACE" in html_body


def test_send_trial_day3_in_progress_email_passes_completed_flow_through():
    """the completed flow reaches the renderer, so the right row is ticked"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_day3_in_progress_email(
            "to@x.org", "automate_pipeline", "https://app", "https://cal"
        )
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "Pick up where you left off"
        assert html_body.index("Setup an automated data pipeline") < html_body.index(
            "Build your first insight"
        )


def test_send_trial_completion_email():
    """the completion email carries the workspace url"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_completion_email("to@x.org", "https://app", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You've completed your tour of Dalgo"
        assert "https://app" in html_body


def test_send_trial_midpoint_email():
    """the midpoint email renders the day-of-total progress bar"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_midpoint_email("to@x.org", 7, 14, "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You're halfway through your Dalgo trial"
        assert "Day 7 of 14" in html_body


def test_send_trial_pre_end_email():
    """the pre-end email shows the remaining days and the formatted end date"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_pre_end_email("to@x.org", 12, 14, "15 Aug 2026", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "2 days left in your Dalgo trial"
        assert "15 Aug 2026" in html_body


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": " a@x.org , ,b@x.org"})
def test_biz_dev_recipients_splits_and_drops_blanks():
    assert biz_dev_recipients() == ["a@x.org", "b@x.org"]


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": ""})
def test_biz_dev_recipients_empty_when_unset():
    assert biz_dev_recipients() == []


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": "a@x.org,b@x.org"})
def test_send_biz_dev_notification_mails_every_recipient():
    with patch("ddpui.utils.awsses.send_text_message") as mock_send:
        send_biz_dev_notification("New org created: Acme", "body")

    assert [call[0][0] for call in mock_send.call_args_list] == ["a@x.org", "b@x.org"]
    mock_send.assert_any_call("a@x.org", "New org created: Acme", "body")


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": "a@x.org,b@x.org"})
def test_send_biz_dev_notification_continues_past_a_failing_recipient():
    """one bouncing address must not stop the rest, and must not raise at the call site"""
    with patch("ddpui.utils.awsses.send_text_message") as mock_send:
        mock_send.side_effect = [Exception("bounced"), None]
        send_biz_dev_notification("subject", "body")

    assert mock_send.call_count == 2


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": ""})
def test_send_biz_dev_notification_noops_when_unconfigured():
    with patch("ddpui.utils.awsses.send_text_message") as mock_send:
        send_biz_dev_notification("subject", "body")

    mock_send.assert_not_called()
