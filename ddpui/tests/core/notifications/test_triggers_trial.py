"""Tests for the trial-lifecycle email triggers.

The renderers themselves have byte-golden tests in ``test_trial_templates.py``
in the same directory. These tests exercise the trigger wrappers — subject
line, delegation to the correct renderer, and the SES transport call.
"""

from unittest.mock import patch

from ddpui.core.notifications.triggers.trial import (
    send_completion,
    send_day3_in_progress,
    send_day3_not_started,
    send_midpoint,
    send_pre_end,
    send_verification,
    send_welcome,
)


def test_send_verification():
    """Verification email — branded HTML with plain-text fallback."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_verification("to_email", "verify_url")
        mock_send.assert_called_once()
        args, _ = mock_send.call_args
        assert args[0] == "to_email"
        assert "verify_url" in args[2]  # plain-text body
        assert "verify_url" in args[3]  # html body
        assert "#00897B" in args[3]  # Dalgo shell


def test_send_welcome():
    """Welcome email — branded HTML with plain-text fallback."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_welcome("to_email", "login_url")
        mock_send.assert_called_once()
        args, _ = mock_send.call_args
        assert args[0] == "to_email"
        assert "login_url" in args[2]
        assert "login_url" in args[3]
        assert "#00897B" in args[3]


def test_send_day3_not_started():
    """Day-3 nudge for a user with no walkthrough progress."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_day3_not_started("to@x.org", "https://app", "https://cal")
        assert mock_send.call_count == 1
        to_email, subject, text_body, html_body = mock_send.call_args[0]
        assert to_email == "to@x.org"
        assert subject == "Ready to see Dalgo in action?"
        assert "Ready to see Dalgo in action?" in text_body
        assert "OPEN WORKSPACE" in html_body


def test_send_day3_in_progress_passes_completed_flow_through():
    """The completed flow reaches the renderer, so the right row is ticked."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_day3_in_progress("to@x.org", "automate_pipeline", "https://app", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "Pick up where you left off"
        assert html_body.index("Setup an automated data pipeline") < html_body.index(
            "Build your first insight"
        )


def test_send_completion():
    """The completion email carries the workspace url."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_completion("to@x.org", "https://app", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You've completed your tour of Dalgo"
        assert "https://app" in html_body


def test_send_midpoint():
    """The midpoint email renders the day-of-total progress bar."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_midpoint("to@x.org", 7, 14, "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You're halfway through your Dalgo trial"
        assert "Day 7 of 14" in html_body


def test_send_pre_end():
    """The pre-end email shows the remaining days and the formatted end date."""
    with patch("ddpui.core.notifications.triggers.trial.send_html_message") as mock_send:
        send_pre_end("to@x.org", 12, 14, "15 Aug 2026", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "2 days left in your Dalgo trial"
        assert "15 Aug 2026" in html_body
