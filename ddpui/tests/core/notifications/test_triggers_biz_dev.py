"""Tests for the biz-dev / partnerships email triggers."""

import os
from unittest.mock import patch

from ddpui.core.notifications.triggers.biz_dev import (
    biz_dev_recipients,
    send_notification,
)


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": " a@x.org , ,b@x.org"})
def test_biz_dev_recipients_splits_and_drops_blanks():
    assert biz_dev_recipients() == ["a@x.org", "b@x.org"]


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": ""})
def test_biz_dev_recipients_empty_when_unset():
    assert biz_dev_recipients() == []


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": "a@x.org,b@x.org"})
def test_send_notification_mails_every_recipient():
    with patch("ddpui.core.notifications.triggers.biz_dev.send_text_message") as mock_send:
        send_notification("New org created: Acme", "body")

    assert [call[0][0] for call in mock_send.call_args_list] == ["a@x.org", "b@x.org"]
    mock_send.assert_any_call("a@x.org", "New org created: Acme", "body")


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": "a@x.org,b@x.org"})
def test_send_notification_continues_past_a_failing_recipient():
    """One bouncing address must not stop the rest, and must not raise at the call site."""
    with patch("ddpui.core.notifications.triggers.biz_dev.send_text_message") as mock_send:
        mock_send.side_effect = [Exception("bounced"), None]
        send_notification("subject", "body")

    assert mock_send.call_count == 2


@patch.dict(os.environ, {"BIZ_DEV_EMAILS": ""})
def test_send_notification_noops_when_unconfigured():
    with patch("ddpui.core.notifications.triggers.biz_dev.send_text_message") as mock_send:
        send_notification("subject", "body")

    mock_send.assert_not_called()
