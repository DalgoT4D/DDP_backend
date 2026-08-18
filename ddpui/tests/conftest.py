"""Suite-wide test guards."""

import os
from unittest.mock import MagicMock

import pytest

# Set to a file path to record every test that reaches an email path (debugging aid only).
_SES_PROBE = os.getenv("DDPUI_TEST_SES_PROBE")


@pytest.fixture(autouse=True)
def _never_send_real_email(monkeypatch, request):
    """Stub the SES client for EVERY test, always.

    Real credentials (SES_ACCESS_KEY_ID / SES_SECRET_ACCESS_KEY / SES_SENDER_EMAIL) are present
    in a normal .env, and `awsses.send_text_message` calls `_get_ses_client()` unconditionally —
    so any test reaching an email path actually delivers mail to real people. That has happened:
    the trial welcome email and the teardown/cleanup ops alerts both fire from paths several
    tests exercise, and `send_trial_ops_alert` swallows its own errors, so nothing even failed
    to reveal it.

    Autouse and suite-wide on purpose — an opt-in guard is one forgotten @patch away from mailing
    someone. Tests that assert on email content should still patch the specific `awsses.send_*`
    function; this only guarantees the floor.
    """

    def _stub():
        if _SES_PROBE:
            with open(_SES_PROBE, "a", encoding="utf-8") as fh:
                fh.write(request.node.nodeid + "\n")
        return MagicMock()

    monkeypatch.setattr("ddpui.utils.awsses._get_ses_client", _stub)
    yield
