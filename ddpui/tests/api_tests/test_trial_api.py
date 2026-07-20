"""Tests for the public free-trial endpoints (no authentication).

Tests:
1. POST /trial/signup — valid new email, existing account (409), invalid email (400)
"""

import os
from unittest.mock import patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ninja.errors import HttpError

from ddpui.api.trial_api import trial_signup, TrialSignupSchema

pytestmark = pytest.mark.django_db


class TestTrialSignup:
    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_valid_new_email(self, mock_exists, mock_create_token, mock_send_email):
        mock_exists.return_value = False
        mock_create_token.return_value = "tok123"

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="account-manager")
        result = trial_signup(None, payload)

        assert result == {"status": "verification_sent"}
        mock_create_token.assert_called_once_with("a@b.org", "Acme", "account-manager")
        mock_send_email.assert_called_once()
        args, _ = mock_send_email.call_args
        assert args[0] == "a@b.org"
        assert "tok123" in args[1]

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_existing_account_returns_409(self, mock_exists, mock_create_token, mock_send_email):
        mock_exists.return_value = True

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="account-manager")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 409
        mock_create_token.assert_not_called()
        mock_send_email.assert_not_called()

    def test_invalid_email_returns_400(self):
        payload = TrialSignupSchema(email="not-an-email", org_name="Acme", role="account-manager")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 400
