"""Tests for the public free-trial endpoints (no authentication).

Tests:
1. POST /trial/signup — valid new email, existing account (409), invalid email (400)
2. POST /trial/activate — valid token, invalid/used token (400)
3. GET /trial/status/{task_id} — progress present, empty/None progress
"""

import os
from unittest.mock import patch, MagicMock

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ninja.errors import HttpError
from django.contrib.auth.models import User
from django.conf import settings

from ddpui.models.org import Org
from ddpui.api.trial_api import (
    trial_signup,
    trial_activate,
    trial_status,
    TrialSignupSchema,
    TrialActivateSchema,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def seed_template_org(monkeypatch):
    """Org that stands in for the TEMPLATE_ORG_SLUG-configured template org."""
    monkeypatch.setattr(settings, "TEMPLATE_ORG_SLUG", "trial-template")
    return Org.objects.create(name="Trial Template", slug="trial-template")


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

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_missing_frontend_url_returns_500(
        self, mock_exists, mock_create_token, mock_send_email, monkeypatch
    ):
        """M3: a misconfigured FRONTEND_URL_V2 must fail fast, before minting a token / emailing."""
        mock_exists.return_value = False
        monkeypatch.setattr(settings, "FRONTEND_URL_V2", "")

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="account-manager")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 500
        mock_create_token.assert_not_called()
        mock_send_email.assert_not_called()


STRONG_PASSWORD = "Str0ngTr!alPassw0rd"


def _configure_redis_lock(mock_redis_cls, acquired=True):
    """Configure a patched RedisClient mock so RedisClient.get_instance().set(nx=True) simulates
    lock acquisition succeeding (acquired=True) or the lock already being held by a concurrent
    activation (acquired=False). Returns the underlying redis-instance mock.
    """
    mock_redis = MagicMock()
    mock_redis.set.return_value = acquired
    mock_redis_cls.get_instance.return_value = mock_redis
    return mock_redis


class TestTrialActivate:
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_valid_token_creates_user_and_enqueues(
        self, mock_consume, mock_exists, mock_redis_cls, mock_clone_task, seed_template_org
    ):
        mock_consume.return_value = {
            "email": "new@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        mock_redis = _configure_redis_lock(mock_redis_cls, acquired=True)

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        result = trial_activate(None, payload)

        assert "task_id" in result
        user = User.objects.get(username="new@b.org")
        assert user.check_password(STRONG_PASSWORD)
        mock_redis.set.assert_called_once_with("trial-activating:new@b.org", "1", nx=True, ex=600)
        mock_clone_task.delay.assert_called_once_with(
            result["task_id"], seed_template_org.id, "new@b.org", "Acme", "account-manager"
        )

    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_invalid_token_returns_400(self, mock_consume):
        mock_consume.return_value = None

        payload = TrialActivateSchema(token="bad", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_existing_account_returns_409_and_no_mutation(
        self, mock_consume, mock_exists, mock_clone_task
    ):
        """I1: activate must re-check account-exists before touching the User."""
        mock_consume.return_value = {
            "email": "existing@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = True

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 409
        assert not User.objects.filter(username="existing@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_concurrent_activation_is_locked_out(
        self, mock_consume, mock_exists, mock_redis_cls, mock_clone_task
    ):
        """I2: a second concurrent activate for the same email must be rejected (409)."""
        mock_consume.return_value = {
            "email": "dup@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        _configure_redis_lock(mock_redis_cls, acquired=False)

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 409
        assert not User.objects.filter(username="dup@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_weak_password_returns_400_and_no_user_created(
        self, mock_consume, mock_exists, mock_redis_cls, mock_clone_task
    ):
        """I3: an empty/short password must be rejected before the user is created."""
        mock_consume.return_value = {
            "email": "weak@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        _configure_redis_lock(mock_redis_cls, acquired=True)

        payload = TrialActivateSchema(token="tok123", password="short")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        assert not User.objects.filter(username="weak@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_empty_password_returns_400_and_no_user_created(
        self, mock_consume, mock_exists, mock_redis_cls, mock_clone_task
    ):
        mock_consume.return_value = {
            "email": "empty@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        _configure_redis_lock(mock_redis_cls, acquired=True)

        payload = TrialActivateSchema(token="tok123", password="")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        assert not User.objects.filter(username="empty@b.org").exists()
        mock_clone_task.delay.assert_not_called()


class TestTrialStatus:
    @patch("ddpui.api.trial_api.TaskProgress")
    def test_status_with_progress(self, mock_taskprogress_cls):
        mock_taskprogress_cls.fetch.return_value = [
            {"message": "queued", "status": "queued"},
            {"message": "done", "status": "completed", "org_slug": "trial-abc"},
        ]

        result = trial_status(None, "task-1")

        mock_taskprogress_cls.fetch.assert_called_once_with("task-1", "trial-clone-task-1")
        assert result["task_id"] == "task-1"
        assert result["status"] == "completed"
        assert result["org_slug"] == "trial-abc"
        assert len(result["progress"]) == 2

    @patch("ddpui.api.trial_api.TaskProgress")
    def test_status_empty_progress_is_pending(self, mock_taskprogress_cls):
        mock_taskprogress_cls.fetch.return_value = None

        result = trial_status(None, "task-2")

        assert result == {"task_id": "task-2", "progress": [], "status": "pending"}
