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
    trial_retry,
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


def _mock_redis(mock_redis_cls):
    """Wire a patched RedisClient so get_instance() returns a MagicMock. Returns that instance
    (used to assert the start-time key is set)."""
    mock_redis = MagicMock()
    mock_redis_cls.get_instance.return_value = mock_redis
    return mock_redis


class TestTrialActivate:
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.store_clone_params")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_valid_token_creates_user_and_enqueues(
        self,
        mock_consume,
        mock_exists,
        mock_redis_cls,
        mock_lock,
        mock_store,
        mock_clone_task,
        seed_template_org,
    ):
        mock_consume.return_value = {
            "email": "new@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        mock_lock.return_value = True
        mock_redis = _mock_redis(mock_redis_cls)

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        result = trial_activate(None, payload)

        assert "task_id" in result
        assert result["email"] == "new@b.org"  # echoed back for the progress screen's auto-login
        user = User.objects.get(username="new@b.org")
        assert user.check_password(STRONG_PASSWORD)
        # the lifetime running-clone lock is taken (released by the task in its finally)
        mock_lock.assert_called_once_with("new@b.org")
        # clone params stashed so POST /trial/retry can re-enqueue without the consumed token
        mock_store.assert_called_once_with(
            result["task_id"], "new@b.org", "Acme", "account-manager", seed_template_org.id
        )
        # start time recorded so the progress screen's elapsed clock survives a refresh
        start_calls = [
            c
            for c in mock_redis.set.call_args_list
            if c.args and c.args[0] == f"trial-clone-start:{result['task_id']}"
        ]
        assert len(start_calls) == 1
        assert isinstance(start_calls[0].args[1], int)
        assert start_calls[0].kwargs.get("ex") == 86400
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
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_concurrent_activation_is_locked_out(
        self, mock_consume, mock_exists, mock_lock, mock_clone_task
    ):
        """I2: a second concurrent activate for the same email must be rejected (409)."""
        mock_consume.return_value = {
            "email": "dup@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        mock_lock.return_value = False

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 409
        assert not User.objects.filter(username="dup@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_weak_password_returns_400_and_no_user_created(
        self, mock_consume, mock_exists, mock_redis_cls, mock_lock, mock_clone_task
    ):
        """I3: an empty/short password must be rejected before the user is created."""
        mock_consume.return_value = {
            "email": "weak@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        payload = TrialActivateSchema(token="tok123", password="short")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        assert not User.objects.filter(username="weak@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    def test_empty_password_returns_400_and_no_user_created(
        self, mock_consume, mock_exists, mock_redis_cls, mock_lock, mock_clone_task
    ):
        mock_consume.return_value = {
            "email": "empty@b.org",
            "org_name": "Acme",
            "role": "account-manager",
        }
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        payload = TrialActivateSchema(token="tok123", password="")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        assert not User.objects.filter(username="empty@b.org").exists()
        mock_clone_task.delay.assert_not_called()


class TestTrialRetry:
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.fetch_clone_params")
    def test_retry_reenqueues_same_task_id(
        self, mock_fetch, mock_exists, mock_lock, mock_redis_cls, mock_clone_task
    ):
        """Happy path: stored params re-drive the clone under the SAME task_id, no user input."""
        mock_fetch.return_value = {
            "email": "r@b.org",
            "org_name": "Acme",
            "role": "account-manager",
            "template_org_id": 42,
        }
        mock_exists.return_value = False
        mock_lock.return_value = True
        mock_redis = _mock_redis(mock_redis_cls)

        result = trial_retry(None, "task-9")

        assert result == {"task_id": "task-9", "email": "r@b.org"}
        mock_lock.assert_called_once_with("r@b.org")
        mock_clone_task.delay.assert_called_once_with(
            "task-9", 42, "r@b.org", "Acme", "account-manager"
        )
        # elapsed clock re-anchored to this retry
        start_calls = [
            c
            for c in mock_redis.set.call_args_list
            if c.args and c.args[0] == "trial-clone-start:task-9"
        ]
        assert len(start_calls) == 1

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.fetch_clone_params")
    def test_retry_unknown_task_returns_400(self, mock_fetch, mock_clone_task):
        """No stored params (expired / never a real task_id) → nothing to retry."""
        mock_fetch.return_value = None

        with pytest.raises(HttpError) as exc:
            trial_retry(None, "ghost")

        assert exc.value.status_code == 400
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.fetch_clone_params")
    def test_retry_existing_account_returns_409(
        self, mock_fetch, mock_exists, mock_lock, mock_clone_task
    ):
        """A completed clone (or a real signup since) is an account now → don't re-provision."""
        mock_fetch.return_value = {
            "email": "done@b.org",
            "org_name": "Acme",
            "role": "account-manager",
            "template_org_id": 42,
        }
        mock_exists.return_value = True

        with pytest.raises(HttpError) as exc:
            trial_retry(None, "task-done")

        assert exc.value.status_code == 409
        mock_lock.assert_not_called()  # bailed before taking the lock
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.fetch_clone_params")
    def test_retry_while_clone_running_returns_409(
        self, mock_fetch, mock_exists, mock_lock, mock_clone_task
    ):
        """Guards the timeout/double-click path: lock held → the first clone is still running,
        so refuse to start a second one for the same email."""
        mock_fetch.return_value = {
            "email": "busy@b.org",
            "org_name": "Acme",
            "role": "account-manager",
            "template_org_id": 42,
        }
        mock_exists.return_value = False
        mock_lock.return_value = False

        with pytest.raises(HttpError) as exc:
            trial_retry(None, "task-busy")

        assert exc.value.status_code == 409
        mock_clone_task.delay.assert_not_called()


class TestTrialStatus:
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.TaskProgress")
    def test_status_with_progress(self, mock_taskprogress_cls, mock_redis_cls):
        mock_taskprogress_cls.fetch.return_value = [
            {"message": "queued", "status": "queued"},
            {"message": "done", "status": "completed", "org_slug": "trial-abc"},
        ]
        mock_redis_cls.get_instance.return_value.get.return_value = b"1700000000"

        result = trial_status(None, "task-1")

        mock_taskprogress_cls.fetch.assert_called_once_with("task-1", "trial-clone-task-1")
        assert result["task_id"] == "task-1"
        assert result["status"] == "completed"
        assert result["org_slug"] == "trial-abc"
        assert len(result["progress"]) == 2
        # start time from redis is surfaced so the frontend elapsed clock survives a refresh
        assert result["started_at"] == 1700000000

    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.TaskProgress")
    def test_status_empty_progress_is_pending(self, mock_taskprogress_cls, mock_redis_cls):
        mock_taskprogress_cls.fetch.return_value = None
        mock_redis_cls.get_instance.return_value.get.return_value = None

        result = trial_status(None, "task-2")

        assert result == {
            "task_id": "task-2",
            "progress": [],
            "status": "pending",
            "started_at": None,
        }
