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
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.trial_signup import TrialSignup
from ddpui.api.trial_api import (
    trial_signup,
    trial_activate,
    trial_retry,
    trial_status,
    trial_validate_password,
)
from ddpui.schemas.trial_schema import (
    ActivationTokenData,
    TrialActivateSchema,
    TrialCloneParams,
    TrialSignupSchema,
    TrialValidatePasswordSchema,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def seed_template_org(monkeypatch):
    """Org that stands in for the TEMPLATE_ORG_SLUG-configured template org."""
    monkeypatch.setattr(settings, "TEMPLATE_ORG_SLUG", "trial-template")
    # pinned so the signup path doesn't fall through to the ambient env: CI never sets
    # FRONTEND_URL_V2, which would trip the fail-fast guard in trial_signup and 500.
    monkeypatch.setattr(settings, "FRONTEND_URL_V2", "http://localhost:3000")
    return Org.objects.create(name="Trial Template", slug="trial-template")


class TestTrialSignup:
    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_valid_new_email(
        self, mock_exists, mock_create_token, mock_send_email, seed_template_org
    ):
        mock_exists.return_value = False
        mock_create_token.return_value = "tok123"

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
        result = trial_signup(None, payload)

        assert result == {"status": "verification_sent"}
        mock_create_token.assert_called_once_with(
            ActivationTokenData(email="a@b.org", org_name="Acme", role="data_technology")
        )
        mock_send_email.assert_called_once()
        args, _ = mock_send_email.call_args
        assert args[0] == "a@b.org"
        assert "tok123" in args[1]

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_existing_account_returns_409(self, mock_exists, mock_create_token, mock_send_email):
        mock_exists.return_value = True

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 409
        mock_create_token.assert_not_called()
        mock_send_email.assert_not_called()

    def test_invalid_email_returns_400(self):
        payload = TrialSignupSchema(email="not-an-email", org_name="Acme", role="data_technology")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 400

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_signup_opens_the_durable_record(
        self, mock_exists, mock_create_token, mock_send_email, seed_template_org
    ):
        """The TrialSignup row is written here — before verification — because it is the only
        trace of this person that survives the day-14 delete."""
        mock_exists.return_value = False

        trial_signup(
            None, TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
        )

        record = TrialSignup.objects.get(email="a@b.org")
        assert record.org_name == "Acme"
        assert record.role == "data_technology"
        assert record.signed_up_at is not None
        assert record.trial_start_date is None
        assert record.deleted_at is None

    @patch("ddpui.api.trial_api.record_signup")
    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_signup_survives_a_record_failure(
        self, mock_exists, mock_create_token, mock_send_email, mock_record, seed_template_org
    ):
        """Bookkeeping must never cost a real user their trial."""
        mock_exists.return_value = False
        mock_record.side_effect = Exception("db down")

        result = trial_signup(
            None, TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
        )

        assert result == {"status": "verification_sent"}
        mock_send_email.assert_called_once()

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_invalid_email_writes_no_record(self, mock_exists, mock_create_token, mock_send_email):
        with pytest.raises(HttpError):
            trial_signup(
                None,
                TrialSignupSchema(email="not-an-email", org_name="Acme", role="data_technology"),
            )

        assert TrialSignup.objects.count() == 0

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_missing_frontend_url_returns_500(
        self, mock_exists, mock_create_token, mock_send_email, monkeypatch
    ):
        """M3: a misconfigured FRONTEND_URL_V2 must fail fast, before minting a token / emailing."""
        mock_exists.return_value = False
        monkeypatch.setattr(settings, "FRONTEND_URL_V2", "")

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
        with pytest.raises(HttpError) as exc:
            trial_signup(None, payload)

        assert exc.value.status_code == 500
        mock_create_token.assert_not_called()
        mock_send_email.assert_not_called()

    @patch("ddpui.api.trial_api.send_trial_verification_email")
    @patch("ddpui.api.trial_api.create_activation_token")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    def test_missing_template_org_returns_500(
        self, mock_exists, mock_create_token, mock_send_email, monkeypatch
    ):
        """A missing template org must fail fast at signup — before minting a token / emailing —
        not surface later at /activate after the user has chosen a password."""
        mock_exists.return_value = False
        monkeypatch.setattr(settings, "TEMPLATE_ORG_SLUG", "nonexistent-template")

        payload = TrialSignupSchema(email="a@b.org", org_name="Acme", role="data_technology")
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


def _token_data(email="new@b.org"):
    return ActivationTokenData(email=email, org_name="Acme", role="data_technology")


class TestTrialActivate:
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.store_clone_params")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_valid_token_creates_user_and_enqueues(
        self,
        mock_peek,
        mock_consume,
        mock_exists,
        mock_redis_cls,
        mock_lock,
        mock_store,
        mock_clone_task,
        seed_template_org,
    ):
        mock_peek.return_value = _token_data()
        mock_consume.return_value = _token_data()
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
            result["task_id"],
            TrialCloneParams(
                email="new@b.org",
                org_name="Acme",
                role="data_technology",
                template_org_id=seed_template_org.id,
            ),
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
            result["task_id"], seed_template_org.id, "new@b.org", "Acme", "data_technology"
        )

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.store_clone_params")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_activate_marks_tnc_accepted_on_the_signup_record(
        self,
        mock_peek,
        mock_consume,
        mock_exists,
        mock_redis_cls,
        mock_lock,
        mock_store,
        mock_clone_task,
        seed_template_org,
    ):
        """This endpoint IS the consent screen's "Accept and Continue" — it must record that."""
        record = TrialSignup.objects.create(email="new@b.org", signed_up_at=timezone.now())
        mock_peek.return_value = _token_data()
        mock_consume.return_value = _token_data()
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        trial_activate(None, TrialActivateSchema(token="tok123", password=STRONG_PASSWORD))

        record.refresh_from_db()
        assert record.tnc_accepted is True
        # accepting is not a started trial — clone_template_org stamps that on success
        assert record.trial_start_date is None

    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_tnc_acceptance_survives_a_failed_activation(
        self, mock_peek, mock_consume, mock_exists, mock_redis_cls, mock_lock
    ):
        """The acceptance happened even though no account got created — the row stays open with
        tnc_accepted True, so follow-up mail can still reach this person."""
        record = TrialSignup.objects.create(email="weak@b.org", signed_up_at=timezone.now())
        mock_peek.return_value = _token_data("weak@b.org")
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        with pytest.raises(HttpError) as exc:
            trial_activate(None, TrialActivateSchema(token="tok123", password="short"))

        assert exc.value.status_code == 400
        record.refresh_from_db()
        assert record.tnc_accepted is True
        assert record.deleted_at is None

    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_invalid_token_returns_400(self, mock_peek):
        mock_peek.return_value = None

        payload = TrialActivateSchema(token="bad", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_existing_account_returns_409_and_no_mutation(
        self, mock_peek, mock_consume, mock_exists, mock_clone_task
    ):
        """I1: activate must check account-exists before touching the User — and the token
        must NOT be burned by the rejection."""
        mock_peek.return_value = _token_data("existing@b.org")
        mock_exists.return_value = True

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 409
        assert not User.objects.filter(username="existing@b.org").exists()
        mock_consume.assert_not_called()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_concurrent_activation_is_locked_out(
        self, mock_peek, mock_consume, mock_exists, mock_lock, mock_clone_task, seed_template_org
    ):
        """I2: a second concurrent activate for the same email must be rejected (409)."""
        mock_peek.return_value = _token_data("dup@b.org")
        mock_consume.return_value = _token_data("dup@b.org")
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
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_weak_password_returns_400_and_token_not_burned(
        self, mock_peek, mock_consume, mock_exists, mock_redis_cls, mock_lock, mock_clone_task
    ):
        """I3: a weak password must be rejected before the user is created — and must NOT
        consume the token or take the lock, so a corrected resubmit of the same link works."""
        mock_peek.return_value = _token_data("weak@b.org")
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        payload = TrialActivateSchema(token="tok123", password="short")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        # the prefix is a contract: the frontend matches on it to tell this 400 apart from the
        # expired-token 400, which shares the status code. Django's reason follows it.
        assert str(exc.value).startswith("password does not meet requirements:")
        assert "too short" in str(exc.value)
        assert not User.objects.filter(username="weak@b.org").exists()
        mock_consume.assert_not_called()
        mock_lock.assert_not_called()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_empty_password_returns_400_and_no_user_created(
        self, mock_peek, mock_consume, mock_exists, mock_redis_cls, mock_lock, mock_clone_task
    ):
        mock_peek.return_value = _token_data("empty@b.org")
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)

        payload = TrialActivateSchema(token="tok123", password="")
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        assert not User.objects.filter(username="empty@b.org").exists()
        mock_consume.assert_not_called()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_missing_template_returns_500_without_side_effects(
        self, mock_peek, mock_consume, mock_exists, mock_lock, mock_clone_task, monkeypatch
    ):
        """The staging incident: TEMPLATE_ORG_SLUG unset/missing must 500 BEFORE consuming the
        token, taking the lock, or creating the user — so once ops fixes the config, the very
        same activation link works. Previously the token was burned and the lock leaked."""
        mock_peek.return_value = _token_data("tmpl@b.org")
        mock_exists.return_value = False
        monkeypatch.setattr(settings, "TEMPLATE_ORG_SLUG", "nonexistent-template")

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 500
        mock_consume.assert_not_called()
        mock_lock.assert_not_called()
        assert not User.objects.filter(username="tmpl@b.org").exists()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_double_post_loser_returns_400(
        self, mock_peek, mock_consume, mock_exists, mock_lock, mock_clone_task, seed_template_org
    ):
        """Double-POST race: both requests peek successfully, but the consume loser (delete
        returned 0) must 400 without taking the lock or enqueuing a second clone."""
        mock_peek.return_value = _token_data("race@b.org")
        mock_exists.return_value = False
        mock_consume.return_value = None  # lost the atomic-delete race

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(HttpError) as exc:
            trial_activate(None, payload)

        assert exc.value.status_code == 400
        mock_lock.assert_not_called()
        mock_clone_task.delay.assert_not_called()

    @patch("ddpui.api.trial_api.release_clone_lock")
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.store_clone_params")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.consume_activation_token")
    @patch("ddpui.api.trial_api.peek_activation_token")
    def test_enqueue_failure_releases_lock(
        self,
        mock_peek,
        mock_consume,
        mock_exists,
        mock_redis_cls,
        mock_lock,
        mock_store,
        mock_clone_task,
        mock_release,
        seed_template_org,
    ):
        """If anything past lock-acquisition raises (here: broker down at .delay), the lock
        must be released — otherwise every retry 409s 'already being set up' until the TTL."""
        mock_peek.return_value = _token_data("boom@b.org")
        mock_consume.return_value = _token_data("boom@b.org")
        mock_exists.return_value = False
        mock_lock.return_value = True
        _mock_redis(mock_redis_cls)
        mock_clone_task.delay.side_effect = RuntimeError("broker down")

        payload = TrialActivateSchema(token="tok123", password=STRONG_PASSWORD)
        with pytest.raises(RuntimeError):
            trial_activate(None, payload)

        mock_release.assert_called_once_with("boom@b.org")


class TestTrialRetry:
    @patch("ddpui.api.trial_api.TaskProgress")
    @patch("ddpui.api.trial_api.clone_trial_org_task")
    @patch("ddpui.api.trial_api.RedisClient")
    @patch("ddpui.api.trial_api.acquire_clone_lock")
    @patch("ddpui.api.trial_api.account_exists_for_email")
    @patch("ddpui.api.trial_api.fetch_clone_params")
    def test_retry_reenqueues_same_task_id(
        self, mock_fetch, mock_exists, mock_lock, mock_redis_cls, mock_clone_task, mock_progress_cls
    ):
        """Happy path: stored params re-drive the clone under the SAME task_id, no user input."""
        mock_fetch.return_value = TrialCloneParams(
            email="r@b.org",
            org_name="Acme",
            role="data_technology",
            template_org_id=42,
        )
        mock_exists.return_value = False
        mock_lock.return_value = True
        mock_redis = _mock_redis(mock_redis_cls)

        result = trial_retry(None, "task-9")

        assert result == {"task_id": "task-9", "email": "r@b.org"}
        mock_lock.assert_called_once_with("r@b.org")
        mock_clone_task.delay.assert_called_once_with(
            "task-9", 42, "r@b.org", "Acme", "data_technology"
        )
        # elapsed clock re-anchored to this retry
        start_calls = [
            c
            for c in mock_redis.set.call_args_list
            if c.args and c.args[0] == "trial-clone-start:task-9"
        ]
        assert len(start_calls) == 1
        # the failed run's progress history is overwritten AT retry time (not when the worker
        # starts) so /status never serves the old fully-advanced step list during the
        # enqueue→pickup gap.
        mock_progress_cls.assert_called_once_with("task-9", "trial-clone-task-9", 24 * 3600)
        mock_progress_cls.return_value.add.assert_called_once_with(
            {"message": "queued", "status": "queued"}
        )

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
        mock_fetch.return_value = TrialCloneParams(
            email="done@b.org",
            org_name="Acme",
            role="data_technology",
            template_org_id=42,
        )
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
        mock_fetch.return_value = TrialCloneParams(
            email="busy@b.org",
            org_name="Acme",
            role="data_technology",
            template_org_id=42,
        )
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


class TestTrialValidatePassword:
    """POST /trial/validate-password — the pre-flight check the activate screen calls so a
    weak password is reported on the password form instead of one screen later, as an
    /activate 400 the client can't tell apart from an expired link."""

    def test_strong_password_is_valid(self):
        result = trial_validate_password(
            None, TrialValidatePasswordSchema(password=STRONG_PASSWORD)
        )
        assert result == {"valid": True}

    def test_short_password_rejected_with_django_reason(self):
        with pytest.raises(HttpError) as exc:
            trial_validate_password(None, TrialValidatePasswordSchema(password="Ab1!x"))
        assert exc.value.status_code == 400
        assert "too short" in str(exc.value)

    def test_common_password_rejected_with_django_reason(self):
        """the rule the frontend cannot mirror without shipping Django's 20k-word list —
        the whole reason this endpoint exists."""
        with pytest.raises(HttpError) as exc:
            trial_validate_password(None, TrialValidatePasswordSchema(password="password123"))
        assert exc.value.status_code == 400
        assert "too common" in str(exc.value)

    def test_all_numeric_password_rejected(self):
        with pytest.raises(HttpError) as exc:
            trial_validate_password(None, TrialValidatePasswordSchema(password="4831067295"))
        assert exc.value.status_code == 400
        assert "entirely numeric" in str(exc.value)

    def test_empty_password_rejected(self):
        with pytest.raises(HttpError) as exc:
            trial_validate_password(None, TrialValidatePasswordSchema(password=""))
        assert exc.value.status_code == 400

    def test_creates_no_user(self):
        """it validates a string and nothing else — no state, no account, nothing to enumerate."""
        before = User.objects.count()
        with pytest.raises(HttpError):
            trial_validate_password(None, TrialValidatePasswordSchema(password="password"))
        assert User.objects.count() == before
