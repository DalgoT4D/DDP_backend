"""Redis-backed activation tokens for the free-trial signup flow."""

import json
from uuid import uuid4

from ddpui.schemas.trial_schema import ActivationTokenData, TrialCloneParams
from ddpui.utils.redis_client import RedisClient

TOKEN_PREFIX = "trial-activation:"
TOKEN_TTL_SECONDS = 3600 * 24

# clone params stashed at /activate, keyed by task_id, so POST /trial/retry can re-enqueue the
# clone after a failure without the (already-consumed) activation token. TTL comfortably outlives
# the clone + any reasonable retry window.
CLONE_PARAMS_PREFIX = "trial-clone-params:"
CLONE_PARAMS_TTL_SECONDS = 3600 * 24

# per-email lifetime lock held for the duration of a running clone task (acquired by the enqueuing
# endpoint, released by the task in a finally). Stops two clones racing for the same email — the
# key guard against a timeout/double-click retry firing while the first clone is still running. The
# TTL is only a dead-worker backstop; the task releases it early on completion. Matches the task's
# hard time_limit so a wedged worker frees the email at roughly the same time Celery kills it.
CLONE_LOCK_PREFIX = "trial-clone-running:"
CLONE_LOCK_TTL_SECONDS = 360


def create_activation_token(data: ActivationTokenData) -> str:
    """Create and store an activation token, returning its hex value."""
    token = uuid4().hex
    redis = RedisClient.get_instance()
    key = f"{TOKEN_PREFIX}{token}"
    redis.set(key, json.dumps(data.model_dump()))
    redis.expire(key, TOKEN_TTL_SECONDS)
    return token


def peek_activation_token(token: str) -> ActivationTokenData | None:
    """Look up an activation token WITHOUT consuming it, returning its payload if valid.

    Used by /activate to run every validation (password strength, account-exists, template
    config) BEFORE the token is burned — a validation failure must leave the link usable for
    a corrected resubmit, not masquerade as "invalid or expired link"."""
    redis = RedisClient.get_instance()
    raw = redis.get(f"{TOKEN_PREFIX}{token}")
    if raw is None:
        return None
    return ActivationTokenData(**json.loads(raw))


def consume_activation_token(token: str) -> ActivationTokenData | None:
    """Look up and delete an activation token, returning its payload if valid.

    The delete's return value guards the double-POST race: two concurrent requests can both
    GET the payload, but only the one whose DELETE actually removed the key wins — the loser
    gets None, exactly as if the token were already used."""
    redis = RedisClient.get_instance()
    key = f"{TOKEN_PREFIX}{token}"
    raw = redis.get(key)
    if raw is None:
        return None
    if not redis.delete(key):
        return None
    return ActivationTokenData(**json.loads(raw))


def store_clone_params(task_id: str, params: TrialCloneParams) -> None:
    """Stash the params needed to (re)run a clone for this task_id, so POST /trial/retry can
    re-enqueue without the consumed activation token."""
    redis = RedisClient.get_instance()
    key = f"{CLONE_PARAMS_PREFIX}{task_id}"
    redis.set(key, json.dumps(params.model_dump()))
    redis.expire(key, CLONE_PARAMS_TTL_SECONDS)


def fetch_clone_params(task_id: str) -> TrialCloneParams | None:
    """Return the stored clone params for a task_id, or None if missing/expired. Not deleted —
    a retry may itself fail and be retried again."""
    redis = RedisClient.get_instance()
    raw = redis.get(f"{CLONE_PARAMS_PREFIX}{task_id}")
    if raw is None:
        return None
    return TrialCloneParams(**json.loads(raw))


def acquire_clone_lock(email: str) -> bool:
    """Try to take the per-email running-clone lock. True if taken, False if a clone is already
    running for this email (SET NX). TTL is a dead-worker backstop; release_clone_lock frees it
    early on task completion."""
    redis = RedisClient.get_instance()
    return bool(redis.set(f"{CLONE_LOCK_PREFIX}{email}", "1", nx=True, ex=CLONE_LOCK_TTL_SECONDS))


def release_clone_lock(email: str) -> None:
    """Release the per-email running-clone lock (idempotent — safe if already expired/absent)."""
    redis = RedisClient.get_instance()
    redis.delete(f"{CLONE_LOCK_PREFIX}{email}")
