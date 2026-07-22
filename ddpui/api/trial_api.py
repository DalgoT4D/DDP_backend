"""Public free-trial signup/activation/status endpoints — no authentication required."""

import time
from uuid import uuid4

from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.core.trial.activation import (
    create_activation_token,
    consume_activation_token,
    store_clone_params,
    fetch_clone_params,
    acquire_clone_lock,
)
from ddpui.core.trial.clone_service import account_exists_for_email
from ddpui.core.trial.tasks import clone_trial_org_task
from ddpui.models.org import Org
from ddpui.models.org_user import UserAttributes
from ddpui.utils.awsses import send_trial_verification_email
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.taskprogress import TaskProgress

trial_router = Router()

# redis key holding the clone's start time (unix seconds), so the progress screen can show an
# elapsed clock that survives a page refresh instead of restarting from 0. Keyed by task_id; TTL
# comfortably outlives the ~90s clone so a slow/queued run still reports a correct elapsed.
TRIAL_START_KEY = "trial-clone-start:{task_id}"
TRIAL_START_TTL_SECONDS = 86400  # 1 day


class TrialSignupSchema(Schema):
    """payload for POST /trial/signup"""

    email: str
    org_name: str
    role: str


class TrialActivateSchema(Schema):
    """payload for POST /trial/activate"""

    token: str
    password: str


@trial_router.post("/signup")
def trial_signup(request, payload: TrialSignupSchema):  # pylint: disable=unused-argument
    """validate the email, email a verification link; no infra is created here"""
    try:
        validate_email(payload.email)
    except ValidationError as err:
        raise HttpError(400, "invalid email") from err

    if account_exists_for_email(payload.email):
        raise HttpError(409, "account already exists; please log in")

    # M3: fail fast if the frontend URL isn't configured, instead of emailing a malformed
    # verify link (before minting a token or sending anything).
    if not settings.FRONTEND_URL_V2:
        raise HttpError(500, "frontend url not configured")

    token = create_activation_token(payload.email, payload.org_name, payload.role)
    verify_url = f"{settings.FRONTEND_URL_V2}/free-trial/activate?token={token}"
    send_trial_verification_email(payload.email, verify_url)

    return {"status": "verification_sent"}


@trial_router.post("/activate")
def trial_activate(request, payload: TrialActivateSchema):  # pylint: disable=unused-argument
    """set the chosen password on the trial user and enqueue the clone task"""
    data = consume_activation_token(payload.token)
    if data is None:
        raise HttpError(400, "invalid or expired link")

    email = data["email"]

    # I1: re-check for a real account AFTER the token is consumed — the token may be old
    # (up to 24h TTL) and the email may have since become a real account (e.g. the user
    # signed up normally, or a previous activation already ran). Without this a replayed/
    # stale activation link could overwrite a real account's password.
    if account_exists_for_email(email):
        raise HttpError(409, "an account already exists for this email; please log in")

    # I2: per-email lifetime lock so two clones can't run for the same email at once. Acquired
    # here before enqueue; the clone task releases it in a finally when it ends (success or fail),
    # so a POST /trial/retry can re-run immediately after a failure. TTL is a dead-worker backstop.
    redis = RedisClient.get_instance()
    if not acquire_clone_lock(email):
        raise HttpError(409, "a trial is already being set up for this email")

    # I3: reject an empty/weak password before creating anything.
    try:
        validate_password(payload.password)
    except ValidationError as err:
        raise HttpError(400, "password does not meet requirements") from err

    # get_or_create doubles as the "dangling user" guard: a User may already exist here from
    # a previous failed/reaped trial attempt (teardown removes the OrgUser but not the User) —
    # either way we (re)set the chosen password ourselves rather than relying on the clone's
    # own get_or_create, which only sets a password when it creates the row.
    user, _ = User.objects.get_or_create(username=email, defaults={"email": email})
    user.set_password(payload.password)
    user.save()
    UserAttributes.objects.get_or_create(user=user, defaults={"email_verified": True})

    template = Org.objects.filter(slug=settings.TEMPLATE_ORG_SLUG).first()
    if template is None:
        raise HttpError(500, "template org not configured")

    task_id = str(uuid4())
    # record the clone's start time so the progress screen's elapsed clock is derived from a
    # fixed origin (survives page refresh) rather than counting up from when the tab mounted.
    redis.set(TRIAL_START_KEY.format(task_id=task_id), int(time.time()), ex=TRIAL_START_TTL_SECONDS)
    # stash the params so POST /trial/retry/{task_id} can re-enqueue after a failure without the
    # (now-consumed) activation token — no re-signup / re-verify / re-password on "Try again".
    store_clone_params(task_id, email, data["org_name"], data["role"], template.id)
    clone_trial_org_task.delay(task_id, template.id, email, data["org_name"], data["role"])

    # email is echoed back so the progress screen can auto-login (POST /login) once the clone
    # completes — the frontend never learns it any other way from this token-opened page.
    return {"task_id": task_id, "email": email}


@trial_router.post("/retry/{task_id}")
def trial_retry(request, task_id: str):  # pylint: disable=unused-argument
    """Re-run a failed trial clone from scratch, reusing the same task_id — no re-signup.

    Reachable only after a failure: the frontend shows "Try again" once /status reports "failed",
    which the backend writes only AFTER teardown has purged the failed run's resources and the
    task's finally has released the running-clone lock. This endpoint therefore starts from a
    clean slate (org/warehouse/workspace/dbt/viz gone) while the person — email, password, and
    verified state — was deliberately kept by teardown, so the clone re-runs with no user input.
    """
    data = fetch_clone_params(task_id)
    if data is None:
        # params gone (expired, or never a real task_id) — nothing to retry.
        raise HttpError(400, "no trial setup found to retry")

    email = data["email"]

    # a completed clone (or a normal signup in the meantime) is a real account now — don't
    # re-provision over it; send the user to log in. Keys on OrgUser, so a failed trial (User
    # kept, OrgUser removed by teardown) is NOT treated as an account and stays retryable.
    if account_exists_for_email(email):
        raise HttpError(409, "an account already exists for this email; please log in")

    # 409 if a clone is still running for this email — guards the timeout / double-click path
    # where "Try again" could fire while the original clone hasn't finished (and torn down) yet.
    if not acquire_clone_lock(email):
        raise HttpError(409, "your workspace is still being set up; please wait a moment")

    redis = RedisClient.get_instance()
    # re-anchor the elapsed clock to now so the progress screen counts from this retry, not the
    # original attempt.
    redis.set(TRIAL_START_KEY.format(task_id=task_id), int(time.time()), ex=TRIAL_START_TTL_SECONDS)
    # overwrite the failed run's progress history RIGHT NOW, not when the worker picks the task
    # up — otherwise /status keeps serving the old run's fully-advanced step list during the
    # enqueue→pickup gap and the progress screen flashes "all steps done" before resetting.
    progress = TaskProgress(task_id, f"trial-clone-{task_id}", 24 * 3600)
    progress.add({"message": "queued", "status": "queued"})
    clone_trial_org_task.delay(
        task_id, data["template_org_id"], email, data["org_name"], data["role"]
    )

    return {"task_id": task_id, "email": email}


@trial_router.get("/status/{task_id}")
def trial_status(request, task_id: str):  # pylint: disable=unused-argument
    """poll the redis-backed progress for a clone task"""
    redis = RedisClient.get_instance()
    # start time (unix seconds) recorded at activate; lets the frontend show an elapsed clock
    # anchored to a fixed origin so it doesn't reset when the user refreshes the progress page.
    raw_start = redis.get(TRIAL_START_KEY.format(task_id=task_id))
    started_at = int(raw_start) if raw_start else None

    progress = TaskProgress.fetch(task_id, f"trial-clone-{task_id}")
    if not progress:
        return {"task_id": task_id, "progress": [], "status": "pending", "started_at": started_at}

    last = progress[-1]
    result = {
        "task_id": task_id,
        "progress": progress,
        "status": last.get("status", "pending"),
        "started_at": started_at,
    }
    if "org_slug" in last:
        result["org_slug"] = last["org_slug"]
    return result
