"""Public free-trial signup/activation/status endpoints — no authentication required."""

from uuid import uuid4

from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.core.trial.activation import create_activation_token, consume_activation_token
from ddpui.core.trial.clone_service import account_exists_for_email
from ddpui.core.trial.tasks import clone_trial_org_task
from ddpui.models.org import Org
from ddpui.models.org_user import UserAttributes
from ddpui.utils.awsses import send_trial_verification_email
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.api.trial_api")

trial_router = Router()


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

    # I2: short-lived per-email lock so two concurrent activations for the same email can't
    # both provision a trial. Not released on success — it naturally expires in 10 minutes,
    # and by then the account-exists guard above covers any repeat.
    redis = RedisClient.get_instance()
    lock_key = f"trial-activating:{email}"
    if not redis.set(lock_key, "1", nx=True, ex=600):
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
    clone_trial_org_task.delay(task_id, template.id, email, data["org_name"], data["role"])

    return {"task_id": task_id}


@trial_router.get("/status/{task_id}")
def trial_status(request, task_id: str):  # pylint: disable=unused-argument
    """poll the redis-backed progress for a clone task"""
    progress = TaskProgress.fetch(task_id, f"trial-clone-{task_id}")
    if not progress:
        return {"task_id": task_id, "progress": [], "status": "pending"}

    last = progress[-1]
    result = {
        "task_id": task_id,
        "progress": progress,
        "status": last.get("status", "pending"),
    }
    if "org_slug" in last:
        result["org_slug"] = last["org_slug"]
    return result
