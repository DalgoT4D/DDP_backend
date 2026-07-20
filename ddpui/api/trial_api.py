"""Public free-trial signup/activation/status endpoints — no authentication required."""

import os
from uuid import uuid4

from django.conf import settings
from django.contrib.auth.models import User
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

    token = create_activation_token(payload.email, payload.org_name, payload.role)
    verify_url = f"{os.getenv('FRONTEND_URL_V2')}/free-trial/activate?token={token}"
    send_trial_verification_email(payload.email, verify_url)

    return {"status": "verification_sent"}


@trial_router.post("/activate")
def trial_activate(request, payload: TrialActivateSchema):  # pylint: disable=unused-argument
    """set the chosen password on the trial user and enqueue the clone task"""
    data = consume_activation_token(payload.token)
    if data is None:
        raise HttpError(400, "invalid or expired link")

    email = data["email"]
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
