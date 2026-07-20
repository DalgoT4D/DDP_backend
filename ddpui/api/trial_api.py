"""Public free-trial signup/activation/status endpoints — no authentication required."""

import os

from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.core.trial.activation import create_activation_token
from ddpui.core.trial.clone_service import account_exists_for_email
from ddpui.utils.awsses import send_trial_verification_email
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.api.trial_api")

trial_router = Router()


class TrialSignupSchema(Schema):
    """payload for POST /trial/signup"""

    email: str
    org_name: str
    role: str


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
