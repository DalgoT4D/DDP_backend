from ninja import NinjaAPI
from ninja.errors import HttpError
from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
)
from ddpui.models.org_user import OrgUser


userpreferencesapi = NinjaAPI(urls_namespace="userpreference")


@userpreferencesapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@userpreferencesapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@userpreferencesapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"detail": "something went wrong"}, status=500)


@userpreferencesapi.post("/", auth=auth.CustomAuthMiddleware())
def create_user_preferences(request, payload: CreateUserPreferencesSchema):
    """creates user preferences for the user"""
    orguser: OrgUser = request.orguser

    if UserPreferences.objects.filter(orguser=orguser).exists():
        raise HttpError(400, "Preferences already exist")

    userpreferences = UserPreferences.objects.create(
        orguser=orguser,
        enable_discord_notifications=payload.enable_discord_notifications,
        enable_email_notifications=payload.enable_email_notifications,
        discord_webhook=payload.discord_webhook,
    )

    preferences = {
        "discord_webhook": userpreferences.discord_webhook,
        "enable_email_notifications": userpreferences.enable_email_notifications,
        "enable_discord_notifications": userpreferences.enable_discord_notifications,
    }

    return {"success": True, "res": preferences}


@userpreferencesapi.put("/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.enable_discord_notifications is not None:
        user_preferences.enable_discord_notifications = (
            payload.enable_discord_notifications
        )
    if payload.enable_email_notifications is not None:
        user_preferences.enable_email_notifications = payload.enable_email_notifications
    if payload.discord_webhook is not None:
        user_preferences.discord_webhook = payload.discord_webhook

    user_preferences.save()

    preferences = {
        "discord_webhook": user_preferences.discord_webhook,
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "enable_discord_notifications": user_preferences.enable_discord_notifications,
    }

    return {"success": True, "res": preferences}


@userpreferencesapi.get("/", auth=auth.CustomAuthMiddleware())
def get_user_preferences(request):
    """gets user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    preferences = {
        "discord_webhook": user_preferences.discord_webhook,
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "enable_discord_notifications": user_preferences.enable_discord_notifications,
    }
    return {"success": True, "res": preferences}
