from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
)
from ddpui.models.org_user import OrgUser

userpreference_router = Router()


@userpreference_router.post("/", auth=auth.CustomAuthMiddleware())
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


@userpreference_router.put("/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.enable_discord_notifications is not None:
        user_preferences.enable_discord_notifications = payload.enable_discord_notifications
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


@userpreference_router.get("/", auth=auth.CustomAuthMiddleware())
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
