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
        llm_optin=payload.llm_optin,
        enable_email_notifications=payload.enable_email_notifications,
    )

    preferences = {
        "enable_email_notifications": userpreferences.enable_email_notifications,
        "llm_optin": userpreferences.llm_optin,
    }

    return {"success": True, "res": preferences}


@userpreference_router.put("/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.llm_optin is not None:
        user_preferences.llm_optin = payload.llm_optin
    if payload.enable_email_notifications is not None:
        user_preferences.enable_email_notifications = payload.enable_email_notifications

    user_preferences.save()

    preferences = {
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "llm_optin": user_preferences.llm_optin,
    }

    return {"success": True, "res": preferences}


@userpreference_router.get("/", auth=auth.CustomAuthMiddleware())
def get_user_preferences(request):
    """gets user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    preferences = {
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "llm_optin": user_preferences.llm_optin,
    }
    return {"success": True, "res": preferences}
