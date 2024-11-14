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

    user_preferences = UserPreferences.objects.create(
        orguser=orguser,
        enable_email_notifications=payload.enable_email_notifications,
    )

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.put("/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.enable_email_notifications is not None:
        user_preferences.enable_email_notifications = payload.enable_email_notifications
    user_preferences.save()

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.get("/", auth=auth.CustomAuthMiddleware())
def get_user_preferences(request):
    """gets user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    return {"success": True, "res": user_preferences.to_json()}
