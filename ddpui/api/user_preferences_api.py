from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
)
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.notifications_service import create_notification
from ddpui.auth import has_permission
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema

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
        disclaimer_shown=payload.disclaimer_shown,
    )

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.put("/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.enable_email_notifications is not None:
        user_preferences.enable_email_notifications = payload.enable_email_notifications
    if payload.disclaimer_shown is not None:
        user_preferences.disclaimer_shown = payload.disclaimer_shown
    user_preferences.save()

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.get("/", auth=auth.CustomAuthMiddleware())
def get_user_preferences(request):
    """gets user preferences for the user"""
    orguser: OrgUser = request.orguser
    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)
    org_preferences, created = OrgPreferences.objects.get_or_create(org=orguser.org)

    res = {
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "disclaimer_shown": user_preferences.disclaimer_shown,
        "is_llm_active": org_preferences.llm_optin,
    }
    return {"success": True, "res": res}


@userpreference_router.post("/llm_analysis/request", auth=auth.CustomAuthMiddleware())
@has_permission(["can_request_llm_analysis_feature"])
def post_request_llm_analysis_feature_enabled(request):
    """Sends a notification to org's account manager for enabling LLM analysis feature"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    # get the account managers of the org
    acc_managers: list[OrgUser] = OrgUser.objects.filter(
        org=org, new_role__slug=ACCOUNT_MANAGER_ROLE
    ).all()

    if len(acc_managers) == 0:
        raise HttpError(400, "No account manager found for the organization")

    # send notification to all account managers
    notification_data = NotificationDataSchema(
        author=orguser.user.email,
        message=f"{orguser.user.first_name} is requesting to enable LLM analysis feature",
        urgent=False,
        scheduled_time=None,
        recipients=[acc_manager.id for acc_manager in acc_managers],
    )

    create_notification(notification_data)

    return {"success": True, "res": "Notified account manager(s) to enable LLM analysis feature"}
