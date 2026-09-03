from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.userpreferences_schema import (
    CreateUserPreferencesSchema,
    UpdateUserPreferencesSchema,
    UpdateTrialWalkthroughSchema,
    TrialWalkthroughFlowState,
)
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.auth import has_permission
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema

userpreference_router = Router()


@userpreference_router.post("/")
def create_user_preferences(request, payload: CreateUserPreferencesSchema):
    """creates user preferences for the user"""
    orguser: OrgUser = request.orguser

    if UserPreferences.objects.filter(orguser=orguser).exists():
        raise HttpError(400, "Preferences already exist")

    user_preferences = UserPreferences.objects.create(
        orguser=orguser,
        enable_email_notifications=payload.enable_email_notifications,
        disclaimer_shown=payload.disclaimer_shown,
        last_visited_transform_tab=payload.last_visited_transform_tab,
    )

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.put("/")
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    """Updates user preferences for the user"""
    orguser: OrgUser = request.orguser

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    if payload.enable_email_notifications is not None:
        user_preferences.enable_email_notifications = payload.enable_email_notifications
    if payload.enable_schema_change_notifications is not None:
        user_preferences.enable_schema_change_notifications = (
            payload.enable_schema_change_notifications
        )
    if payload.disclaimer_shown is not None:
        user_preferences.disclaimer_shown = payload.disclaimer_shown
    if payload.last_visited_transform_tab is not None:
        user_preferences.last_visited_transform_tab = payload.last_visited_transform_tab
    user_preferences.save()

    return {"success": True, "res": user_preferences.to_json()}


@userpreference_router.get("/")
def get_user_preferences(request):
    """gets user preferences for the user"""
    orguser: OrgUser = request.orguser
    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)
    org_preferences, created = OrgPreferences.objects.get_or_create(org=orguser.org)

    res = {
        "enable_email_notifications": user_preferences.enable_email_notifications,
        "enable_schema_change_notifications": user_preferences.enable_schema_change_notifications,
        "disclaimer_shown": user_preferences.disclaimer_shown,
        "last_visited_transform_tab": user_preferences.last_visited_transform_tab,
        "is_llm_active": org_preferences.llm_optin,
        "enable_llm_requested": org_preferences.enable_llm_request,
        "trial_walkthrough": user_preferences.trial_walkthrough,
    }
    return {"success": True, "res": res}


@userpreference_router.put("/trial-walkthrough")
def update_trial_walkthrough(request, payload: UpdateTrialWalkthroughSchema):
    """Marks one trial-walkthrough flow (product_tour/insights/automate_pipeline) skipped
    or completed. Merges into the existing dict so updating one flow never clobbers the
    other two, and enforces skipped/completed as mutually exclusive."""
    orguser: OrgUser = request.orguser

    # Rejects an all-falsy payload, not just an all-None one. `{"completed": false}` passes a
    # `is None` check but matches neither branch below, so it used to return 200 having written
    # nothing — a silent no-op the caller reads as success. There is no "un-complete" or
    # "un-skip" operation: a flow only ever moves forward, so the only meaningful request sets
    # one of these to true.
    if not payload.completed and not payload.skipped:
        raise HttpError(400, "Set either skipped or completed to true")

    user_preferences, created = UserPreferences.objects.get_or_create(orguser=orguser)

    # Each write replaces the WHOLE flow object rather than patching one key, which is what
    # keeps skipped/completed mutually exclusive: completing a flow skipped earlier clears the
    # stale `skipped` in the same write. `completed` is checked first so a caller sending both
    # resolves to completed rather than storing a contradiction.
    walkthrough = dict(user_preferences.trial_walkthrough or {})
    walkthrough[payload.flow] = TrialWalkthroughFlowState(
        skipped=not payload.completed, completed=bool(payload.completed)
    ).model_dump()

    user_preferences.trial_walkthrough = walkthrough
    user_preferences.save()

    return {"success": True, "res": walkthrough}


@userpreference_router.post("/llm_analysis/request")
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
        message=f"{orguser.user.email} is requesting to enable LLM analysis feature",
        email_subject=f"{org.name}: Request to enable LLM analysis feature",
        urgent=False,
        scheduled_time=None,
        recipients=[acc_manager.id for acc_manager in acc_managers],
    )

    error, res = create_notification(notification_data)
    if res and "errors" in res and len(res["errors"]) > 0:
        raise HttpError(400, "Issue with creating the request notification")

    rows_updated = OrgPreferences.objects.filter(org=org).update(
        enable_llm_request=True, enable_llm_requested_by=orguser
    )
    if rows_updated == 0:
        raise HttpError(
            400, "No rows were updated. OrgPreferences may not exist for this organization."
        )

    return {"success": True, "res": "Notified account manager(s) to enable LLM analysis feature"}
