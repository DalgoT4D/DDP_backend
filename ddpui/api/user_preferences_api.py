from ninja import NinjaAPI
from ninja.errors import HttpError
from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth
from ddpui.models.notifications import UserPreference
from ddpui.schemas.user_preferences_schema import CreateUserPreferencesSchema, UpdateUserPreferencesSchema


userpreferencesapi = NinjaAPI(urls_namespace="user-preference")


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


@userpreferencesapi.post("/create/", auth=auth.CustomAuthMiddleware())
def create_user_preferences(request, payload: CreateUserPreferencesSchema):
    orguser = request.orguser
    
    preferences = UserPreference.objects.create(
        orguser=orguser,
        enable_discord_notifications=payload.enable_discord_notifications,
        enable_email_notifications=payload.enable_email_notifications,
        discord_webhook=payload.discord_webhook,
    )

    return {'success': True, 'message': 'Preferences created successfully', 'res': preferences}


@userpreferencesapi.put("/update/", auth=auth.CustomAuthMiddleware())
def update_user_preferences(request, payload: UpdateUserPreferencesSchema):
    orguser = request.orguser
    
    try:
        preferences = UserPreference.objects.get(orguser=orguser)
    except UserPreference.DoesNotExist:
        new_preferences = UserPreference.objects.create(
            orguser=orguser,
            enable_discord_notifications=False,
            enable_email_notifications=False,
            discord_webhook=None
        )
        return {'success': True, 'message': 'Preferences created successfully', 'res': new_preferences}
    
    if payload.enable_discord_notifications is not None:
        preferences.enable_discord_notifications = payload.enable_discord_notifications
    if payload.enable_email_notifications is not None:
        preferences.enable_email_notifications = payload.enable_email_notifications
    if payload.discord_webhook is not None:
        preferences.discord_webhook = payload.discord_webhook
    
    preferences.save()
    
    return {'success': True, 'message': 'Preferences updated successfully'}


@userpreferencesapi.get("/get/", auth=auth.CustomAuthMiddleware())
def get_user_preferences(request):
    orguser = request.orguser

    try:
        user_preference = UserPreference.objects.get(orguser=orguser)
        preferences = {
            'orguser': orguser.user,
            'discord_webhook': user_preference.discord_webhook,
            'enable_email_notifications': user_preference.enable_email_notifications,
            'enable_discord_notifications': user_preference.enable_discord_notifications,
            'email': user_preference.email
        }
        return {'status': 'success', 'preferences': preferences}
    
    except UserPreference.DoesNotExist:
        new_preferences = UserPreference.objects.create(
            orguser=orguser,
            enable_discord_notifications=False,
            enable_email_notifications=False,
            discord_webhook=None
        )
        return {'success': True, 'res': new_preferences}
    
    except Exception as e:
        raise HttpError(500, str(e))
