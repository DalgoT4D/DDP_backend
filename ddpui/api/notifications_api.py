from ninja import NinjaAPI
from ninja.errors import ValidationError, HttpError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
)
from ddpui.models.org_user import OrgUser


notificationsapi = NinjaAPI(urls_namespace="notifications")


@notificationsapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@notificationsapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@notificationsapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"detail": "something went wrong"}, status=500)


@notificationsapi.post("/")
def create_notification(request, payload: CreateNotificationPayloadSchema):
    """Handle the task of creating a notification"""

    # Filter OrgUser data based on sent_to field
    error, recipients = notifications_service.get_recipients(
        payload.sent_to, payload.org_slug, payload.user_email, payload.manager_or_above
    )

    if error is not None:
        raise HttpError(400, error)

    notification_data = {
        "author": payload.author,
        "message": payload.message,
        "urgent": payload.urgent,
        "scheduled_time": payload.scheduled_time,
        "recipients": recipients,
    }

    error, result = notifications_service.create_notification(notification_data)

    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.get("/history")
def get_notification_history(request, page: int = 1, limit: int = 10):
    """
    Returns all the notifications including the
    past and the future scheduled notifications
    """
    error, result = notifications_service.get_notification_history(page, limit)
    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.get("/recipients")
def get_notification_recipients(request, notification_id: int):
    """
    Returns all the recipients for a notification
    """
    error, result = notifications_service.get_notification_recipients(notification_id)
    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.get("/", auth=auth.CustomAuthMiddleware())
def get_user_notifications(request, page: int = 1, limit: int = 10):
    """
    Returns all the notifications for a particular user.
    It returns only the past notifications,i.e,notifications
    which are already sent
    """
    orguser = request.orguser
    error, result = notifications_service.get_user_notifications(orguser, page, limit)
    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.put("/", auth=auth.CustomAuthMiddleware())
def mark_as_read(request, payload: UpdateReadStatusSchema):
    """
    Handles the task of updating the read_status
    of a notification to true or false
    """
    orguser: OrgUser = request.orguser
    error, result = notifications_service.mark_notification_as_read_or_unread(
        orguser.id, payload.notification_id, payload.read_status
    )
    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.delete("/")
def delete_notification(request, notification_id: int):
    """
    Used to delete past notifications,i.e, notifications
    which are already sent. Accepts notification_id in the
    payload and deletes it.
    """
    error, result = notifications_service.delete_scheduled_notification(notification_id)
    if error is not None:
        raise HttpError(400, error)

    return result


@notificationsapi.get("/unread_count", auth=auth.CustomAuthMiddleware())
def get_unread_notifications_count(request):
    """Get count of unread notifications"""
    orguser: OrgUser = request.orguser
    error, result = notifications_service.get_unread_notifications_count(orguser)
    if error is not None:
        raise HttpError(400, error)

    return result
