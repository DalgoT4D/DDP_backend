from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
    UpdateReadStatusSchemav1,
)
from ddpui.models.org_user import OrgUser

notification_router = Router()


@notification_router.post("/")
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


@notification_router.get("/history")
def get_notification_history(request, page: int = 1, limit: int = 10, read_status: int = None):
    """
    Returns all the notifications including the
    past and the future scheduled notifications
    """
    error, result = notifications_service.get_notification_history(page, limit, read_status=None)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/recipients")
def get_notification_recipients(request, notification_id: int):
    """
    Returns all the recipients for a notification
    """
    error, result = notifications_service.get_notification_recipients(notification_id)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/", auth=auth.CustomAuthMiddleware(), deprecated=True)
def get_user_notifications(request, page: int = 1, limit: int = 10):
    """
    Returns all the notifications for a particular user.
    It returns only the past notifications,i.e,notifications
    which are already sent
    """
    orguser = request.orguser
    error, result = notifications_service.fetch_user_notifications(orguser, page, limit)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/v1", auth=auth.CustomAuthMiddleware())
def get_user_notifications_v1(request, page: int = 1, limit: int = 10, read_status: int = None):
    """
    Returns all the notifications for a particular user.
    It returns only the past notifications,i.e,notifications
    which are already sent
    """
    orguser = request.orguser
    error, result = notifications_service.fetch_user_notifications_v1(
        orguser, page, limit, read_status
    )
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.put("/", auth=auth.CustomAuthMiddleware(), deprecated=True)
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


@notification_router.put("/v1", auth=auth.CustomAuthMiddleware())
def mark_as_read_v1(request, payload: UpdateReadStatusSchemav1):
    """
    Bulk update of read status of notifications
    """
    orguser: OrgUser = request.orguser
    error, result = notifications_service.mark_notifications_as_read_or_unread(
        orguser.id, payload.notification_ids, payload.read_status
    )
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.delete("/")
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


@notification_router.get("/unread_count", auth=auth.CustomAuthMiddleware())
def get_unread_notifications_count(request):
    """Get count of unread notifications"""
    orguser: OrgUser = request.orguser
    error, result = notifications_service.get_unread_notifications_count(orguser)
    if error is not None:
        raise HttpError(400, error)

    return result
