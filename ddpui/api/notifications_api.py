from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
    UpdateReadStatusSchemav1,
    CategorySubscriptionSchema,
)
from ddpui.models.org_user import OrgUser

notification_router = Router()


@notification_router.post("/")
def post_create_notification(request, payload: CreateNotificationPayloadSchema):
    """
    Create a new notification to be sent to specified recipients.

    Creates a notification that can be sent immediately or scheduled for later.
    Recipients are determined based on the sent_to criteria, organization slug,
    and user roles within the organization.

    Args:
        request: HTTP request object containing authentication data
        payload (CreateNotificationPayloadSchema): Notification data including:
            - author: Notification sender
            - message: Notification content
            - urgent: Priority flag
            - scheduled_time: When to send (optional, immediate if not provided)
            - sent_to: Recipient criteria
            - org_slug: Target organization
            - user_email: Specific user email (optional)
            - manager_or_above: Whether to include managers (optional)
            - category: Notification category (optional, defaults to INCIDENT)

    Returns:
        dict: Created notification details including ID and delivery status

    Raises:
        HttpError: 400 if recipient filtering fails or notification creation fails
    """

    # Filter OrgUser data based on sent_to field
    error, recipients = notifications_service.get_recipients(
        payload.sent_to,
        payload.org_slug,
        payload.user_email,
        payload.manager_or_above,
        payload.category,
    )

    if error is not None:
        raise HttpError(400, error)

    notification_data = {
        "author": payload.author,
        "message": payload.message,
        "email_subject": payload.message,  # Use message as email subject if not provided
        "urgent": payload.urgent,
        "scheduled_time": payload.scheduled_time,
        "recipients": recipients,
        "category": payload.category,
    }

    error, result = notifications_service.create_notification(notification_data)

    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/history")
def get_notification_history(
    request, page: int = 1, limit: int = 10, read_status: int = None, category: str = None
):
    """
    Retrieve the complete notification history including past and scheduled notifications.

    Returns a paginated list of all notifications in the system, including those
    that have been sent and those scheduled for future delivery. Supports filtering
    by read status and category.

    Args:
        request: HTTP request object containing authentication data
        page (int, optional): Page number for pagination. Defaults to 1
        limit (int, optional): Number of notifications per page. Defaults to 10
        read_status (int, optional): Filter by read status (0=unread, 1=read).
                                   None returns all notifications
        category (str, optional): Filter by notification category

    Returns:
        dict: Paginated notification history with metadata

    Raises:
        HttpError: 400 if history retrieval fails
    """
    error, result = notifications_service.get_notification_history(
        page, limit, read_status, category
    )
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/recipients")
def get_notification_recipients(request, notification_id: int):
    """
    Retrieve all recipients for a specific notification.

    Returns the complete list of users who received or will receive a particular
    notification, including their user details and delivery status.

    Args:
        request: HTTP request object containing authentication data
        notification_id (int): Unique identifier of the notification

    Returns:
        dict: List of recipients with their user details and delivery status

    Raises:
        HttpError: 400 if notification not found or recipient retrieval fails
    """
    error, result = notifications_service.get_notification_recipients(notification_id)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/v1")
def get_user_notifications_v1(
    request, page: int = 1, limit: int = 10, read_status: int = None, category: str = None
):
    """
    Retrieve notifications for the authenticated user.

    Returns a paginated list of notifications that have been sent to the current
    user. Only includes past notifications that have already been delivered,
    not future scheduled notifications. Supports filtering by read status and category.

    Args:
        request: HTTP request object containing orguser authentication data
        page (int, optional): Page number for pagination. Defaults to 1
        limit (int, optional): Number of notifications per page. Defaults to 10
        read_status (int, optional): Filter by read status (0=unread, 1=read).
                                   None returns all notifications
        category (str, optional): Filter by notification category

    Returns:
        dict: Paginated user notifications with metadata

    Raises:
        HttpError: 400 if notification retrieval fails
    """
    orguser = request.orguser
    error, result = notifications_service.fetch_user_notifications_v1(
        orguser, page, limit, read_status, category
    )
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.put("/v1")
def mark_as_read_v1(request, payload: UpdateReadStatusSchemav1):
    """
    Bulk update the read status of multiple notifications.

    Allows marking multiple notifications as read or unread in a single operation.
    Only notifications belonging to the authenticated user can be updated.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (UpdateReadStatusSchemav1): Update data including:
            - notification_ids: List of notification IDs to update
            - read_status: New read status (0=unread, 1=read)

    Returns:
        dict: Update operation result including number of notifications affected

    Raises:
        HttpError: 400 if status update fails or user lacks permission
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
    Delete a scheduled notification before it is sent.

    Removes a notification that was scheduled for future delivery but has not
    yet been sent. Only works for scheduled notifications, not for notifications
    that have already been delivered.

    Args:
        request: HTTP request object containing authentication data
        notification_id (int): Unique identifier of the notification to delete

    Returns:
        dict: Deletion operation result

    Raises:
        HttpError: 400 if notification not found, already sent, or deletion fails
    """
    error, result = notifications_service.delete_scheduled_notification(notification_id)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/unread_count")
def get_unread_notifications_count(request):
    """
    Get the count of unread notifications for the authenticated user.

    Returns the total number of notifications that the current user has
    received but not yet marked as read. Useful for displaying notification
    badges in the UI.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        dict: Count of unread notifications for the user

    Raises:
        HttpError: 400 if count retrieval fails
    """
    orguser: OrgUser = request.orguser
    error, result = notifications_service.get_unread_notifications_count(orguser)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/urgent")
def get_urgent_notifications(request):
    """
    Get urgent notifications that haven't been dismissed by the user.

    Returns urgent notifications that should be displayed in the prominent
    notification bar at the top of the page.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        dict: List of urgent notifications that haven't been dismissed

    Raises:
        HttpError: 400 if retrieval fails
    """
    orguser: OrgUser = request.orguser
    error, result = notifications_service.get_urgent_notifications(orguser)
    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.put("/mark_all_as_read")
def mark_all_notifications_as_read(request):
    """Mark all notifications as read for the user"""
    orguser: OrgUser = request.orguser
    error, result = notifications_service.mark_all_notifications_as_read(orguser.id)
    if error is not None:
        raise HttpError(400, error)

    return result
