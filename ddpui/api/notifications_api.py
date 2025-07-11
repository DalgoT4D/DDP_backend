from ninja import Router
from ninja.errors import HttpError
from ddpui import auth
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
    UpdateReadStatusSchemav1,
    UserNotificationPreferencesSchema,
    NotificationFiltersSchema,
    NotificationCategoryEnum,
    UserNotificationPreferencesResponseSchema,
    NotificationResponseSchema,
    UrgentNotificationSchema,
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

    Returns:
        dict: Created notification details including ID and delivery status

    Raises:
        HttpError: 400 if recipient filtering fails or notification creation fails
    """

    # Filter OrgUser data based on sent_to field
    error, recipients = notifications_service.get_recipients(
        payload.sent_to, payload.org_slug, payload.user_email, payload.manager_or_above
    )

    if error is not None:
        raise HttpError(400, error)

    notification_data = {
        "author": payload.author,
        "message": payload.message,
        "email_subject": "Notification from Dalgo",  # Default subject
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
def get_notification_history(request, page: int = 1, limit: int = 10, read_status: int = None):
    """
    Retrieve the complete notification history including past and scheduled notifications.

    Returns a paginated list of all notifications in the system, including those
    that have been sent and those scheduled for future delivery. Supports filtering
    by read status.

    Args:
        request: HTTP request object containing authentication data
        page (int, optional): Page number for pagination. Defaults to 1
        limit (int, optional): Number of notifications per page. Defaults to 10
        read_status (int, optional): Filter by read status (0=unread, 1=read).
                                   None returns all notifications

    Returns:
        dict: Paginated notification history with metadata

    Raises:
        HttpError: 400 if history retrieval fails
    """
    error, result = notifications_service.get_notification_history(page, limit, read_status=None)
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
def get_user_notifications_v1(request, page: int = 1, limit: int = 10, read_status: int = None):
    """
    Retrieve notifications for the authenticated user.

    Returns a paginated list of notifications that have been sent to the current
    user. Only includes past notifications that have already been delivered,
    not future scheduled notifications. Supports filtering by read status.

    Args:
        request: HTTP request object containing orguser authentication data
        page (int, optional): Page number for pagination. Defaults to 1
        limit (int, optional): Number of notifications per page. Defaults to 10
        read_status (int, optional): Filter by read status (0=unread, 1=read).
                                   None returns all notifications

    Returns:
        dict: Paginated user notifications with metadata

    Raises:
        HttpError: 400 if notification retrieval fails
    """
    orguser = request.orguser
    error, result = notifications_service.fetch_user_notifications_v1(
        orguser, page, limit, read_status
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


@notification_router.get("/categories")
def get_notification_categories(request):
    """
    Get all available notification categories.

    Returns a list of all notification categories that can be used
    for filtering and subscription management.

    Args:
        request: HTTP request object containing authentication data

    Returns:
        dict: List of available notification categories

    """
    categories = [
        {"value": category.value, "label": category.label} for category in NotificationCategoryEnum
    ]
    return {"success": True, "res": categories}


@notification_router.get("/preferences")
def get_user_notification_preferences(request) -> UserNotificationPreferencesResponseSchema:
    """
    Get notification preferences for the authenticated user.

    Returns the user's subscription preferences for different notification
    categories and email notification settings.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        UserNotificationPreferencesResponseSchema: User's notification preferences

    Raises:
        HttpError: 400 if preferences retrieval fails
    """
    orguser: OrgUser = request.orguser
    preferences = notifications_service.get_user_notification_preferences(orguser)

    return {
        "email_notifications_enabled": preferences.email_notifications_enabled,
        "incident_notifications": preferences.incident_notifications,
        "schema_change_notifications": preferences.schema_change_notifications,
        "job_failure_notifications": preferences.job_failure_notifications,
        "late_run_notifications": preferences.late_run_notifications,
        "dbt_test_failure_notifications": preferences.dbt_test_failure_notifications,
        "system_notifications": preferences.system_notifications,
    }


@notification_router.put("/preferences")
def update_user_notification_preferences(request, payload: UserNotificationPreferencesSchema):
    """
    Update notification preferences for the authenticated user.

    Allows users to subscribe/unsubscribe from different notification categories
    and control email notification delivery.

    Args:
        request: HTTP request object containing orguser authentication data
        payload (UserNotificationPreferencesSchema): Updated preference settings

    Returns:
        dict: Success message with updated preferences

    Raises:
        HttpError: 400 if preferences update fails
    """
    orguser: OrgUser = request.orguser

    updated_preferences = notifications_service.update_user_notification_preferences(
        orguser=orguser,
        email_notifications_enabled=payload.enable_email_notifications,
        incident_notifications=payload.subscribe_incident,
        schema_change_notifications=payload.subscribe_schema_change,
        job_failure_notifications=payload.subscribe_job_failure,
        late_run_notifications=payload.subscribe_late_run,
        dbt_test_failure_notifications=payload.subscribe_dbt_test_failure,
        system_notifications=payload.subscribe_system,
    )

    return {
        "success": True,
        "message": "Notification preferences updated successfully",
        "preferences": {
            "email_notifications_enabled": updated_preferences.email_notifications_enabled,
            "incident_notifications": updated_preferences.incident_notifications,
            "schema_change_notifications": updated_preferences.schema_change_notifications,
            "job_failure_notifications": updated_preferences.job_failure_notifications,
            "late_run_notifications": updated_preferences.late_run_notifications,
            "dbt_test_failure_notifications": updated_preferences.dbt_test_failure_notifications,
            "system_notifications": updated_preferences.system_notifications,
        },
    }


@notification_router.get("/filtered")
def get_filtered_notifications(
    request,
    page: int = 1,
    limit: int = 10,
    category: str = None,
    urgent_only: bool = False,
    read_status: int = None,
):
    """
    Get filtered notifications for the authenticated user.

    Returns notifications filtered by category, urgency, and read status.
    Supports pagination and provides comprehensive filtering options.

    Args:
        request: HTTP request object containing orguser authentication data
        page (int, optional): Page number for pagination. Defaults to 1
        limit (int, optional): Number of notifications per page. Defaults to 10
        category (str, optional): Filter by notification category
        urgent_only (bool, optional): Show only urgent notifications. Defaults to False
        read_status (int, optional): Filter by read status (0=unread, 1=read)

    Returns:
        dict: Filtered notifications with pagination metadata

    Raises:
        HttpError: 400 if filtering fails
    """
    orguser: OrgUser = request.orguser

    error, result = notifications_service.get_filtered_notifications(
        orguser=orguser,
        page=page,
        limit=limit,
        category=category,
        urgent_only=urgent_only,
        read_status=read_status,
    )

    if error is not None:
        raise HttpError(400, error)

    return result


@notification_router.get("/urgent-bar")
def get_urgent_notifications_bar(request):
    """
    Get urgent unread notifications for the notification bar.

    Returns urgent notifications that should be displayed prominently
    in the UI notification bar at the top of the page.

    Args:
        request: HTTP request object containing orguser authentication data

    Returns:
        dict: List of urgent notifications for display in notification bar

    Raises:
        HttpError: 400 if retrieval fails
    """
    orguser: OrgUser = request.orguser
    urgent_notifications = notifications_service.get_urgent_notifications_for_user(orguser)

    return {"success": True, "res": urgent_notifications}
