from ninja import NinjaAPI
from ninja.errors import ValidationError, HttpError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth
from ddpui.core import notifications_service
from ddpui.schemas.notifications_api_schemas import (
    CreateNotificationPayloadSchema,
    UpdateReadStatusSchema,
    DeleteNotificationSchema,
    SentToEnum,
    CreateNotificationSchema,
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
    recipients = []

    # send to all users
    if payload.sent_to == SentToEnum.ALL_USERS:
        recipients = OrgUser.objects.all().values_list("user_id", flat=True)

    # send to all users in an org
    elif payload.sent_to == SentToEnum.ALL_ORG_USERS:
        if payload.org_slug:
            recipients = OrgUser.objects.filter(org__slug=payload.org_slug).values_list(
                "user_id", flat=True
            )
            if not recipients:
                raise HttpError(400, "No users found with the provided org_slug")
        else:
            raise HttpError(
                400, "org_slug is required to sent notification to all org users."
            )

    # send to a single user
    elif payload.sent_to == SentToEnum.SINGLE_USER:
        if payload.user_email:
            try:
                recipient = OrgUser.objects.get(user__email=payload.user_email)
                recipients = [recipient.user_id]
            except OrgUser.DoesNotExist:
                raise HttpError(400, "User with the provided email does not exist")
        else:
            raise HttpError(
                400, "user email is required to sent notification to a user."
            )

    # role based filtering
    if payload.manager_or_above and payload.sent_to != SentToEnum.SINGLE_USER:
        recipients = OrgUser.objects.filter(
            new_role_id__lte=3, user_id__in=recipients
        ).values_list("user_id", flat=True)
        if not recipients:
            raise HttpError(400, "No users found for the given information")

    payload_dict = {
        "author": payload.author,
        "message": payload.message,
        "urgent": payload.urgent,
        "scheduled_time": payload.scheduled_time,
        "recipients": list(recipients),
    }

    error, result = notifications_service.create_notification(
        CreateNotificationSchema(**payload_dict)
    )

    if error is not None:
        raise HttpError(400, error)

    return Response(result)


@notificationsapi.get("/history")
def get_notification_history(request):
    """
    Returns all the notifications including the
    past and the future scheduled notifications
    """
    error, result = notifications_service.get_notification_history()
    if error is not None:
        raise HttpError(400, error)

    return Response(result)


@notificationsapi.get("/", auth=auth.CustomAuthMiddleware())
def get_user_notifications(request):
    """
    Returns all the notifications for a particular user.
    It returns only the past notifications,i.e,notifications
    which are already sent
    """
    orguser = request.orguser
    error, result = notifications_service.get_user_notifications(orguser)
    if error is not None:
        raise HttpError(400, error)

    return Response(result)


@notificationsapi.put("/", auth=auth.CustomAuthMiddleware())
def mark_as_read(request, payload: UpdateReadStatusSchema):
    """
    Handles the task of updating the read_status
    of a notification to true or false
    """
    orguser: OrgUser = request.orguser
    error, result = notifications_service.mark_notification_as_read_or_unread(
        orguser.user_id, payload.notification_id, payload.read_status
    )
    if error is not None:
        raise HttpError(400, error)

    return Response(result)


@notificationsapi.delete("/")
def delete_notification(request, payload: DeleteNotificationSchema):
    """
    Used to delete past notifications,i.e, notifications
    which are already sent. Accepts notification_id in the
    payload and deletes it.
    """
    error, result = notifications_service.delete_scheduled_notification(
        payload.notification_id
    )
    if error is not None:
        raise HttpError(400, error)

    return Response(result)
