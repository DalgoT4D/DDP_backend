"""Access-request notification triggers.

Two events fire from ``api/access_api.py``:
- Requester submits an access request → notify the resource owner.
- Owner approves or declines → notify the requester.

Both flow through ``create_notification`` — in-app row + generic templated
email via ``render_notification_email``. Failure is logged and swallowed so
the API call never fails on a mail hiccup.

Also owns the resource-title + resource-URL helpers used across the notification
triggers (``triggers/share.py`` imports them from here).
"""

from typing import Optional

from django.conf import settings

from ddpui.auth import ADMIN_ROLE
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import AccessRequest, AccessRequestStatus
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.access")


_RTYPE_URL_PATH = {
    "dashboard": "dashboards",
    "chart": "charts",
    "report": "reports",
}


def resource_title(resource) -> str:
    """Best-effort human label for a resource across rtypes."""
    return getattr(resource, "title", None) or getattr(resource, "name", None) or "resource"


def resource_url(rtype: str, resource_id, *, open_share: bool = False) -> str:
    """Frontend URL for a resource; ``open_share`` deep-links to the share modal.

    KPIs are a special case — there's no /kpis/{id} route, so the deep link
    points at the KPI list page with a ``kpiId`` query param that the list
    handles to open the share modal for that specific row.
    """
    frontend_url = (
        getattr(settings, "FRONTEND_URL_V2", None)
        or getattr(settings, "FRONTEND_URL", None)
        or "http://localhost:3000"
    ).rstrip("/")

    if rtype == "kpi":
        return (
            f"{frontend_url}/kpis?openShare=true&kpiId={resource_id}"
            if open_share
            else f"{frontend_url}/kpis"
        )

    path = _RTYPE_URL_PATH.get(rtype, rtype + "s")
    url = f"{frontend_url}/{path}/{resource_id}"
    if open_share:
        url += "?openShare=true"
    return url


def notify_owner_of_new_request(req: AccessRequest, resource, rtype: str) -> None:
    """Notify the resource owner + org admins that a new access request has been
    submitted. Admins are included as governance backup; super-admins are NOT
    (org-wide role, not org-day-to-day).

    No-op only when no recipient can be resolved (orphan resource + no admins)."""
    recipients: set[int] = set()
    owner = getattr(resource, "created_by", None)
    if owner is not None:
        recipients.add(owner.id)

    # Sender's org acts as the scope; the resource lives inside it.
    admin_ids = OrgUser.objects.filter(org=req.org, new_role__slug=ADMIN_ROLE).values_list(
        "id", flat=True
    )
    recipients.update(admin_ids)
    recipients.discard(req.requester_id)  # requester shouldn't notify themselves

    if not recipients:
        return

    title = resource_title(resource)
    requester_role = getattr(getattr(req.requester, "new_role", None), "name", None)
    requester_label = (
        f"{req.requester.user.email} ({requester_role})"
        if requester_role
        else req.requester.user.email
    )
    url = resource_url(rtype, resource.pk, open_share=True)
    note_clause = f' with note "{req.note}"' if req.note else ""
    body = (
        f"{requester_label} has requested to {req.requested_level} your {rtype}, "
        f"'{title}'{note_clause}. Visit {url} to review"
    )
    try:
        create_notification(
            NotificationDataSchema(
                author=req.requester.user.email,
                message=body,
                email_subject=f"Access request for {title}",
                urgent=False,
                recipients=list(recipients),
                cta_label="Share",
            )
        )
    except Exception as err:  # notification failure never blocks the API call
        logger.error(f"access-request notification failed: {err}")


def notify_requester_of_response(
    req: AccessRequest,
    resource,
    rtype: str,
    responder: OrgUser,
    granted_level: Optional[str],
) -> None:
    """Notify the requester of the owner's decision on their access request."""
    title = resource_title(resource)
    url = resource_url(rtype, resource.pk)
    if req.status == AccessRequestStatus.APPROVED:
        body = (
            f"{responder.user.email} approved your request for {rtype} '{title}' "
            f"at {granted_level} level.\n{url}"
        )
        subject = f"Access request approved: {title}"
    else:
        body = (
            f"{responder.user.email} declined your request for {req.requested_level} "
            f"access to {rtype} '{title}'.\n{url}"
        )
        subject = f"Access request declined: {title}"
    try:
        create_notification(
            NotificationDataSchema(
                author=responder.user.email,
                message=body,
                email_subject=subject,
                urgent=False,
                recipients=[req.requester_id],
            )
        )
    except Exception as err:
        logger.error(f"access-response notification failed: {err}")
