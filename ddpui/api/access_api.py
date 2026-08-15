"""HTTP surface for resource sharing (grants API).

Endpoints are thin wrappers around ``ddpui.core.access.resource_share`` —
this file owns authz gates, request-shape parsing, and HTTP error mapping.
"""

from django.conf import settings
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.access import resource_share, shareable_types
from ddpui.core.access.access_control import get_user_access
from ddpui.core.access.ownership import is_creator_or_admin, transfer_ownership, OwnershipError
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import AccessLevel, AccessRequest, AccessRequestStatus
from ddpui.schemas.access.resource_share_schema import (
    AccessRequestSchema,
    AddGrantsPayload,
    GrantsListResponse,
    PrivateTogglePayload,
    RequestAccessPayload,
    RespondToRequestPayload,
    ShareRowSchema,
    TransferOwnershipPayload,
    UpdateGrantPayload,
)
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.custom_logger import CustomLogger


access_router = Router()
logger = CustomLogger("ddpui")


def _fetch_resource_or_404(request, rtype: str, resource_id):
    """Common prelude: resolve org-scoped resource; 404 if missing."""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    try:
        resource = shareable_types.get_resource(orguser.org, rtype, resource_id)
    except ValueError as err:  # unknown rtype
        raise HttpError(400, str(err)) from err
    if resource is None:
        raise HttpError(404, f"{rtype} not found")
    return orguser, resource


def _require_edit_or_admin(orguser: OrgUser, rtype: str, resource, action: str) -> None:
    if is_creator_or_admin(orguser, resource):
        return
    if get_user_access(orguser, rtype, resource.pk) == AccessLevel.EDIT:
        return
    raise HttpError(403, f"edit access required to {action}")


def _resource_title(resource) -> str:
    """Best-effort human label for a resource across rtypes."""
    return getattr(resource, "title", None) or getattr(resource, "name", None) or "resource"


_RTYPE_URL_PATH = {
    "dashboard": "dashboards",
    "chart": "charts",
    "report": "reports",
}


def _resource_url(rtype: str, resource_id, *, open_share: bool = False) -> str:
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


def _notify_owner_of_new_request(req: AccessRequest, resource, rtype: str) -> None:
    """Notify the resource owner that a new access request has been submitted.
    No-op when the resource has no owner (legacy row with created_by=None)."""
    owner = getattr(resource, "created_by", None)
    if owner is None:
        return
    title = _resource_title(resource)
    requester_role = getattr(getattr(req.requester, "new_role", None), "name", None)
    requester_label = (
        f"{req.requester.user.email} ({requester_role})"
        if requester_role
        else req.requester.user.email
    )
    resource_url = _resource_url(rtype, resource.pk, open_share=True)
    body = (
        f"{requester_label} requested {req.requested_level} access to "
        f"{rtype} '{title}'.\n{resource_url}"
    )
    if req.note:
        body += f'\nNote: "{req.note}"'
    try:
        create_notification(
            NotificationDataSchema(
                author=req.requester.user.email,
                message=body,
                email_subject=f"Access request for {title}",
                urgent=False,
                recipients=[owner.id],
            )
        )
    except Exception as err:  # notification failure never blocks the API call
        logger.error(f"access-request notification failed: {err}")


def _notify_requester_of_response(
    req: AccessRequest,
    resource,
    rtype: str,
    responder: OrgUser,
    granted_level: str | None,
) -> None:
    """Notify the requester of the owner's decision on their access request."""
    title = _resource_title(resource)
    resource_url = _resource_url(rtype, resource.pk)
    if req.status == AccessRequestStatus.APPROVED:
        body = (
            f"{responder.user.email} approved your request for {rtype} '{title}' "
            f"at {granted_level} level.\n{resource_url}"
        )
        subject = f"Access request approved: {title}"
    else:
        body = (
            f"{responder.user.email} declined your request for {rtype} '{title}'.\n"
            f"{resource_url}"
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


# ---------------------------------------------------------------------------
# Grants


@access_router.get("/{rtype}/{resource_id}/grants", response=GrantsListResponse)
def list_resource_grants(request, rtype: str, resource_id: str):
    """List everyone with access to this resource."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "view sharing")
    shares = resource_share.list_grants(orguser.org, rtype, resource_id)
    caller_is_owner = getattr(resource, "created_by_id", None) == orguser.id
    return GrantsListResponse(shares=shares, caller_is_owner=caller_is_owner)


@access_router.post("/{rtype}/{resource_id}/grants", response=list[ShareRowSchema])
def add_resource_grants(request, rtype: str, resource_id: str, payload: AddGrantsPayload):
    """Apply staged chips from the share modal — bulk add/update."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "modify sharing")
    try:
        resource_share.add_grants(
            orguser,
            rtype,
            resource_id,
            payload.principals,
            payload.pending_grants,
            payload.invite_role_uuid,
        )
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err
    return resource_share.list_grants(orguser.org, rtype, resource_id)


@access_router.patch(
    "/{rtype}/{resource_id}/grants/{share_id}",
    response=list[ShareRowSchema],
)
def update_resource_grant(
    request, rtype: str, resource_id: str, share_id: int, payload: UpdateGrantPayload
):
    """Change the access level on one existing row."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "modify sharing")
    try:
        resource_share.update_grant(orguser, share_id, payload.access_level)
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err
    return resource_share.list_grants(orguser.org, rtype, resource_id)


@access_router.delete(
    "/{rtype}/{resource_id}/grants/{share_id}",
    response=list[ShareRowSchema],
)
def remove_resource_grant(request, rtype: str, resource_id: str, share_id: int):
    """Remove one share row (revoke access)."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "modify sharing")
    try:
        resource_share.remove_grant(orguser, share_id)
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err
    return resource_share.list_grants(orguser.org, rtype, resource_id)


# ---------------------------------------------------------------------------
# Private toggle


@access_router.patch("/{rtype}/{resource_id}/private")
def toggle_private(request, rtype: str, resource_id: str, payload: PrivateTogglePayload):
    """Mark a resource private (floor-based access bypassed) or public."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "change privacy")

    resource.is_private = payload.is_private
    update_fields = ["is_private"]

    if payload.is_private:
        if hasattr(resource, "is_public"):
            resource.is_public = False
            update_fields.append("is_public")
        if hasattr(resource, "public_share_token"):
            resource.public_share_token = None
            update_fields.append("public_share_token")

    resource.save(update_fields=update_fields)
    return {"is_private": resource.is_private}


# ---------------------------------------------------------------------------
# Ownership transfer


@access_router.post("/{rtype}/{resource_id}/transfer-ownership")
def transfer_resource_ownership(
    request, rtype: str, resource_id: str, payload: TransferOwnershipPayload
):
    """Transfer ownership of a resource to another org member."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    try:
        transfer_ownership(orguser, rtype, resource, payload.to_orguser_id)
    except OwnershipError as err:
        raise HttpError(400, str(err)) from err
    return {"success": True}


# ---------------------------------------------------------------------------
# Request access


def _access_request_to_schema(req: AccessRequest) -> AccessRequestSchema:
    return AccessRequestSchema(
        id=req.id,
        requester_id=req.requester_id,
        requester_email=req.requester.user.email,
        requested_level=req.requested_level,
        note=req.note,
        status=req.status,
        created_at=req.created_at.isoformat(),
    )


@access_router.post("/{rtype}/{resource_id}/request-access", response=AccessRequestSchema)
def create_access_request(
    request, rtype: str, resource_id: str, payload: RequestAccessPayload
):
    """Submit a request for access to a resource the caller cannot currently see."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)

    existing_access = get_user_access(orguser, rtype, resource.pk)
    if existing_access not in (None, AccessLevel.NO_ACCESS):
        raise HttpError(409, "you already have access to this resource")

    if AccessRequest.objects.filter(
        org=orguser.org,
        resource_type=rtype,
        resource_id=str(resource_id),
        requester=orguser,
        status=AccessRequestStatus.PENDING,
    ).exists():
        raise HttpError(409, "a pending request already exists for this resource")

    req = AccessRequest.objects.create(
        org=orguser.org,
        resource_type=rtype,
        resource_id=str(resource_id),
        requester=orguser,
        requested_level=payload.requested_level,
        note=payload.note,
    )
    req.refresh_from_db()
    req.requester = orguser
    _notify_owner_of_new_request(req, resource, rtype)
    return _access_request_to_schema(req)


@access_router.get("/{rtype}/{resource_id}/request-access", response=list[AccessRequestSchema])
def list_access_requests(request, rtype: str, resource_id: str):
    """List pending access requests for this resource. Requires Edit access."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "view access requests")

    requests = (
        AccessRequest.objects.filter(
            org=orguser.org,
            resource_type=rtype,
            resource_id=str(resource_id),
            status=AccessRequestStatus.PENDING,
        )
        .select_related("requester__user")
        .order_by("created_at")
    )
    return [_access_request_to_schema(r) for r in requests]


@access_router.post("/{rtype}/{resource_id}/request-access/{req_id}/respond")
def respond_to_access_request(
    request, rtype: str, resource_id: str, req_id: int, payload: RespondToRequestPayload
):
    """Approve or decline a pending access request."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "respond to access requests")

    req = (
        AccessRequest.objects.filter(
            id=req_id,
            org=orguser.org,
            resource_type=rtype,
            resource_id=str(resource_id),
        )
        .select_related("requester__user")
        .first()
    )
    if req is None:
        raise HttpError(404, "access request not found")
    if req.status != AccessRequestStatus.PENDING:
        raise HttpError(409, "request has already been decided")

    granted_level = None
    if payload.decision == "approved":
        granted_level = payload.granted_level or req.requested_level
        try:
            from ddpui.schemas.access.resource_share_schema import PrincipalGrantPayload
            from ddpui.models.resource_share import ResourceSharePrincipalType

            resource_share.add_grants(
                orguser,
                rtype,
                resource_id,
                principals=[
                    PrincipalGrantPayload(
                        principal_type=ResourceSharePrincipalType.USER,
                        principal_id=req.requester_id,
                        access_level=granted_level,
                    )
                ],
                pending_grants=[],
                invite_role_uuid=None,
            )
        except resource_share.GrantError as err:
            raise HttpError(400, str(err)) from err
        req.status = AccessRequestStatus.APPROVED
    else:
        req.status = AccessRequestStatus.DECLINED

    req.save(update_fields=["status", "updated_at"])
    _notify_requester_of_response(req, resource, rtype, orguser, granted_level)
    return {"success": True, "status": req.status}
