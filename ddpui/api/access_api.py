"""HTTP surface for resource sharing (grants API).

Endpoints are thin wrappers around ``ddpui.core.access.resource_share`` —
this file owns authz gates, request-shape parsing, and HTTP error mapping.
"""

import secrets
from typing import Optional

from django.conf import settings
from django.utils import timezone
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.access import resource_share, shareable_types
from ddpui.core.access.access_control import get_access_map_for_resource, get_user_access
from ddpui.core.access.ownership import is_creator_or_admin, transfer_ownership, OwnershipError
from ddpui.core.audit_log_service import create_audit_log
from ddpui.core.notifications.triggers.access import (
    notify_owner_of_new_request,
    notify_requester_of_response,
    resource_title,
)
from ddpui.core.notifications.triggers.share import (
    classify_share_recipients,
    notify_row_level_change,
    notify_share_recipients,
    snapshot_direct_levels,
)
from ddpui.models.audit_log import AuditLogAction, AuditLogResourceType
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import (
    AccessLevel,
    AccessRequest,
    AccessRequestStatus,
    LEVEL_RANK,
    ResourceShare,
)
from ddpui.schemas.access.resource_share_schema import (
    AccessRequestSchema,
    AddGrantsPayload,
    AddGrantsResponse,
    GeneralAccessPayload,
    GeneralAccessState,
    GrantsListResponse,
    OwnerInfo,
    RequestAccessPayload,
    RespondToRequestPayload,
    ShareRowSchema,
    TransferCandidateSchema,
    TransferOwnershipPayload,
    UpdateGrantPayload,
)
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


# ---------------------------------------------------------------------------
# Grants


def _general_access_state(orguser: OrgUser, rtype: str, resource) -> GeneralAccessState:
    """Snapshot the resource's current org-wide access mode for the share modal."""
    supports_public = hasattr(resource, "is_public")
    is_private = bool(getattr(resource, "is_private", False))
    is_public = bool(getattr(resource, "is_public", False))
    mode = "private" if is_private else ("public" if is_public else "internal")

    prefs = OrgPreferences.objects.filter(org=orguser.org).first() or OrgPreferences()
    allow_public_sharing = bool(prefs.allow_public_sharing)

    public_url = None
    token = getattr(resource, "public_share_token", None)
    if is_public and token:
        public_url = _public_share_url(rtype, token)

    last_public_accessed = getattr(resource, "last_public_accessed", None)
    return GeneralAccessState(
        mode=mode,
        supports_public=supports_public,
        allow_public_sharing=allow_public_sharing,
        public_url=public_url,
        public_access_count=int(getattr(resource, "public_access_count", 0) or 0),
        last_public_accessed=last_public_accessed.isoformat() if last_public_accessed else None,
    )


def _owner_info(resource) -> Optional[OwnerInfo]:
    """Best-effort owner display info for the People-with-access section.
    None on orphan resources whose ``created_by`` OrgUser was deleted."""
    owner = getattr(resource, "created_by", None)
    if owner is None:
        return None
    email = getattr(getattr(owner, "user", None), "email", None)
    if not email:
        return None
    role_name = getattr(getattr(owner, "new_role", None), "name", None)
    return OwnerInfo(orguser_id=owner.id, email=email, role_name=role_name)


@access_router.get("/{rtype}/{resource_id}/grants", response=GrantsListResponse)
def list_resource_grants(request, rtype: str, resource_id: str):
    """List everyone with access to this resource."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "view sharing")
    shares = resource_share.list_grants(orguser.org, rtype, resource_id)
    caller_is_owner = getattr(resource, "created_by_id", None) == orguser.id
    return GrantsListResponse(
        shares=shares,
        caller_is_owner=caller_is_owner,
        general_access=_general_access_state(orguser, rtype, resource),
        owner=_owner_info(resource),
    )


@access_router.post("/{rtype}/{resource_id}/grants", response=AddGrantsResponse)
def add_resource_grants(request, rtype: str, resource_id: str, payload: AddGrantsPayload):
    """Apply staged chips from the share modal — bulk add/update."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "modify sharing")
    before = snapshot_direct_levels(orguser.org, rtype, resource_id)
    try:
        written, warnings = resource_share.add_grants(
            orguser,
            rtype,
            resource_id,
            payload.principals,
            payload.pending_grants,
            payload.invite_role_uuid,
        )
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err

    try:
        classified = classify_share_recipients(before, written, orguser.id)
        notify_share_recipients(orguser, rtype, resource, classified)
    except Exception as err:  # notification failure never blocks the API call
        logger.error(f"share notification failed: {err}")

    shares = resource_share.list_grants(orguser.org, rtype, resource_id)
    return AddGrantsResponse(shares=shares, warnings=warnings)


@access_router.patch(
    "/{rtype}/{resource_id}/grants/{share_id}",
    response=list[ShareRowSchema],
)
def update_resource_grant(
    request, rtype: str, resource_id: str, share_id: int, payload: UpdateGrantPayload
):
    """Change the access level on one existing row.

    The targeted principal is notified on BOTH upgrade (view → edit) and
    downgrade (edit → view). This is deliberately more informative than the
    bulk-add flow — the row dropdown is a targeted action, so either direction
    warrants a notification. Same-level saves are silent.
    """
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "modify sharing")

    # Snapshot the pre-mutation level so notify_row_level_change can decide
    # upgrade vs downgrade vs no-op.
    existing = ResourceShare.objects.filter(id=share_id, org=orguser.org).first()
    before_level = existing.access_level if existing else None

    try:
        updated = resource_share.update_grant(orguser, share_id, payload.access_level)
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err

    if before_level is not None:
        try:
            notify_row_level_change(orguser, rtype, resource, updated, before_level)
        except Exception as err:  # notification failure never blocks the API call
            logger.error(f"row-level share notification failed: {err}")

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
# General access (Everyone / Private / Public)


_PUBLIC_URL_PATH = {"dashboard": "dashboard", "report": "report"}


def _public_share_url(rtype: str, token: str) -> str:
    """Frontend URL for the anonymous-viewer share link."""
    frontend_url = (
        getattr(settings, "FRONTEND_URL_V2", None)
        or getattr(settings, "FRONTEND_URL", None)
        or "http://localhost:3000"
    ).rstrip("/")
    return f"{frontend_url}/share/{_PUBLIC_URL_PATH.get(rtype, rtype)}/{token}"


@access_router.patch("/{rtype}/{resource_id}/general-access")
def update_general_access(request, rtype: str, resource_id: str, payload: GeneralAccessPayload):
    """Set the org-wide access mode for a resource.

    - ``internal`` — floor-based org access, no public link
    - ``private`` — only explicitly-shared users
    - ``public`` — floor-based org access + anyone with the link (dashboards/reports only)
    """
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_edit_or_admin(orguser, rtype, resource, "change access")

    is_private = payload.mode == "private"
    is_public = payload.mode == "public"
    supports_public = hasattr(resource, "is_public")

    if is_public and not supports_public:
        raise HttpError(400, f"public sharing not supported for {rtype}")

    if is_public:
        prefs = OrgPreferences.objects.filter(org=orguser.org).first()
        if prefs and not prefs.allow_public_sharing:
            raise HttpError(403, "public sharing is disabled by your admin")

    update_fields: list[str] = []
    prev_public = bool(getattr(resource, "is_public", False))

    resource.is_private = is_private
    update_fields.append("is_private")

    if supports_public:
        resource.is_public = is_public
        update_fields.append("is_public")

        if is_public:
            if not getattr(resource, "public_share_token", None):
                resource.public_share_token = secrets.token_urlsafe(48)
                update_fields.append("public_share_token")
            if hasattr(resource, "public_shared_at"):
                resource.public_shared_at = timezone.now()
                update_fields.append("public_shared_at")
            if hasattr(resource, "public_disabled_at"):
                resource.public_disabled_at = None
                update_fields.append("public_disabled_at")
        else:
            if prev_public and hasattr(resource, "public_disabled_at"):
                resource.public_disabled_at = timezone.now()
                update_fields.append("public_disabled_at")
            # Token stays dormant across public → internal AND public → private, so
            # re-enabling public later reuses the same URL. The public endpoint gates
            # on `is_public`, not token existence, so leaving the token in place while
            # `is_public=False` doesn't expose anything.

    resource.save(update_fields=update_fields)

    if supports_public and prev_public != is_public:
        rtype_map = {
            "dashboard": AuditLogResourceType.DASHBOARD,
            "report": AuditLogResourceType.REPORT,
        }
        audit_rtype = rtype_map.get(rtype)
        if audit_rtype is not None:
            create_audit_log(
                org=orguser.org,
                orguser=orguser,
                resource_type=audit_rtype,
                resource_id=str(resource.pk),
                action=AuditLogAction.SHARE,
                resource_fields={
                    "title": resource_title(resource),
                    "is_public": {"old": prev_public, "new": is_public},
                },
            )

    response = {
        "mode": payload.mode,
        "is_private": resource.is_private,
        "is_public": bool(getattr(resource, "is_public", False)),
    }
    if is_public and getattr(resource, "public_share_token", None):
        response["public_url"] = _public_share_url(rtype, resource.public_share_token)
        response["public_share_token"] = resource.public_share_token
    return response


# ---------------------------------------------------------------------------
# Ownership transfer


@access_router.post("/{rtype}/{resource_id}/transfer-ownership")
def transfer_resource_ownership(
    request, rtype: str, resource_id: str, payload: TransferOwnershipPayload
):
    """Transfer ownership of a resource to another org member."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    try:
        transfer_ownership(
            orguser,
            rtype,
            resource,
            payload.to_orguser_id,
            strip_previous_owner_access=payload.strip_previous_owner_access,
        )
    except OwnershipError as err:
        raise HttpError(400, str(err)) from err
    return {"success": True}


@access_router.get("/{rtype}/{resource_id}/candidates", response=list[TransferCandidateSchema])
def list_transfer_candidates(request, rtype: str, resource_id: str):
    """Every active user in the org with their effective access level on this
    resource. The transfer-ownership picker uses this to disable users who
    don't have Edit — only Edit-holders can become owners.

    Restricted to creator or admin — mirrors the transfer-ownership endpoint
    itself (no one else needs this list)."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    if not is_creator_or_admin(orguser, resource):
        raise HttpError(403, "only the owner or an admin can list transfer candidates")

    access_map = get_access_map_for_resource(orguser.org, rtype, resource.pk)
    owner_id = getattr(resource, "created_by_id", None)

    users = OrgUser.objects.filter(org=orguser.org).select_related("user", "new_role")
    return [
        TransferCandidateSchema(
            orguser_id=u.id,
            email=u.user.email,
            role_name=u.new_role.name if u.new_role else None,
            access_level=access_map.get(u.id, "no_access"),
            is_owner=owner_id is not None and u.id == owner_id,
        )
        for u in users
    ]


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
def create_access_request(request, rtype: str, resource_id: str, payload: RequestAccessPayload):
    """Submit a request for access to a resource the caller cannot currently see."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)

    # Allow strict upgrade requests (e.g. view → edit). Reject same-level and
    # downgrade requests: those aren't a request for *more* access.
    existing = get_user_access(orguser, rtype, resource.pk) or AccessLevel.NO_ACCESS
    if LEVEL_RANK[existing] >= LEVEL_RANK[payload.requested_level]:
        raise HttpError(409, "you already have access at this level or higher")

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
    notify_owner_of_new_request(req, resource, rtype)
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
            )  # warnings ignored — access-request approval is always a direct user grant
        except resource_share.GrantError as err:
            raise HttpError(400, str(err)) from err
        req.status = AccessRequestStatus.APPROVED
    else:
        req.status = AccessRequestStatus.DECLINED

    req.save(update_fields=["status", "updated_at"])
    notify_requester_of_response(req, resource, rtype, orguser, granted_level)
    return {"success": True, "status": req.status}
