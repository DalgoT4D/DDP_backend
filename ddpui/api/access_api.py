"""HTTP surface for resource sharing (grants API).

Endpoints are thin wrappers around ``ddpui.core.access.resource_share`` —
this file owns authz gates, request-shape parsing, and HTTP error mapping.
"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.access import resource_share, shareable_types
from ddpui.core.access.ownership import is_creator_or_admin
from ddpui.models.org_user import OrgUser
from ddpui.schemas.access.resource_share_schema import (
    AddGrantsPayload,
    ShareRowSchema,
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


def _require_owner_or_admin(orguser: OrgUser, resource, action: str) -> None:
    if not is_creator_or_admin(orguser, resource):
        raise HttpError(
            403, f"only the {resource.__class__.__name__.lower()} owner or an admin can {action}"
        )


# ---------------------------------------------------------------------------
# Grants


@access_router.get("/{rtype}/{resource_id}/grants", response=list[ShareRowSchema])
def list_resource_grants(request, rtype: str, resource_id: str):
    """List everyone with access to this resource."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_owner_or_admin(orguser, resource, "view sharing")
    return resource_share.list_grants(orguser.org, rtype, resource_id)


@access_router.post("/{rtype}/{resource_id}/grants", response=list[ShareRowSchema])
def add_resource_grants(request, rtype: str, resource_id: str, payload: AddGrantsPayload):
    """Apply staged chips from the share modal — bulk add/update."""
    orguser, resource = _fetch_resource_or_404(request, rtype, resource_id)
    _require_owner_or_admin(orguser, resource, "modify sharing")
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
    _require_owner_or_admin(orguser, resource, "modify sharing")
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
    _require_owner_or_admin(orguser, resource, "modify sharing")
    try:
        resource_share.remove_grant(orguser, share_id)
    except resource_share.GrantError as err:
        raise HttpError(400, str(err)) from err
    return resource_share.list_grants(orguser.org, rtype, resource_id)
