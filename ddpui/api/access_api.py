"""Access (Resource Sharing) API — `/api/access/*`.

Thin routes generic over ``{rtype}``: validate the rtype against the
shareable-types registry, gate, delegate to ``sharing_actions``, wrap in
``api_response``.

Gating (plan Sec 4.5 / Task 5 brief):
- GET (read): resolver **view** gate only. There is no static
  ``@has_permission`` slug literal here because the route is generic over
  rtype — anyone the resolver admits to view the resource may see who else
  has access to it.
- Mutations (POST/DELETE grants, PUT general): ``require_share_permission``
  — the dynamic mirror of ``@has_permission`` reading the rtype's
  ``share_permission_slug`` from the registry — PLUS resolver **edit** on
  the object.
"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.core.sharing import sharing_actions
from ddpui.core.sharing.exceptions import (
    GrantNotFoundError,
    PrincipalNotFoundError,
    SharingValidationError,
)
from ddpui.core.sharing.gates import (
    require_edit_access,
    require_owner_access,
    require_share_permission,
    require_view_access,
)
from ddpui.core.sharing.shareable_types import get_resource_type
from ddpui.models.org_user import OrgUser
from ddpui.schemas.access_schema import (
    AccessOverviewResponse,
    GeneralAccessUpdate,
    GeneralAccessUpdateResponse,
    GrantCreate,
    GrantOut,
    OwnerOut,
    OwnerTransferRequest,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.access_api")

access_router = Router()


def _get_resource_or_404(orguser: OrgUser, rtype: str, resource_id: str):
    """Registry-validate the rtype and fetch the resource within the caller's
    org. Unknown rtype and missing/cross-org resource are both 404 — a
    cross-org id must be indistinguishable from a nonexistent one."""
    entry = get_resource_type(rtype)
    if entry is None:
        raise HttpError(404, f"Unknown resource type '{rtype}'")
    try:
        resource = entry.model.objects.filter(pk=resource_id, org=orguser.org).first()
    except (ValueError, TypeError) as err:
        raise HttpError(404, "Resource not found") from err
    if resource is None:
        raise HttpError(404, "Resource not found")
    return resource


@access_router.get("/{rtype}/{resource_id}/", response=ApiResponse[AccessOverviewResponse])
def get_access(request, rtype: str, resource_id: str):
    """Who has access to this resource and via which path (owner, general
    access, grants). Gate: resolver view on the resource."""
    orguser: OrgUser = request.orguser
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_view_access(orguser, rtype, resource)

    overview = sharing_actions.get_access_overview(orguser, rtype, resource)
    return api_response(success=True, data=overview)


@access_router.post("/{rtype}/{resource_id}/grants/", response=ApiResponse[GrantOut])
def create_grant(request, rtype: str, resource_id: str, payload: GrantCreate):
    """Grant a user view/edit on this resource (duplicate grants update in
    place). Gate: share slug (registry) + resolver edit."""
    orguser: OrgUser = request.orguser
    require_share_permission(request, rtype)
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_edit_access(orguser, rtype, resource)

    try:
        grant = sharing_actions.upsert_grant(orguser, rtype, resource, payload)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err
    except PrincipalNotFoundError as err:
        raise HttpError(404, err.message) from err

    return api_response(success=True, data=grant, message="Access granted")


@access_router.delete("/{rtype}/{resource_id}/grants/{grant_id}/", response=ApiResponse[None])
def delete_grant(request, rtype: str, resource_id: str, grant_id: int):
    """Revoke one grant. Gate: share slug (registry) + resolver edit."""
    orguser: OrgUser = request.orguser
    require_share_permission(request, rtype)
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_edit_access(orguser, rtype, resource)

    try:
        sharing_actions.remove_grant(orguser, rtype, resource, grant_id)
    except GrantNotFoundError as err:
        raise HttpError(404, err.message) from err
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err

    return api_response(success=True, message="Access revoked")


@access_router.put(
    "/{rtype}/{resource_id}/general/", response=ApiResponse[GeneralAccessUpdateResponse]
)
def update_general_access(request, rtype: str, resource_id: str, payload: GeneralAccessUpdate):
    """Change general access with the narrowing warn-and-offer protocol.
    Gate: share slug (registry) + resolver edit."""
    orguser: OrgUser = request.orguser
    require_share_permission(request, rtype)
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_edit_access(orguser, rtype, resource)

    try:
        result = sharing_actions.set_general_access(orguser, rtype, resource, payload)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err
    except GrantNotFoundError as err:
        raise HttpError(404, err.message) from err

    message = "Confirmation required" if result.requires_confirmation else "General access updated"
    return api_response(success=True, data=result, message=message)


@access_router.post("/{rtype}/{resource_id}/owner/", response=ApiResponse[OwnerOut])
def transfer_owner(request, rtype: str, resource_id: str, payload: OwnerTransferRequest):
    """Transfer ownership to another same-org, active OrgUser. The old
    owner keeps an explicit Edit grant; there is no reclaim. Gate: share
    slug (registry) + owner-or-admin (stricter than resolver edit --
    general access/grants alone never pass this)."""
    orguser: OrgUser = request.orguser
    require_share_permission(request, rtype)
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_owner_access(orguser, rtype, resource)

    try:
        new_owner = sharing_actions.transfer_ownership(
            orguser, rtype, resource, payload.new_owner_orguser_id
        )
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err
    except PrincipalNotFoundError as err:
        raise HttpError(404, err.message) from err

    return api_response(success=True, data=new_owner, message="Ownership transferred")
