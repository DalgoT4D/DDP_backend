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
- Access requests (Milestone 9): creating a request has NO gate beyond
  "authenticated org member" — a Member without access must be able to ask.
  Approve/decline gate on ``require_owner_access`` ONLY (owner-or-admin) —
  deliberately WITHOUT ``require_share_permission``, unlike every other
  mutation here (including ownership transfer). Deciding a request is owner
  business, not a sharing-permission-gated action: a Member who owns a
  resource (e.g. via ownership transfer, Task 12) holds no ``can_share_*``
  slug but must still be able to approve/decline requests on their own
  resource — the task brief spells out the slug interaction for create
  ("no share slug") and pointedly omits one for approve/decline, read here
  as intentional.
"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.core.sharing import access_requests, sharing_actions
from ddpui.core.sharing.exceptions import (
    GrantNotFoundError,
    PrincipalNotFoundError,
    SharingPermissionError,
    SharingValidationError,
)
from ddpui.core.sharing.access_resolver import effective_permission
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
    AccessRequestCreate,
    AccessRequestDecision,
    AccessRequestListResponse,
    AccessRequestOut,
    BulkAccessRequest,
    BulkAccessResponse,
    BulkSkippedItem,
    GeneralAccessUpdate,
    GeneralAccessUpdateResponse,
    GrantCreate,
    GrantCreateResponse,
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


@access_router.post("/{rtype}/{resource_id}/grants/", response=ApiResponse[GrantCreateResponse])
def create_grant(request, rtype: str, resource_id: str, payload: GrantCreate):
    """Grant a user view/edit on this resource (duplicate grants update in
    place). Gate: share slug (registry) + resolver edit.

    v1.1 M2: granting on a DASHBOARD whose tiles the new principal cannot
    see standalone returns `requires_confirmation` with the under-covering
    charts named and writes nothing — re-send with `extend_chart_ids` (a
    subset of those charts; needs Edit on each) and/or `proceed=true` to
    commit (the broadening warning's contract)."""
    orguser: OrgUser = request.orguser
    require_share_permission(request, rtype)
    resource = _get_resource_or_404(orguser, rtype, resource_id)
    require_edit_access(orguser, rtype, resource)

    try:
        result = sharing_actions.upsert_grant_with_coverage(orguser, rtype, resource, payload)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err
    except SharingPermissionError as err:
        raise HttpError(403, err.message) from err
    except PrincipalNotFoundError as err:
        raise HttpError(404, err.message) from err

    message = "Confirmation required" if result.requires_confirmation else "Access granted"
    return api_response(success=True, data=result, message=message)


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
    except SharingPermissionError as err:
        # v1.1 M2: a confirmed broadening `extend_chart_ids` needs Edit on
        # each named chart
        raise HttpError(403, err.message) from err
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


@access_router.post("/bulk/", response=ApiResponse[BulkAccessResponse])
def bulk_access(request, payload: BulkAccessRequest):
    """Apply ONE action (add_grant / set_general / toggle_public) across a
    selection of resources, mixed rtypes allowed. Apply-where-possible:
    every item is independently gated — registry rtype, the rtype's share
    slug (a Member with no slugs gets every item skipped, not a 403),
    org-scoped fetch (cross-org ids are `not_found`, indistinguishable from
    nonexistent), resolver edit — and per-item failures become `skipped`
    entries with a reason code. Only request-shape problems 4xx the whole
    request: empty selection, selection over BULK_MAX_ITEMS, unknown
    action, missing/malformed action payload, and (for set_general
    re-sends) `remove_grant_ids` that don't belong to the selection."""
    orguser: OrgUser = request.orguser

    if not payload.items:
        raise HttpError(400, "items must not be empty")
    if len(payload.items) > sharing_actions.BULK_MAX_ITEMS:
        raise HttpError(
            400, f"a bulk selection is capped at {sharing_actions.BULK_MAX_ITEMS} items"
        )
    action_payloads = {
        "add_grant": payload.add_grant,
        "set_general": payload.set_general,
        "toggle_public": payload.toggle_public,
    }
    if payload.action not in action_payloads:
        raise HttpError(400, f"invalid action '{payload.action}'")
    if action_payloads[payload.action] is None:
        raise HttpError(400, f"the '{payload.action}' payload is required for this action")

    permissions = set(request.permissions or [])
    resolved = []
    skipped: list[BulkSkippedItem] = []
    seen = set()
    for item in payload.items:
        if (item.rtype, item.id) in seen:
            continue
        seen.add((item.rtype, item.id))
        entry = get_resource_type(item.rtype)
        if entry is None:
            skipped.append(BulkSkippedItem(rtype=item.rtype, id=item.id, reason="not_found"))
            continue
        if entry.share_permission_slug not in permissions:
            skipped.append(
                BulkSkippedItem(rtype=item.rtype, id=item.id, reason="share_permission_denied")
            )
            continue
        try:
            resource = entry.model.objects.filter(pk=item.id, org=orguser.org).first()
        except (ValueError, TypeError):
            resource = None
        if resource is None:
            skipped.append(BulkSkippedItem(rtype=item.rtype, id=item.id, reason="not_found"))
            continue
        if effective_permission(orguser, item.rtype, resource) != "edit":
            skipped.append(
                BulkSkippedItem(rtype=item.rtype, id=item.id, reason="edit_access_denied")
            )
            continue
        resolved.append((item.rtype, resource))

    try:
        result = sharing_actions.bulk_apply(orguser, payload, resolved, skipped)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err
    except SharingPermissionError as err:
        # A caller-privilege problem (e.g. non-admin invite_role escalation)
        # fails the whole request — it can't succeed for ANY item.
        raise HttpError(403, err.message) from err
    except GrantNotFoundError as err:
        raise HttpError(404, err.message) from err
    except PrincipalNotFoundError as err:
        raise HttpError(404, err.message) from err

    message = f"Applied to {result.applied_count} of {len(seen)} resources"
    if result.requires_confirmation:
        message = "Confirmation required for some resources"
    return api_response(success=True, data=result, message=message)


def _get_access_request_or_404(orguser: OrgUser, request_id: int):
    """Fetch an ``AccessRequest`` scoped to the caller's org -- a cross-org
    id is indistinguishable from a nonexistent one, same rule as
    ``_get_resource_or_404``."""
    access_request = access_requests.get_access_request_or_none(orguser.org_id, request_id)
    if access_request is None:
        raise HttpError(404, "Access request not found")
    return access_request


@access_router.post("/{rtype}/{resource_id}/requests/", response=ApiResponse[AccessRequestOut])
def create_access_request(request, rtype: str, resource_id: str, payload: AccessRequestCreate):
    """Ask for access to this resource. Gate: any authenticated org member
    -- no share-permission slug (Members must be able to ask). 400s if the
    caller already has effective access, or the rtype doesn't support
    requests."""
    orguser: OrgUser = request.orguser
    resource = _get_resource_or_404(orguser, rtype, resource_id)

    try:
        access_request = access_requests.create_access_request(orguser, rtype, resource, payload)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err

    return api_response(success=True, data=access_request, message="Access requested")


@access_router.get("/requests/", response=ApiResponse[AccessRequestListResponse])
def list_access_requests(request):
    """The caller's access-request inbox: `incoming` (pending requests on
    resources they can decide) + `outgoing` (their own requests, any
    status). Gate: any authenticated org member."""
    orguser: OrgUser = request.orguser
    result = access_requests.list_access_requests(orguser)
    return api_response(success=True, data=result)


@access_router.post("/requests/{request_id}/approve/", response=ApiResponse[AccessRequestOut])
def approve_access_request(request, request_id: int, payload: AccessRequestDecision):
    """Approve a request: inserts a grant (works for every registered
    rtype, including grants=False metric/kpi -- an internal write, same
    pattern as ownership transfer). Gate: owner-or-admin on the requested
    resource ONLY -- NOT mere editors, deciding access is owner business;
    deliberately no share-permission slug (see module docstring)."""
    orguser: OrgUser = request.orguser
    access_request = _get_access_request_or_404(orguser, request_id)
    resource = _get_resource_or_404(
        orguser, access_request.resource_type, access_request.resource_id
    )
    require_owner_access(orguser, access_request.resource_type, resource)

    try:
        result = access_requests.approve_access_request(orguser, access_request, resource, payload)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err

    return api_response(success=True, data=result, message="Access request approved")


@access_router.post("/requests/{request_id}/decline/", response=ApiResponse[AccessRequestOut])
def decline_access_request(request, request_id: int):
    """Decline a request: no grant, notifies the requester. Gate:
    owner-or-admin on the requested resource ONLY (no share-permission
    slug -- see module docstring)."""
    orguser: OrgUser = request.orguser
    access_request = _get_access_request_or_404(orguser, request_id)
    resource = _get_resource_or_404(
        orguser, access_request.resource_type, access_request.resource_id
    )
    require_owner_access(orguser, access_request.resource_type, resource)

    try:
        result = access_requests.decline_access_request(orguser, access_request, resource)
    except SharingValidationError as err:
        raise HttpError(400, err.message) from err

    return api_response(success=True, data=result, message="Access request declined")
