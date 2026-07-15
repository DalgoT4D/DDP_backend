"""Groups API — `/api/groups/*` (Task 7).

Thin routes: validate, gate, delegate to `user_groups_service`, wrap in
`api_response`.

Gating:
- Base access (list/get, create) is Analyst+ via the static
  `@has_permission` decorator (`can_view_user_groups` for reads,
  `can_manage_user_groups` for create and every mutation of a specific
  group). Member holds neither slug.
- Rename/delete/add-member/remove-member additionally require the caller be
  the group's creator or an Admin — an object-level check enforced in
  `user_groups_service`, mirroring `ddpui.core.ownership`'s pattern.
"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.user_groups import user_groups_service
from ddpui.core.user_groups.exceptions import (
    GroupNameCollisionError,
    GroupNotFoundError,
    GroupPermissionError,
    GroupValidationError,
    MemberNotFoundError,
)
from ddpui.models.org_user import OrgUser
from ddpui.schemas.group_schema import (
    GroupCreate,
    GroupDetailOut,
    GroupMemberCreate,
    GroupMemberOut,
    GroupOut,
    GroupUpdate,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.groups_api")

groups_router = Router()


@groups_router.get("/", response=ApiResponse[List[GroupOut]])
@has_permission(["can_view_user_groups"])
def list_groups(request):
    """List this org's groups, with member/shared-resource counts."""
    orguser: OrgUser = request.orguser
    groups = user_groups_service.list_groups(orguser)
    return api_response(success=True, data=groups)


@groups_router.post("/", response=ApiResponse[GroupOut])
@has_permission(["can_manage_user_groups"])
def create_group(request, payload: GroupCreate):
    """Create a group (caller becomes its creator)."""
    orguser: OrgUser = request.orguser
    try:
        group = user_groups_service.create_group(orguser, payload)
    except (GroupValidationError, GroupNameCollisionError) as err:
        raise HttpError(400, err.message) from err
    return api_response(success=True, data=group, message="Group created")


@groups_router.get("/{group_id}/", response=ApiResponse[GroupDetailOut])
@has_permission(["can_view_user_groups"])
def get_group(request, group_id: int):
    """One group plus its members."""
    orguser: OrgUser = request.orguser
    try:
        group = user_groups_service.get_group(orguser, group_id)
    except GroupNotFoundError as err:
        raise HttpError(404, err.message) from err
    return api_response(success=True, data=group)


@groups_router.put("/{group_id}/", response=ApiResponse[GroupOut])
@has_permission(["can_manage_user_groups"])
def update_group(request, group_id: int, payload: GroupUpdate):
    """Rename a group. Creator or Admin only."""
    orguser: OrgUser = request.orguser
    try:
        group = user_groups_service.update_group(orguser, group_id, payload)
    except GroupNotFoundError as err:
        raise HttpError(404, err.message) from err
    except GroupPermissionError as err:
        raise HttpError(403, err.message) from err
    except (GroupValidationError, GroupNameCollisionError) as err:
        raise HttpError(400, err.message) from err
    return api_response(success=True, data=group, message="Group renamed")


@groups_router.delete("/{group_id}/", response=ApiResponse[None])
@has_permission(["can_manage_user_groups"])
def delete_group(request, group_id: int):
    """Delete a group (and its ResourceShare grant rows). Creator or Admin
    only."""
    orguser: OrgUser = request.orguser
    try:
        user_groups_service.delete_group(orguser, group_id)
    except GroupNotFoundError as err:
        raise HttpError(404, err.message) from err
    except GroupPermissionError as err:
        raise HttpError(403, err.message) from err
    return api_response(success=True, message="Group deleted")


@groups_router.post("/{group_id}/members/", response=ApiResponse[GroupMemberOut])
@has_permission(["can_manage_user_groups"])
def add_member(request, group_id: int, payload: GroupMemberCreate):
    """Add a member by ``orguser_id`` OR ``email`` (exactly one). An unknown
    email invites them (Member only) and stages a pending row. Idempotent.
    Creator or Admin only."""
    orguser: OrgUser = request.orguser
    try:
        member = user_groups_service.add_member(orguser, group_id, payload)
    except GroupNotFoundError as err:
        raise HttpError(404, err.message) from err
    except GroupPermissionError as err:
        raise HttpError(403, err.message) from err
    except MemberNotFoundError as err:
        raise HttpError(404, err.message) from err
    except GroupValidationError as err:
        raise HttpError(400, err.message) from err
    return api_response(success=True, data=member, message="Member added")


@groups_router.delete("/{group_id}/members/{member_id}/", response=ApiResponse[None])
@has_permission(["can_manage_user_groups"])
def remove_member(request, group_id: int, member_id: int):
    """Remove a membership row. Creator or Admin only."""
    orguser: OrgUser = request.orguser
    try:
        user_groups_service.remove_member(orguser, group_id, member_id)
    except GroupNotFoundError as err:
        raise HttpError(404, err.message) from err
    except GroupPermissionError as err:
        raise HttpError(403, err.message) from err
    except MemberNotFoundError as err:
        raise HttpError(404, err.message) from err
    return api_response(success=True, message="Member removed")
