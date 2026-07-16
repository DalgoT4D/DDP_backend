"""Request/response contracts for the `/api/groups/*` endpoints. The
list/detail response shapes are a frontend contract — keep them stable.
"""

from datetime import datetime
from typing import List, Optional

from ninja import Schema


class GroupCreatorOut(Schema):
    """The group's creator (``created_by``), null once that OrgUser is
    deleted — the FK is SET_NULL, the group survives."""

    orguser_id: int
    email: str
    name: str


class GroupOut(Schema):
    """One row in the groups list, or the shape returned by create/rename."""

    id: int
    name: str
    member_count: int
    shared_resource_count: int
    created_by: Optional[GroupCreatorOut] = None
    created_at: datetime
    # Up to 4 active member emails for the Groups-table avatar stack. Only
    # the list path fills this; create/rename/detail leave it empty.
    member_preview: List[str] = []


class GroupMemberOut(Schema):
    """One `UserGroupMember` row. `pending_email` rows have no
    `orguser_id`/`email`/`name`."""

    id: int
    orguser_id: Optional[int] = None
    email: Optional[str] = None
    name: Optional[str] = None
    pending_email: Optional[str] = None
    status: str
    # The member's org-role slug, populated on the detail path. Pending-email
    # rows have no OrgUser yet, so this stays None for them.
    role: Optional[str] = None


class GroupDetailOut(GroupOut):
    """GET /api/groups/{id} — the group plus its members."""

    members: List[GroupMemberOut]


class GroupCreate(Schema):
    """POST /api/groups/ — create a group."""

    name: str


class GroupUpdate(Schema):
    """PUT /api/groups/{id} — rename a group."""

    name: str


class GroupMemberCreate(Schema):
    """POST /api/groups/{id}/members — add a member by OrgUser id or by email
    (exactly one). An email matching an org member adds them directly; an
    unknown email invites them and stages a pending row that activates on
    signup. ``invite_role`` only applies to that invite path (default Member;
    higher roles need an admin caller)."""

    orguser_id: Optional[int] = None
    email: Optional[str] = None
    invite_role: Optional[str] = None
