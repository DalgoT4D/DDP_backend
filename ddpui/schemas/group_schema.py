"""Request/response contracts for the `/api/groups/*` endpoints (Task 7).

The list/detail response shapes are a frontend contract (the Groups page and
the sharing modal's group picker both consume them) — keep them stable.
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
    # Phase A / A2 (design alignment): up to 4 ACTIVE member emails for the
    # avatar stack in the Groups table. Only the list path fills this;
    # create/rename/detail leave it empty.
    member_preview: List[str] = []


class GroupMemberOut(Schema):
    """One `UserGroupMember` row. `pending_email` rows (M4 invite flow) have
    no `orguser_id`/`email`/`name` — those are schema-only in this task."""

    id: int
    orguser_id: Optional[int] = None
    email: Optional[str] = None
    name: Optional[str] = None
    pending_email: Optional[str] = None
    status: str
    # The member's org-role slug (e.g. "analyst"), populated on the detail
    # path from their OrgUser.new_role. Pending-email rows have no OrgUser
    # yet, so this stays None for them (Phase F5).
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
    """POST /api/groups/{id}/members — add a member by OrgUser id OR by
    email (exactly one). An email matching an existing org member adds them
    directly; an unknown (or not-yet-active) email invites them via the
    share-flow's invite machinery and stages a PENDING row that activates on
    signup (M4 -- see ``orguserfunctions.activate_pending_shares_and_memberships``)."""

    orguser_id: Optional[int] = None
    email: Optional[str] = None
