"""Request/response contracts for the `/api/access/*` sharing endpoints.

The GET-access response shape is a frontend contract (the sharing modal
renders its sections off `capabilities`, not entity-type conditionals) —
keep it stable.
"""

from datetime import datetime
from typing import List, Optional

from ninja import Field, Schema


class CapabilityFlags(Schema):
    """The registry capability flags for one shareable rtype, echoed to the
    client so the modal renders sections off flags."""

    general: bool
    grants: bool
    public_link: bool
    requests: bool


class OwnerOut(Schema):
    """The resource's owner (owner FK, falling back to created_by)."""

    orguser_id: int
    email: str
    name: str


class GeneralAccessOut(Schema):
    """Layer 1: the resource's org-wide general-access setting."""

    audience: str
    level: str


class GrantOut(Schema):
    """One ResourceShare row (active or pending).

    ``member_count`` is only populated for ``principal_type="group"`` rows
    (the count of that group's active members) — the sharing modal uses it
    to render "Funders (3 members)" without a second round trip.
    """

    id: int
    principal_type: str
    principal_id: Optional[int] = None
    email: Optional[str] = None
    name: Optional[str] = None
    permission: str
    status: str
    member_count: Optional[int] = None


class ViewerOut(Schema):
    """What the calling viewer may do with this resource."""

    effective_permission: Optional[str] = None
    is_owner: bool


class AccessOverviewResponse(Schema):
    """GET /api/access/{rtype}/{resource_id}/ — who has access and via which path."""

    resource_type: str
    resource_id: str
    capabilities: CapabilityFlags
    owner: Optional[OwnerOut] = None
    general_access: Optional[GeneralAccessOut] = None
    grants: List[GrantOut]
    viewer: ViewerOut


class GrantCreate(Schema):
    """POST /api/access/{rtype}/{resource_id}/grants/ — create/update one grant.

    Accepts principal_type "user" or "group" (a same-org id in both cases).
    "audience" is deferred by design and always rejected with 400.

    For principal_type="user", the sharing modal may address the principal
    either by `principal_id` (a same-org OrgUser) or by `email` (Task 9 —
    the share-flow invite): an `email` belonging to an existing OrgUser
    grants instantly; an unknown `email` invites them (as a Member) and
    creates a pending grant that activates when they accept. Exactly one of
    `principal_id`/`email` must be set; `email` is invalid for
    principal_type="group".
    """

    principal_type: str
    principal_id: Optional[int] = None
    email: Optional[str] = None
    permission: str


class GeneralAccessUpdate(Schema):
    """PUT /api/access/{rtype}/{resource_id}/general/ — change general access.

    `remove_grant_ids` drives the narrowing warn-and-offer protocol: absent
    (None) on a narrowing change with active grants, the server returns
    `requires_confirmation` and changes nothing; the client re-sends with
    the field present (possibly []) to commit.
    """

    audience: str
    level: str
    remove_grant_ids: Optional[List[int]] = None


class GeneralAccessUpdateResponse(Schema):
    """Either a warn-and-offer response (`requires_confirmation=True`,
    nothing changed) or the committed general-access setting."""

    requires_confirmation: bool = False
    persisting_grants: List[GrantOut] = []
    general_access: Optional[GeneralAccessOut] = None


class OwnerTransferRequest(Schema):
    """POST /api/access/{rtype}/{resource_id}/owner/ — transfer ownership to
    another same-org, active OrgUser. Transfer is final; there is no
    reclaim/undo."""

    new_owner_orguser_id: int


class RequesterOut(Schema):
    """An OrgUser reference on an ``AccessRequest`` (the requester, or the
    decider once decided)."""

    orguser_id: int
    email: str
    name: str


class AccessRequestOut(Schema):
    """One ``AccessRequest`` row (Milestone 9 — request-access).

    ``requested_permission`` is always the ORIGINAL ask; an owner's
    downgrade-on-approve (Edit -> View) is reflected in the resulting
    grant (see GET /api/access/{rtype}/{resource_id}/), not here.
    """

    id: int
    resource_type: str
    resource_id: str
    requester: RequesterOut
    requested_permission: str
    note: Optional[str] = None
    status: str
    decided_by: Optional[RequesterOut] = None
    expires_at: datetime
    created_at: datetime


class AccessRequestCreate(Schema):
    """POST /api/access/{rtype}/{resource_id}/requests/ — ask for access.

    Any authenticated org member may call this (no share-permission slug —
    Members must be able to ask). 400s if the caller already has effective
    access, or if the rtype doesn't support requests
    (``capabilities.requests``). A duplicate pending request from the same
    requester for the same resource refreshes the existing row instead of
    stacking a second one.
    """

    requested_permission: str
    note: Optional[str] = Field(None, max_length=500)


class AccessRequestDecision(Schema):
    """POST /api/access/requests/{request_id}/approve/ — decide a request.

    ``permission`` is an optional owner override, capped at the originally
    requested permission (an owner may downgrade Edit -> View, never
    escalate beyond what was asked). Omitted = grant exactly what was
    requested. Ignored by the decline endpoint (no body).
    """

    permission: Optional[str] = None


class AccessRequestListResponse(Schema):
    """GET /api/access/requests/ — the caller's access-request inbox.

    ``incoming``: pending requests on resources the caller can decide (they
    are the owner, owner-fallback `created_by`, or an admin/super-admin) —
    an actionable inbox, so only ``status="pending"`` rows appear here.
    ``outgoing``: the caller's own requests, any status, most recent first
    — so they can see the outcome of a past ask.
    """

    incoming: List[AccessRequestOut]
    outgoing: List[AccessRequestOut]
