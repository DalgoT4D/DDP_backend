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
    grants instantly; an unknown `email` invites them and creates a pending
    grant that activates when they accept. Exactly one of
    `principal_id`/`email` must be set; `email` is invalid for
    principal_type="group".

    `invite_role` (Phase C3) is only consulted on that unknown-email invite
    path: the invited user's role, one of the member/analyst/admin slugs
    (default member). Non-member values require the CALLER to be an
    admin/super-admin — 403 otherwise.
    """

    principal_type: str
    principal_id: Optional[int] = None
    email: Optional[str] = None
    permission: str
    invite_role: Optional[str] = None


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


class BulkItemRef(Schema):
    """One resource in a bulk selection: the rtype + stringified pk pair.
    Also the shape of each entry in ``BulkAccessResponse.applied``."""

    rtype: str
    id: str


class BulkPublicToggle(Schema):
    """The ``toggle_public`` action payload: the desired public state."""

    is_public: bool


class BulkAccessRequest(Schema):
    """POST /api/access/bulk/ — apply ONE action across a selection.

    ``items`` may mix rtypes (1..BULK_MAX_ITEMS entries; duplicates are
    deduplicated). ``action`` picks exactly one of the three payload
    fields, which must be present:

    - ``add_grant``: the same ``GrantCreate`` shape as the single-item
      grants endpoint, applied per resource. An unknown ``email`` sends
      ONE invitation and creates one pending grant per eligible resource.
    - ``set_general``: the same ``GeneralAccessUpdate`` shape.
      ``remove_grant_ids`` is a flat, global list (grant ids are unique
      PKs, so they need no per-resource nesting); the server partitions
      them per resource. Absent (None) = first call: resources narrowing
      onto active grants come back in ``requires_confirmation`` (nothing
      changed for them), everything else applies immediately. Present
      (possibly []) = commit: narrowing applies and the listed grants are
      deleted.
    - ``toggle_public``: only rtypes with the ``public_link`` capability
      (dashboard, report) can apply; enabling is blocked per-resource
      while the org kill switch is off; disabling is always allowed.
    """

    items: List[BulkItemRef]
    action: str  # add_grant | set_general | toggle_public
    add_grant: Optional[GrantCreate] = None
    set_general: Optional[GeneralAccessUpdate] = None
    toggle_public: Optional[BulkPublicToggle] = None


class BulkSkippedItem(Schema):
    """One selection item the bulk action did not apply to, and why.

    ``reason`` codes: ``not_found`` (unknown rtype, nonexistent or
    cross-org id — indistinguishable by design), ``share_permission_denied``
    (caller lacks the rtype's can_share_* slug), ``edit_access_denied``
    (resolver says the caller can't edit this resource),
    ``grants_not_supported`` / ``general_access_not_supported`` /
    ``public_link_not_supported`` (registry capability flags),
    ``public_sharing_disabled`` (org kill switch is off, enable refused),
    ``principal_not_found`` / ``validation_error`` (per-item action
    failures, e.g. granting above the caller's own level on that resource).
    """

    rtype: str
    id: str
    reason: str


class BulkConfirmationItem(Schema):
    """One resource whose ``set_general`` narrowing needs confirmation:
    these active grants would keep people in. Re-send the SAME bulk request
    with ``remove_grant_ids`` present (any subset of these ids, possibly
    []) to commit."""

    rtype: str
    id: str
    persisting_grants: List[GrantOut]


class BulkAccessResponse(Schema):
    """POST /api/access/bulk/ — the per-item outcome of a bulk action.

    Every deduplicated selection item lands in exactly one of ``applied``,
    ``skipped``, or ``requires_confirmation`` (each list individually
    preserves selection order). Counts cover the first two only —
    confirmation items are still undecided."""

    applied: List[BulkItemRef]
    skipped: List[BulkSkippedItem]
    requires_confirmation: List[BulkConfirmationItem]
    applied_count: int
    skipped_count: int


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
