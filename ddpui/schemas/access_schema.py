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
    """Layer 1: the resource's per-role general-access levels (D1).

    Replaces the old ``audience``/``level`` pair -- Admins are never
    represented here (they always have full access); Analysts and Members
    each get their own independent level ("none"/"view"/"edit").
    """

    analyst_level: str
    member_level: str


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


class PrincipalGapOut(Schema):
    """One dashboard audience member a chart's own access does not admit
    (v1.1 M2 coverage). ``principal_type`` is "user", "group", or "invite"
    (an unknown-email invite — no OrgUser exists yet, so no id/name).
    ``skipped_member=True`` marks a Member-role principal: extend never
    copies those onto the chart (Member chart sharing is deferred), the
    warning copy must say so."""

    principal_type: str
    principal_id: Optional[int] = None
    name: Optional[str] = None
    email: Optional[str] = None
    skipped_member: bool = False


class ChartCoverageOut(Schema):
    """One chart's coverage verdict against a dashboard's audience (v1.1
    M2). ``covered=True`` means no gap of any class. Gap classes:

    - ``role_gaps``: "analyst" (dashboard admits Analysts, chart's
      ``analyst_level`` is "none" — extendable) and/or "member" (dashboard
      admits Members; charts can't in v1.1 — informational).
    - ``principal_gaps``: dashboard direct grants the chart doesn't cover.
    - ``public_exposure``: the dashboard's public link exposes the chart
      anonymously (informational, never extendable).

    ``extendable`` is True when "extend" can close at least one gap (an
    analyst role gap or a non-Member principal gap); ``viewer_can_edit``
    says whether THIS caller resolves to Edit on the chart (extend needs
    it), so the UI can offer extend vs request-access."""

    chart_id: int
    title: str
    covered: bool
    role_gaps: List[str] = []
    principal_gaps: List[PrincipalGapOut] = []
    public_exposure: bool = False
    extendable: bool = False
    viewer_can_edit: bool = False


class DashboardChartCoverageResponse(Schema):
    """GET /api/dashboards/{id}/chart-coverage/ — with ``chart_id``, that
    one chart's verdict (whether or not it is a tile yet — the embed
    pre-flight); without, every under-covering tile on the dashboard (the
    broadening panels' listing). ``covered`` is the AND over ``charts``."""

    dashboard_id: int
    covered: bool
    charts: List[ChartCoverageOut]


class EmbedCoverageConfirmation(Schema):
    """The 409 body ``PUT /api/dashboards/{id}/`` returns when the tabs
    payload adds charts that under-cover the dashboard's audience and the
    request carried neither ``extend_chart_ids`` nor ``proceed`` (v1.1 M2
    embed warning). Nothing was saved; re-send with the confirm fields."""

    requires_confirmation: bool = True
    under_covering_charts: List[ChartCoverageOut] = []
    detail: str = "Confirmation required: newly added charts under-cover this dashboard"


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

    `extend_chart_ids`/`proceed` (v1.1 M2) drive the dashboard-broadening
    warn-and-offer, mirroring `remove_grant_ids` on the narrowing side:
    a first call granting on a DASHBOARD whose tiles the new principal
    can't see standalone returns `requires_confirmation` (nothing written)
    when BOTH fields are absent. The re-send commits: `extend_chart_ids`
    (subset of the warned charts; caller needs Edit on each) extends those
    charts to cover the dashboard's audience, `proceed=true` commits
    without touching charts (exposure acknowledged — tiles render inline
    regardless). Ignored for every other rtype.
    """

    principal_type: str
    principal_id: Optional[int] = None
    email: Optional[str] = None
    permission: str
    invite_role: Optional[str] = None
    extend_chart_ids: Optional[List[int]] = None
    proceed: Optional[bool] = None


class GrantCreateResponse(Schema):
    """POST grants response (v1.1 M2): either the created/updated grant, or
    a dashboard-broadening confirmation (`requires_confirmation=True`,
    nothing written) naming the under-covering charts."""

    requires_confirmation: bool = False
    under_covering_charts: List[ChartCoverageOut] = []
    grant: Optional[GrantOut] = None


class GeneralAccessUpdate(Schema):
    """PUT /api/access/{rtype}/{resource_id}/general/ — change general access.

    `analyst_level`/`member_level` (D1) replace the old `audience`/`level`
    pair -- each independently one of "none"/"view"/"edit".

    `remove_grant_ids` drives the narrowing warn-and-offer protocol: absent
    (None) on a narrowing change with active grants, the server returns
    `requires_confirmation` and changes nothing; the client re-sends with
    the field present (possibly []) to commit. Narrowing is now evaluated
    per role -- e.g. dropping `member_level` from "view" to "none" only
    flags grants held by Members.

    `extend_chart_ids`/`proceed` (v1.1 M2) are the BROADENING mirror, for
    dashboards only: raising a role's level past an inner chart's own
    access returns `requires_confirmation` with the under-covering charts
    named (nothing changed) when both fields are absent. The re-send
    commits: `extend_chart_ids` (subset of the warned charts; caller needs
    Edit on each) raises those charts to cover, `proceed=true` commits
    without touching charts. A request that narrows one role AND widens the
    other may need both confirmations in one round trip.
    """

    analyst_level: str
    member_level: str
    remove_grant_ids: Optional[List[int]] = None
    extend_chart_ids: Optional[List[int]] = None
    proceed: Optional[bool] = None


class GeneralAccessUpdateResponse(Schema):
    """Either a warn-and-offer response (`requires_confirmation=True`,
    nothing changed) or the committed general-access setting.
    `persisting_grants` is the narrowing prompt's contents;
    `under_covering_charts` (v1.1 M2) the broadening prompt's — a request
    that narrows one role and widens the other can populate both."""

    requires_confirmation: bool = False
    persisting_grants: List[GrantOut] = []
    under_covering_charts: List[ChartCoverageOut] = []
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
    """The ``toggle_public`` action payload: the desired public state.
    ``proceed`` (v1.1 M2): enabling a DASHBOARD's public link exposes every
    tile anonymously — the first call (proceed absent) returns those
    dashboards in ``requires_confirmation`` with their charts named;
    re-send with ``proceed=true`` to commit. Public exposure is never
    extendable, so there is no ``extend_chart_ids`` here."""

    is_public: bool
    proceed: Optional[bool] = None


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
    """One resource whose bulk action needs confirmation. For ``set_general``
    narrowing: ``persisting_grants`` are the active grants that would keep
    people in — re-send with ``remove_grant_ids`` present (possibly []) to
    commit. For dashboard BROADENING (``set_general`` raise, ``add_grant``,
    ``toggle_public`` enable — v1.1 M2): ``under_covering_charts`` names the
    exposed charts — re-send with ``extend_chart_ids`` present and/or
    ``proceed=true`` on the action payload to commit."""

    rtype: str
    id: str
    persisting_grants: List[GrantOut] = []
    under_covering_charts: List[ChartCoverageOut] = []


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
