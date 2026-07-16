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
    """The resource's per-role general-access levels. Admins are never
    represented (they always have full access); Analysts and Members each
    get an independent "none"/"view"/"edit" level."""

    analyst_level: str
    member_level: str


class GrantOut(Schema):
    """One ResourceShare row (active or pending). ``member_count`` is only
    populated for group rows — active members of that group."""

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
    """One dashboard audience member a chart's own access does not admit.
    ``principal_type`` is "user", "group", or "invite" (unknown-email — no
    id/name yet). ``skipped_member=True`` marks a Member-role principal:
    extend never copies those onto the chart."""

    principal_type: str
    principal_id: Optional[int] = None
    name: Optional[str] = None
    email: Optional[str] = None
    skipped_member: bool = False


class ChartCoverageOut(Schema):
    """One chart's coverage verdict against a dashboard's audience.
    ``covered=True`` means no gap of any class ("analyst"/"member" role gaps,
    principal gaps, public exposure). ``extendable`` is True when "extend"
    can close at least one gap; ``viewer_can_edit`` says whether this caller
    resolves to Edit on the chart (extend needs it)."""

    chart_id: int
    title: str
    covered: bool
    role_gaps: List[str] = []
    principal_gaps: List[PrincipalGapOut] = []
    public_exposure: bool = False
    extendable: bool = False
    viewer_can_edit: bool = False


class DashboardChartCoverageResponse(Schema):
    """GET /api/dashboards/{id}/chart-coverage/ — one chart's verdict (with
    ``chart_id``) or every under-covering tile (without). ``covered`` is the
    AND over ``charts``."""

    dashboard_id: int
    covered: bool
    charts: List[ChartCoverageOut]


class EmbedCoverageConfirmation(Schema):
    """The 409 body ``PUT /api/dashboards/{id}/`` returns when the tabs payload
    adds under-covering charts and no confirm field was sent. Nothing was
    saved; re-send with ``extend_chart_ids``/``proceed``."""

    requires_confirmation: bool = True
    under_covering_charts: List[ChartCoverageOut] = []
    detail: str = "Confirmation required: newly added charts under-cover this dashboard"


class GrantCreate(Schema):
    """POST /api/access/{rtype}/{resource_id}/grants/ — create/update one grant.

    principal_type is "user" or "group" ("audience" is always rejected). For
    "user", exactly one of `principal_id`/`email`: an email matching an org
    user grants instantly, an unknown email invites them and creates a
    pending grant. `invite_role` only applies to that invite path (default
    member; higher roles need an admin caller). `extend_chart_ids`/`proceed`
    drive the dashboard-broadening warn-and-offer; ignored for other rtypes.
    """

    principal_type: str
    principal_id: Optional[int] = None
    email: Optional[str] = None
    permission: str
    invite_role: Optional[str] = None
    extend_chart_ids: Optional[List[int]] = None
    proceed: Optional[bool] = None


class GrantCreateResponse(Schema):
    """Either the created/updated grant, or a dashboard-broadening confirmation
    (`requires_confirmation=True`, nothing written) naming the exposed charts."""

    requires_confirmation: bool = False
    under_covering_charts: List[ChartCoverageOut] = []
    grant: Optional[GrantOut] = None


class GeneralAccessUpdate(Schema):
    """PUT /api/access/{rtype}/{resource_id}/general/ — change general access.

    `remove_grant_ids` drives the narrowing warn-and-offer: absent on a
    narrowing change with active grants, the server returns
    `requires_confirmation` and changes nothing; re-send with the field
    present (possibly []) to commit. `extend_chart_ids`/`proceed` are the
    broadening mirror for dashboards. A request that narrows one role and
    widens the other may need both confirmations in one round trip.
    """

    analyst_level: str
    member_level: str
    remove_grant_ids: Optional[List[int]] = None
    extend_chart_ids: Optional[List[int]] = None
    proceed: Optional[bool] = None


class GeneralAccessUpdateResponse(Schema):
    """Either a warn-and-offer response (`requires_confirmation=True`, nothing
    changed) or the committed setting. `persisting_grants` is the narrowing
    prompt's contents; `under_covering_charts` the broadening prompt's —
    one request can populate both."""

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
    """The ``toggle_public`` action payload. Enabling a dashboard's link
    without ``proceed`` returns it in ``requires_confirmation`` with its
    charts named; re-send with ``proceed=true`` to commit."""

    is_public: bool
    proceed: Optional[bool] = None


class BulkAccessRequest(Schema):
    """POST /api/access/bulk/ — apply one action across a selection.

    ``items`` may mix rtypes (1..BULK_MAX_ITEMS; duplicates deduplicated).
    ``action`` picks exactly one payload field, which must be present:
    ``add_grant`` (per resource; an unknown email sends one invitation),
    ``set_general`` (``remove_grant_ids`` is a flat list the server
    partitions per resource; absent = first call, present = commit), or
    ``toggle_public`` (public_link rtypes only; enabling is blocked while
    the org kill switch is off, disabling always works).
    """

    items: List[BulkItemRef]
    action: str  # add_grant | set_general | toggle_public
    add_grant: Optional[GrantCreate] = None
    set_general: Optional[GeneralAccessUpdate] = None
    toggle_public: Optional[BulkPublicToggle] = None


class BulkSkippedItem(Schema):
    """One selection item the bulk action did not apply to. ``reason`` codes:
    ``not_found``, ``share_permission_denied``, ``edit_access_denied``, the
    ``*_not_supported`` capability codes, ``public_sharing_disabled``,
    ``member_grants_deferred``, ``principal_not_found``, ``validation_error``.
    """

    rtype: str
    id: str
    reason: str


class BulkConfirmationItem(Schema):
    """One resource whose bulk action needs confirmation. Narrowing:
    ``persisting_grants`` are the grants that would keep people in — re-send
    with ``remove_grant_ids``. Broadening: ``under_covering_charts`` names the
    exposed charts — re-send with ``extend_chart_ids`` and/or ``proceed``."""

    rtype: str
    id: str
    persisting_grants: List[GrantOut] = []
    under_covering_charts: List[ChartCoverageOut] = []


class BulkAccessResponse(Schema):
    """Per-item outcome of a bulk action. Every deduplicated selection item
    lands in exactly one of ``applied``/``skipped``/``requires_confirmation``.
    Counts cover the first two only — confirmation items are still undecided."""

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
    """One ``AccessRequest`` row. ``requested_permission`` is always the
    original ask; an owner's downgrade-on-approve is reflected in the
    resulting grant, not here."""

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
    Any authenticated org member may call this. A duplicate pending request
    refreshes the existing row instead of stacking a second one."""

    requested_permission: str
    note: Optional[str] = Field(None, max_length=500)


class AccessRequestDecision(Schema):
    """POST /api/access/requests/{request_id}/approve/. ``permission`` is an
    optional owner override, capped at the originally requested permission
    (downgrade only); omitted = grant exactly what was asked."""

    permission: Optional[str] = None


class AccessRequestListResponse(Schema):
    """GET /api/access/requests/ — the caller's access-request inbox.
    ``incoming``: pending requests on resources the caller can decide.
    ``outgoing``: the caller's own requests, any status, most recent first."""

    incoming: List[AccessRequestOut]
    outgoing: List[AccessRequestOut]
