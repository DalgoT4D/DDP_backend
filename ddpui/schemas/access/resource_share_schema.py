"""Payloads and responses for the resource-share (grants) API.

Every dict shape used by ``ddpui.core.access.resource_share`` is declared
here so callers can see the contract without reading the implementation.
"""

from typing import Literal, Optional

from ninja import Schema


AccessLevel = Literal["view", "edit"]
EffectiveAccessLevel = Literal["no_access", "view", "edit"]
PrincipalType = Literal["user", "group"]
ShareRowKind = Literal["user", "group", "invitation"]
ShareRowStatus = Literal["active", "pending"]
GeneralAccessMode = Literal["internal", "private", "public"]


# ---------------------------------------------------------------------------
# Read


class CascadeSourceSchema(Schema):
    """One dashboard that cascaded access to this resource."""

    dashboard_id: int
    dashboard_title: str


class ShareRowSchema(Schema):
    """One row in the "People with access" list. ``GET /api/access/{rtype}/{id}``
    returns ``list[ShareRowSchema]`` directly.

    ``share_id`` is the direct grant row id — None when the principal only has
    cascade-derived access (no direct grant exists). The frontend uses share_id
    for PATCH/DELETE; None means those controls are disabled.
    """

    share_id: Optional[int] = None
    principal_type: ShareRowKind
    principal_id: Optional[int] = None
    email: Optional[str] = None
    label: str
    role_or_group: Optional[str] = None
    access_level: AccessLevel
    status: ShareRowStatus
    cascade_sources: list[CascadeSourceSchema] = []


class OwnerInfo(Schema):
    """Resource owner (``created_by``) surfaced separately from the shares list
    since the owner has implicit Edit access — no ``ResourceShare`` row exists
    for them. Null on orphan resources (creator was deleted from the org)."""

    orguser_id: int
    email: str
    role_name: Optional[str] = None


class ParentBlockSchema(Schema):
    """A parent dashboard that would block a visibility downgrade on a nested resource."""

    dashboard_id: int
    dashboard_title: str
    mode: GeneralAccessMode


class GeneralAccessState(Schema):
    """Current org-wide access state, embedded in the grants response so the
    share modal can render its "General access" section from a single GET.

    ``supports_public`` is False for rtypes that have no ``is_public`` field
    (charts, KPIs) — the frontend hides the Public option accordingly.

    ``caller_access_via_floor`` is True when the caller's Edit access comes
    solely from the org floor (not a direct share, owner, or admin). The
    frontend hides the General Access section for these users.

    ``parent_blocks`` lists parent dashboards whose visibility outranks the
    current resource, constraining which modes the dropdown can show.
    """

    mode: GeneralAccessMode  # "internal" | "private" | "public"
    supports_public: bool
    allow_public_sharing: bool  # org-level; when False the Public option is disabled
    public_url: Optional[str] = None
    public_access_count: int = 0
    last_public_accessed: Optional[str] = None
    caller_access_via_floor: bool = False
    parent_blocks: list[ParentBlockSchema] = []


class GrantsListResponse(Schema):
    """Wrapper returned by ``GET /api/access/{rtype}/{id}/grants``.

    ``caller_is_owner`` is True when the requesting user is the resource
    creator (``created_by``). The frontend uses this to decide whether to
    show the Transfer Ownership option.
    """

    shares: list[ShareRowSchema]
    caller_is_owner: bool
    general_access: GeneralAccessState
    owner: Optional[OwnerInfo] = None


# ---------------------------------------------------------------------------
# Write


class PrincipalGrantPayload(Schema):
    """One row for a concrete principal (existing orguser or group)."""

    principal_type: PrincipalType
    principal_id: int
    access_level: AccessLevel


class PendingGrantPayload(Schema):
    """One row for an email that is not yet an orguser. Backend creates an
    ``Invitation`` (using ``invite_role_uuid`` from the parent payload) and
    stores the share pointing at it. On acceptance the share is promoted to
    a direct user grant.
    """

    email: str
    access_level: AccessLevel


class AddGrantsPayload(Schema):
    """Body for ``POST /api/access/{rtype}/{id}/grants`` — staged chips from
    the share modal, applied in one call.
    """

    principals: list[PrincipalGrantPayload] = []
    pending_grants: list[PendingGrantPayload] = []
    invite_role_uuid: Optional[str] = None  # required when pending_grants is non-empty


class AddGrantsResponse(Schema):
    """Response body for ``POST /api/access/{rtype}/{id}/grants``.

    ``warnings`` carries advisory messages for the frontend (e.g. the resource
    owner is a member of a shared group — the share is still created but owner
    access is unaffected).
    """

    shares: list[ShareRowSchema]
    warnings: list[str] = []


class UpdateGrantPayload(Schema):
    """Body for ``PATCH /api/access/{rtype}/{id}/grants/{share_id}`` — the
    per-row access-level dropdown change.
    """

    access_level: AccessLevel


class GeneralAccessPayload(Schema):
    """Body for ``PATCH /api/access/{rtype}/{id}/general-access``.

    Maps 1:1 to the "General access" dropdown in the share modal:
    - ``internal`` — org-wide role-floor access, no public link
    - ``private`` — only explicitly-shared users
    - ``public`` — org-wide access plus anyone with the link
    """

    mode: GeneralAccessMode


class TransferOwnershipPayload(Schema):
    """Body for ``POST /api/access/{rtype}/{id}/transfer-ownership``."""

    to_orguser_id: int
    strip_previous_owner_access: bool = False


class TransferCandidateSchema(Schema):
    """One row in the transfer-ownership picker. ``access_level`` is the user's
    effective level on the resource; only ``edit`` candidates are selectable.
    ``is_owner`` marks the current owner (client hides them from the picker)."""

    orguser_id: int
    email: str
    role_name: Optional[str] = None
    access_level: EffectiveAccessLevel  # "no_access" | "view" | "edit"
    is_owner: bool = False


# ---------------------------------------------------------------------------
# Request access


class RequestAccessPayload(Schema):
    """Body for ``POST /api/access/{rtype}/{id}/request-access``."""

    requested_level: AccessLevel
    note: Optional[str] = None


class AccessRequestSchema(Schema):
    """One pending access request — returned by the list/respond endpoints."""

    id: int
    requester_id: int
    requester_email: str
    requested_level: str
    note: Optional[str] = None
    status: str
    created_at: str


class RespondToRequestPayload(Schema):
    """Body for ``POST /api/access/{rtype}/{id}/request-access/{req_id}/respond``."""

    decision: Literal["approved", "declined"]
    granted_level: Optional[AccessLevel] = None  # defaults to requested_level when approved
