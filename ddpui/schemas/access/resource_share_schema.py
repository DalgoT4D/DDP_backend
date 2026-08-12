"""Payloads and responses for the resource-share (grants) API.

Every dict shape used by ``ddpui.core.access.resource_share`` is declared
here so callers can see the contract without reading the implementation.
"""

from typing import Literal, Optional

from ninja import Schema


AccessLevel = Literal["view", "edit"]
PrincipalType = Literal["user", "group"]
ShareRowKind = Literal["user", "group", "invitation"]
ShareRowStatus = Literal["active", "pending"]


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


class GrantsListResponse(Schema):
    """Wrapper returned by ``GET /api/access/{rtype}/{id}/grants``.

    ``caller_is_owner`` is True when the requesting user is the resource
    creator (``created_by``). The frontend uses this to decide whether to
    show the Transfer Ownership option.
    """

    shares: list[ShareRowSchema]
    caller_is_owner: bool


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


class UpdateGrantPayload(Schema):
    """Body for ``PATCH /api/access/{rtype}/{id}/grants/{share_id}`` — the
    per-row access-level dropdown change.
    """

    access_level: AccessLevel


class PrivateTogglePayload(Schema):
    """Body for ``PATCH /api/access/{rtype}/{id}/private``."""

    is_private: bool


class TransferOwnershipPayload(Schema):
    """Body for ``POST /api/access/{rtype}/{id}/transfer-ownership``."""

    to_orguser_id: int


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
