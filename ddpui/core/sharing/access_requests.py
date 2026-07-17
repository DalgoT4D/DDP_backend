"""Request-access flow: a user without access asks, an owner (or admin)
decides. Approving inserts a ``ResourceShare`` grant — the only sharing
write outside ``sharing_actions``. Reuses that module's private helpers so
owner resolution stays identical to the resolver's; ``access_resolver``
stays read-only.
"""

from typing import Optional

from django.db import transaction
from django.utils import timezone as django_timezone

from ddpui.auth import ADMIN_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core.sharing.access_resolver import PERMISSION_RANK, effective_permission
from ddpui.core.sharing.deep_links import (
    DEEP_LINK_PATH as _DEEP_LINK_PATH,  # noqa: F401  (re-exported; extracted to deep_links)
    NOUN_BY_RTYPE as _NOUN_BY_RTYPE,
    build_resource_url as _build_resource_url,
    frontend_url as _frontend_url,
    resource_label as _resource_label,
)
from ddpui.core.sharing.exceptions import SharingValidationError
from ddpui.core.sharing.sharing_actions import _entry_for, _orguser_name, _owner_orguser
from ddpui.core.sharing.shareable_types import get_resource_type
from ddpui.models.access_request import AccessRequest, default_access_request_expiry
from ddpui.models.general_access import GeneralLevel
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.schemas.access_schema import (
    AccessRequestCreate,
    AccessRequestDecision,
    AccessRequestListResponse,
    AccessRequestOut,
    RequesterOut,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.sharing.access_requests")


def _requester_out(orguser: Optional[OrgUser]) -> Optional[RequesterOut]:
    if orguser is None:
        return None
    return RequesterOut(
        orguser_id=orguser.id, email=orguser.user.email, name=_orguser_name(orguser)
    )


def _request_out(access_request: AccessRequest) -> AccessRequestOut:
    return AccessRequestOut(
        id=access_request.id,
        resource_type=access_request.resource_type,
        resource_id=access_request.resource_id,
        requester=_requester_out(access_request.requester),
        requested_permission=access_request.requested_permission,
        note=access_request.note,
        status=access_request.status,
        decided_by=_requester_out(access_request.decided_by),
        expires_at=access_request.expires_at,
        created_at=access_request.created_at,
    )


def _notify(
    recipient: OrgUser,
    author_email: str,
    message: str,
    email_subject: str,
    metadata: Optional[dict] = None,
) -> None:
    """One in-app Notification + NotificationRecipient row, sent immediately.
    `metadata` is the structured payload the Notifications page reads to render
    inline Approve/Deny on a "new request" row; None renders as plain text."""
    notification = Notification.objects.create(
        author=author_email,
        message=message,
        email_subject=email_subject,
        urgent=False,
        sent_time=django_timezone.now(),
        metadata=metadata,
    )
    NotificationRecipient.objects.create(notification=notification, recipient=recipient)


def _notify_new_request(
    requester: OrgUser, rtype: str, resource, access_request: AccessRequest
) -> None:
    """Notify the resource owner (fallback: org admins/super-admins if the
    owner is null) that a new request landed."""
    owner = _owner_orguser(resource)
    recipients = (
        [owner]
        if owner is not None
        else list(
            OrgUser.objects.filter(
                org_id=resource.org_id, new_role__slug__in=(ADMIN_ROLE, SUPER_ADMIN_ROLE)
            )
        )
    )
    if not recipients:
        return

    noun = _NOUN_BY_RTYPE.get(rtype, rtype)
    label = _resource_label(rtype, resource)
    requester_name = _orguser_name(requester)
    link = _build_resource_url(rtype, access_request.resource_id)
    message = (
        f"{requester_name} requested {access_request.requested_permission} access to "
        f'your {noun} "{label}". {link}'
    )
    subject = f'{requester_name} requested access to "{label}"'
    # `kind` discriminates future actionable-notification types. Only the
    # new-request notification gets metadata — a decision has nothing to act on.
    metadata = {
        "kind": "access_request",
        "request_id": access_request.id,
        "resource_type": rtype,
        "resource_name": label,
        "requester_email": requester.user.email,
        "requested_permission": access_request.requested_permission,
    }
    for recipient in recipients:
        _notify(recipient, requester.user.email, message, subject, metadata=metadata)


def _notify_decision(
    decider: OrgUser, rtype: str, resource, access_request: AccessRequest, granted_permission: str
) -> None:
    """Notify the requester of the owner's decision."""
    requester = access_request.requester
    if requester is None:
        return

    noun = _NOUN_BY_RTYPE.get(rtype, rtype)
    label = _resource_label(rtype, resource)
    decider_name = _orguser_name(decider)
    link = _build_resource_url(rtype, access_request.resource_id)

    if access_request.status == AccessRequest.STATUS_APPROVED:
        message = (
            f"{decider_name} granted you {granted_permission} access to the {noun} "
            f'"{label}". {link}'
        )
        subject = f'Access request approved: "{label}"'
    else:
        message = f'{decider_name} declined your access request for the {noun} "{label}". {link}'
        subject = f'Access request declined: "{label}"'

    _notify(requester, decider.user.email, message, subject)


def _ensure_decidable(access_request: AccessRequest) -> None:
    if access_request.status != AccessRequest.STATUS_PENDING:
        raise SharingValidationError(f"this request has already been {access_request.status}")
    if access_request.expires_at < django_timezone.now():
        raise SharingValidationError("this request has expired")


def get_access_request_or_none(org_id, request_id: int) -> Optional[AccessRequest]:
    """Fetch an ``AccessRequest`` scoped to `org_id` — a cross-org id is
    indistinguishable from a nonexistent one."""
    return (
        AccessRequest.objects.select_related(
            "requester", "requester__user", "decided_by", "decided_by__user"
        )
        .filter(id=request_id, org_id=org_id)
        .first()
    )


def create_access_request(
    requester: OrgUser, rtype: str, resource, payload: AccessRequestCreate
) -> AccessRequestOut:
    """Ask for access to `resource`. 400s if the rtype doesn't support requests,
    the permission is invalid, or the requester already has access. A duplicate
    pending request (including one past expiry but not yet swept) is refreshed
    in place instead of stacking a second row, and does not re-notify the owner."""
    entry = _entry_for(rtype)
    if not entry.requests:
        raise SharingValidationError(f"{rtype} does not support access requests")

    # On member_sharing=False rtypes a Member's request could never be
    # granted, so block it with a pointer at the path that does work.
    requester_slug = requester.new_role.slug if requester.new_role else None
    if not entry.member_sharing and requester_slug == MEMBER_ROLE:
        noun = _NOUN_BY_RTYPE.get(rtype, rtype)
        raise SharingValidationError(
            f"{noun}s can't be shared with Members yet — request access to "
            f"the dashboard instead"
        )

    if payload.requested_permission not in GeneralLevel.values:
        raise SharingValidationError(f"invalid permission '{payload.requested_permission}'")

    # Reject only when current access already covers the ask — a View holder
    # requesting Edit is a legitimate upgrade request.
    current = effective_permission(requester, rtype, resource)
    if current == GeneralLevel.EDIT or current == payload.requested_permission:
        raise SharingValidationError("you already have this access to this resource")

    existing = AccessRequest.objects.filter(
        org_id=requester.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
        requester=requester,
        status=AccessRequest.STATUS_PENDING,
    ).first()
    if existing is not None:
        existing.requested_permission = payload.requested_permission
        existing.note = payload.note
        existing.expires_at = default_access_request_expiry()
        existing.save(update_fields=["requested_permission", "note", "expires_at"])
        return _request_out(existing)

    access_request = AccessRequest.objects.create(
        org_id=requester.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
        requester=requester,
        requested_permission=payload.requested_permission,
        note=payload.note,
    )
    _notify_new_request(requester, rtype, resource, access_request)
    return _request_out(access_request)


def _decidable_resource_ids(viewer: OrgUser, rtype: str, resource_ids: list) -> set:
    """The subset of `resource_ids` the viewer may decide requests for — the
    resources they own. Admins short-circuit at the caller."""
    model = get_resource_type(rtype).model
    owned = set()
    for pk, owner_id, created_by_id in model.objects.filter(
        pk__in=resource_ids, org_id=viewer.org_id
    ).values_list("pk", "owner_id", "created_by_id"):
        resolved_owner_id = owner_id if owner_id is not None else created_by_id
        if resolved_owner_id == viewer.id:
            owned.add(str(pk))
    return owned


def list_access_requests(viewer: OrgUser) -> AccessRequestListResponse:
    """`incoming`: pending requests on resources the viewer can decide (owner,
    or admin — admins see every pending request in-org). `outgoing`: the
    viewer's own requests, any status."""
    outgoing = list(
        AccessRequest.objects.filter(org_id=viewer.org_id, requester=viewer)
        .select_related("requester", "requester__user", "decided_by", "decided_by__user")
        .order_by("-created_at")
    )

    pending = list(
        AccessRequest.objects.filter(org_id=viewer.org_id, status=AccessRequest.STATUS_PENDING)
        .exclude(requester=viewer)
        .select_related("requester", "requester__user", "decided_by", "decided_by__user")
        .order_by("-created_at")
    )

    is_admin = viewer.new_role is not None and viewer.new_role.slug in (
        ADMIN_ROLE,
        SUPER_ADMIN_ROLE,
    )
    if is_admin:
        incoming = pending
    else:
        incoming = []
        by_rtype: dict = {}
        for req in pending:
            by_rtype.setdefault(req.resource_type, []).append(req)
        for rtype, reqs in by_rtype.items():
            entry = get_resource_type(rtype)
            if entry is None:
                continue
            owned_ids = _decidable_resource_ids(viewer, rtype, [r.resource_id for r in reqs])
            incoming.extend(r for r in reqs if r.resource_id in owned_ids)
        incoming.sort(key=lambda r: r.created_at, reverse=True)

    return AccessRequestListResponse(
        incoming=[_request_out(r) for r in incoming],
        outgoing=[_request_out(r) for r in outgoing],
    )


def _insert_grant(
    actor: OrgUser, rtype: str, resource, principal: OrgUser, permission: str
) -> None:
    """Grant write for approve. Deliberately bypasses `upsert_grant`'s
    `entry.grants` flag, the Member-grants-deferred check, and the
    dashboard-broadening warning: the owner is deciding a request that
    already named the resource, not making a proactive share. Not a Member
    back door onto member_sharing=False rtypes — Member requesters are
    blocked upstream, and the resolver drops Member grant contributions
    on charts at read time anyway."""
    ResourceShare.objects.update_or_create(
        org_id=resource.org_id,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal.id,
        defaults={
            "permission": permission,
            "status": "active",
            "pending_email": None,
            "created_by": actor,
        },
    )


def approve_access_request(
    decider: OrgUser,
    access_request: AccessRequest,
    resource,
    payload: AccessRequestDecision,
) -> AccessRequestOut:
    """Grant the request: insert a `ResourceShare` row and mark the request
    approved. `payload.permission` may only downgrade the original ask
    (Edit -> View), never escalate above it. 400s on an already-decided or
    expired request."""
    _ensure_decidable(access_request)

    permission = payload.permission or access_request.requested_permission
    if permission not in GeneralLevel.values:
        raise SharingValidationError(f"invalid permission '{permission}'")
    if PERMISSION_RANK.get(permission, 0) > PERMISSION_RANK.get(
        access_request.requested_permission, 0
    ):
        raise SharingValidationError("cannot grant a higher level than was requested")

    if access_request.requester is None:
        raise SharingValidationError("the requester no longer exists")

    with transaction.atomic():
        _insert_grant(
            decider, access_request.resource_type, resource, access_request.requester, permission
        )
        access_request.status = AccessRequest.STATUS_APPROVED
        access_request.decided_by = decider
        access_request.save(update_fields=["status", "decided_by", "updated_at"])

    _notify_decision(decider, access_request.resource_type, resource, access_request, permission)
    return _request_out(access_request)


def decline_access_request(
    decider: OrgUser, access_request: AccessRequest, resource
) -> AccessRequestOut:
    """Decline the request: no grant, notify the requester. 400s on an
    already-decided or expired request."""
    _ensure_decidable(access_request)

    access_request.status = AccessRequest.STATUS_DECLINED
    access_request.decided_by = decider
    access_request.save(update_fields=["status", "decided_by", "updated_at"])

    _notify_decision(decider, access_request.resource_type, resource, access_request, None)
    return _request_out(access_request)


def expire_stale_requests() -> int:
    """Daily cleanup: mark pending rows past `expires_at` as expired.
    approve/decline 400 on expired rows on their own; this sweep just keeps
    the pending inbox/badge honest."""
    updated = AccessRequest.objects.filter(
        status=AccessRequest.STATUS_PENDING, expires_at__lt=django_timezone.now()
    ).update(status=AccessRequest.STATUS_EXPIRED)
    if updated:
        logger.info(f"expire_stale_requests: marked {updated} request(s) expired")
    return updated
