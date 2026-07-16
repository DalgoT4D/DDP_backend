"""Groups service: create/rename/delete ``UserGroup`` rows and manage their
membership. Kept outside ``core/sharing`` — ``ResourceShare`` only ever sees
a group by id via its soft ``principal_type="group"`` pointer.

No HTTP concerns: raise ``ddpui.core.user_groups.exceptions``; the API layer
maps them to status codes. "Creator or Admin" gates every mutation of a
specific group; base create/read access is checked by ``@has_permission``.
"""

from typing import List, Optional

from django.conf import settings
from django.db import transaction
from django.db.models import Count, IntegerField, OuterRef, Prefetch, Q, Subquery
from django.db.models.functions import Coalesce

from ddpui.core.ownership import is_admin_or_super_admin
from ddpui.core.sharing.exceptions import SharingPermissionError, SharingValidationError

# The one place Groups depends on core/sharing: reuse the share-flow invite
# primitive rather than re-implementing invite/Invitation handling here.
from ddpui.core.sharing.sharing_actions import _invite_email_once
from ddpui.core.user_groups.exceptions import (
    GroupNameCollisionError,
    GroupNotFoundError,
    GroupPermissionError,
    GroupValidationError,
    MemberNotFoundError,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.schemas.group_schema import (
    GroupCreate,
    GroupCreatorOut,
    GroupDetailOut,
    GroupMemberCreate,
    GroupMemberOut,
    GroupOut,
    GroupUpdate,
)
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.user_groups.user_groups_service")


def _workspace_url() -> str:
    """Absolute frontend root for the "Explore Workspace" CTA. Kept local so
    user_groups stays free of any core/sharing dependency."""
    return (
        getattr(settings, "FRONTEND_URL_V2", None)
        or getattr(settings, "FRONTEND_URL", None)
        or "http://localhost:3001"
    )


def _notify_added_to_group(adder: OrgUser, group: UserGroup, member: OrgUser) -> None:
    """Email a newly-added active member that they were added to `group`.
    Best-effort: a send failure is logged and swallowed, never failing the add."""
    try:
        awsses.send_added_to_group_email(
            to_email=member.user.email,
            group_name=group.name,
            added_by_email=adder.user.email,
            workspace_url=_workspace_url(),
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            f"failed to send added-to-group email to {member.user.email} for group {group.id}"
        )


def _display_name(user) -> str:
    """Display name convention used across the codebase."""
    return f"{user.first_name} {user.last_name}".strip() or user.email


def _creator_out(orguser: Optional[OrgUser]) -> Optional[GroupCreatorOut]:
    if orguser is None:
        return None
    return GroupCreatorOut(
        orguser_id=orguser.id, email=orguser.user.email, name=_display_name(orguser.user)
    )


def _member_out(member: UserGroupMember) -> GroupMemberOut:
    orguser = member.orguser
    return GroupMemberOut(
        id=member.id,
        orguser_id=member.orguser_id,
        email=orguser.user.email if orguser else None,
        name=_display_name(orguser.user) if orguser else None,
        pending_email=member.pending_email,
        status=member.status,
        # Pending-email rows have no orguser -> role stays None.
        role=orguser.new_role.slug if orguser and orguser.new_role_id else None,
    )


def _shared_resource_count_expr():
    """Correlated count of active ``ResourceShare`` rows granted to a group, as
    a queryset annotation — one query for the whole list."""
    counts = (
        ResourceShare.objects.filter(
            principal_type="group",
            principal_id=OuterRef("pk"),
            status="active",
        )
        .values("principal_id")
        .annotate(c=Count("id"))
        .values("c")
    )
    return Coalesce(Subquery(counts[:1], output_field=IntegerField()), 0)


def _annotated_groups(org_id):
    """Every group in this org, annotated with `member_count` and
    `shared_resource_count` in the query itself (no N+1)."""
    return (
        UserGroup.objects.filter(org_id=org_id)
        .select_related("created_by__user")
        .annotate(
            annotated_member_count=Count(
                "members",
                filter=Q(members__status=UserGroupMemberStatus.ACTIVE),
                distinct=True,
            ),
            annotated_shared_resource_count=_shared_resource_count_expr(),
        )
    )


# How many member emails the list path returns for the Groups-table avatar stack.
MEMBER_PREVIEW_LIMIT = 4


def _group_out(group: UserGroup, member_preview: Optional[List[str]] = None) -> GroupOut:
    return GroupOut(
        id=group.id,
        name=group.name,
        member_count=group.annotated_member_count,
        shared_resource_count=group.annotated_shared_resource_count,
        created_by=_creator_out(group.created_by),
        created_at=group.created_at,
        member_preview=member_preview or [],
    )


def list_groups(orguser: OrgUser) -> List[GroupOut]:
    """All of the org's groups, with counts and a preview of up to
    ``MEMBER_PREVIEW_LIMIT`` active member emails (prefetched, no N+1)."""
    groups = (
        _annotated_groups(orguser.org_id)
        .order_by("name")
        .prefetch_related(
            Prefetch(
                "members",
                queryset=UserGroupMember.objects.filter(
                    status=UserGroupMemberStatus.ACTIVE, orguser__isnull=False
                )
                .select_related("orguser__user")
                .order_by("id"),
                to_attr="prefetched_active_members",
            )
        )
    )
    return [
        _group_out(
            g,
            member_preview=[
                m.orguser.user.email for m in g.prefetched_active_members[:MEMBER_PREVIEW_LIMIT]
            ],
        )
        for g in groups
    ]


def _get_group_or_404(orguser: OrgUser, group_id: int) -> UserGroup:
    """Cross-org must be indistinguishable from nonexistent -> 404 either way."""
    group = _annotated_groups(orguser.org_id).filter(id=group_id).first()
    if group is None:
        raise GroupNotFoundError("group not found")
    return group


def get_group(orguser: OrgUser, group_id: int) -> GroupDetailOut:
    """One group plus its members."""
    group = _get_group_or_404(orguser, group_id)
    members = list(
        group.members.select_related("orguser__user", "orguser__new_role").order_by("id")
    )
    return GroupDetailOut(
        id=group.id,
        name=group.name,
        member_count=group.annotated_member_count,
        shared_resource_count=group.annotated_shared_resource_count,
        created_by=_creator_out(group.created_by),
        created_at=group.created_at,
        members=[_member_out(m) for m in members],
    )


def _require_creator_or_admin(orguser: OrgUser, group: UserGroup) -> None:
    if group.created_by_id == orguser.id or is_admin_or_super_admin(orguser):
        return
    raise GroupPermissionError("only the group's creator or an Admin can do this")


def _clean_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise GroupValidationError("group name cannot be blank")
    return name


def create_group(orguser: OrgUser, payload: GroupCreate) -> GroupOut:
    """Create a group owned (as creator) by `orguser`. A name collision
    within the org fails cleanly, not as a 500 IntegrityError."""
    name = _clean_name(payload.name)
    if UserGroup.objects.filter(org_id=orguser.org_id, name=name).exists():
        raise GroupNameCollisionError(f"a group named '{name}' already exists in this org")

    group = UserGroup.objects.create(org_id=orguser.org_id, name=name, created_by=orguser)
    return _group_out(_annotated_groups(orguser.org_id).get(id=group.id))


def update_group(orguser: OrgUser, group_id: int, payload: GroupUpdate) -> GroupOut:
    """Rename a group. Creator or Admin only."""
    group = _get_group_or_404(orguser, group_id)
    _require_creator_or_admin(orguser, group)

    name = _clean_name(payload.name)
    if UserGroup.objects.filter(org_id=orguser.org_id, name=name).exclude(id=group.id).exists():
        raise GroupNameCollisionError(f"a group named '{name}' already exists in this org")

    group.name = name
    group.save(update_fields=["name"])
    return _group_out(_annotated_groups(orguser.org_id).get(id=group.id))


def delete_group(orguser: OrgUser, group_id: int) -> None:
    """Delete a group. Creator or Admin only. Also deletes its
    `ResourceShare` grant rows — a dangling group grant must not keep
    admitting people after the group is gone."""
    group = _get_group_or_404(orguser, group_id)
    _require_creator_or_admin(orguser, group)

    with transaction.atomic():
        ResourceShare.objects.filter(
            org_id=orguser.org_id, principal_type="group", principal_id=group.id
        ).delete()
        group.delete()  # cascades UserGroupMember rows via FK


def _add_active_member(adder: OrgUser, group: UserGroup, target: OrgUser) -> GroupMemberOut:
    """Create (or no-op onto) an active membership row for `target`, notifying
    on a genuinely new one. All add paths end up here so the idempotency and
    notification rule lives in exactly one place."""
    member, created = UserGroupMember.objects.get_or_create(
        group=group,
        orguser=target,
        defaults={"status": UserGroupMemberStatus.ACTIVE},
    )
    # Notify only on a genuinely new active membership for an active org user.
    if created and target.user.is_active:
        _notify_added_to_group(adder, group, target)
    return _member_out(member)


def add_member(orguser: OrgUser, group_id: int, payload: GroupMemberCreate) -> GroupMemberOut:
    """Add a member by OrgUser id or by email (exactly one). Creator or Admin
    only; adding an existing member is idempotent. An email matching an org
    member behaves like the orguser_id path; an unknown email goes through the
    share-flow invite and leaves a pending row, flipped to active on signup."""
    group = _get_group_or_404(orguser, group_id)
    _require_creator_or_admin(orguser, group)

    if payload.orguser_id is not None and payload.email:
        raise GroupValidationError("provide only one of orguser_id or email")
    if payload.orguser_id is None and not payload.email:
        raise GroupValidationError("orguser_id or email is required")

    if payload.orguser_id is not None:
        target = (
            OrgUser.objects.filter(id=payload.orguser_id, org_id=orguser.org_id)
            .select_related("user")
            .first()
        )
        if target is None:
            raise MemberNotFoundError("orguser not found in this organization")
        return _add_active_member(orguser, group, target)

    email = payload.email.strip().lower()
    target = (
        OrgUser.objects.filter(org_id=orguser.org_id, user__email__iexact=email)
        .select_related("user")
        .first()
    )
    if target is not None:
        return _add_active_member(orguser, group, target)

    try:
        instant_principal = _invite_email_once(orguser, email, payload.invite_role)
    except SharingPermissionError as err:
        raise GroupPermissionError(str(err)) from err
    except SharingValidationError as err:
        raise GroupValidationError(str(err)) from err

    if instant_principal is not None:
        return _add_active_member(orguser, group, instant_principal)

    member, _created = UserGroupMember.objects.get_or_create(
        group=group,
        pending_email=email,
        defaults={"status": UserGroupMemberStatus.PENDING},
    )
    return _member_out(member)


def remove_member(orguser: OrgUser, group_id: int, member_id: int) -> None:
    """Remove one membership row. Creator or Admin only. The row must
    belong to this group."""
    group = _get_group_or_404(orguser, group_id)
    _require_creator_or_admin(orguser, group)

    deleted, _ = UserGroupMember.objects.filter(id=member_id, group=group).delete()
    if deleted == 0:
        raise MemberNotFoundError("member not found for this group")
