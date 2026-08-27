from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import (
    AccessLevel,
    ResourceShare,
    ResourceSharePrincipalType,
    ResourceType,
)
from ddpui.models.role_based_access import RoleSlug


class OwnershipError(Exception):
    """Business-rule violation with a user-facing message. The API layer
    catches this and returns 400."""


def is_creator_or_admin(orguser: OrgUser, resource) -> bool:
    """Return True if orguser created the resource or holds an admin role.

    The shared owner-level policy for administrative actions on a resource
    (delete, sharing management):
    - orguser created the resource (the creator is its owner), OR
    - orguser holds the admin or super-admin role (org-level override).

    Ownership is keyed off ``created_by`` — populated at creation time but
    nullable (SET_NULL when the creator's OrgUser is deleted), so orphaned
    resources remain manageable by admins.
    """
    if resource.created_by_id is not None and resource.created_by_id == orguser.id:
        return True
    if orguser.new_role is None:
        return False
    return orguser.new_role.slug in (RoleSlug.ADMIN, RoleSlug.SUPER_ADMIN)


def transfer_ownership(caller: OrgUser, rtype: str, resource, to_orguser_id: int) -> None:
    """Transfer ``created_by`` on ``resource`` to another org member.

    Rules:
    - Caller must be the current owner or Admin.
    - Recipient must be in the same org.
    - Recipient must already have effective Edit access (floor or direct grant).
    - Previous owner is guaranteed Edit access after transfer: their existing
      direct share is updated to Edit, or a new Edit share is created if none exists.
    """
    from ddpui.core.access.access_control import get_user_access  # late import — avoid cycle

    if not is_creator_or_admin(caller, resource):
        raise OwnershipError("only the owner or an admin can transfer ownership")

    if caller.org is None:
        raise OwnershipError("no associated org")

    to_orguser = OrgUser.objects.filter(org=caller.org, id=to_orguser_id).first()
    if to_orguser is None:
        raise OwnershipError("recipient not found in this org")

    if to_orguser.id == resource.created_by_id:
        return  # no-op: transferring to current owner

    effective = get_user_access(to_orguser, rtype, resource.pk)
    if effective != AccessLevel.EDIT:
        raise OwnershipError(
            "recipient does not have Edit access on this resource; "
            "share it with them first or ensure their role floor is Edit"
        )

    previous_owner_id = resource.created_by_id

    resource.created_by = to_orguser
    resource.save(update_fields=["created_by"])

    # Ensure the previous owner retains Edit access.
    if previous_owner_id is not None:
        share, created = ResourceShare.objects.get_or_create(
            org=caller.org,
            resource_type=rtype,
            resource_id=str(resource.pk),
            principal_type=ResourceSharePrincipalType.USER,
            principal_id=previous_owner_id,
            parent=None,
            defaults={"access_level": AccessLevel.EDIT, "created_by": caller},
        )
        if not created and share.access_level != AccessLevel.EDIT:
            share.access_level = AccessLevel.EDIT
            share.save(update_fields=["access_level"])

    # Re-sync the dashboard cascade so:
    #  - the previous owner's cascade rows flip from VIEW → EDIT (they're now a
    #    regular Edit-share holder, not the owner self-share).
    #  - the new owner gets their own auto self-share + VIEW cascade rows on
    #    every inner chart/KPI, materialising their access against future
    #    org-floor changes.
    if rtype == ResourceType.DASHBOARD:
        from ddpui.core.access.resource_share import sync_dashboard_cascade

        sync_dashboard_cascade(resource)
