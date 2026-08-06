from ddpui.auth import ADMIN_ROLE, SUPER_ADMIN_ROLE
from ddpui.models.org_user import OrgUser


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
    return orguser.new_role.slug in (ADMIN_ROLE, SUPER_ADMIN_ROLE)


def can_delete_resource(orguser: OrgUser, resource) -> bool:
    """Return True if orguser may delete resource — creator or admin."""
    return is_creator_or_admin(orguser, resource)
