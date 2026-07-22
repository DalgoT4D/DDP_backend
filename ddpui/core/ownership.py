from ddpui.auth import ADMIN_ROLE, SUPER_ADMIN_ROLE
from ddpui.models.org_user import OrgUser


def is_admin_or_super_admin(orguser: OrgUser) -> bool:
    """True if orguser holds the admin or super-admin role. `accessible_filter`
    is deliberately not admin-aware, so list endpoints check this themselves."""
    return orguser.new_role is not None and orguser.new_role.slug in (
        ADMIN_ROLE,
        SUPER_ADMIN_ROLE,
    )


def is_owner(orguser: OrgUser, resource) -> bool:
    """True if orguser is literally the resource's owner: ``owner_id``, falling
    back to ``created_by_id`` when owner is null. No admin override, unlike
    ``can_delete_resource``. Mirrors ``access_resolver._is_owner``."""
    owner_id = getattr(resource, "owner_id", None)
    if owner_id is not None:
        return owner_id == orguser.id
    created_by_id = getattr(resource, "created_by_id", None)
    return created_by_id is not None and created_by_id == orguser.id


def can_delete_resource(orguser: OrgUser, resource) -> bool:
    """Return True if orguser may delete resource.

    Allowed when:
    - orguser is the resource's owner (``owner_id``), OR
    - ``owner_id`` is null and orguser created the resource (``created_by_id``) —
      a fallback for rows that predate the ``owner`` column or whose owner was
      cleared by a SET_NULL on user deletion, OR
    - orguser holds the admin or super-admin role (org-level override).

    Uses getattr-safe access since some resources passed in (e.g. legacy mocks
    in tests) may not define ``owner``.
    """
    owner_id = getattr(resource, "owner_id", None)
    if owner_id is not None:
        if owner_id == orguser.id:
            return True
    else:
        created_by_id = getattr(resource, "created_by_id", None)
        if created_by_id is not None and created_by_id == orguser.id:
            return True
    return is_admin_or_super_admin(orguser)


def is_creator_or_admin(orguser: OrgUser, resource) -> bool:
    """Main's name for the owner-level policy (delete, sharing management).
    On this branch ownership is transferable, so it resolves owner-first
    (``owner_id``, ``created_by`` fallback) or admin — not creator-only."""
    return can_delete_resource(orguser, resource)
