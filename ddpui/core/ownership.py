from ddpui.models.org_user import OrgUser


def can_delete_resource(orguser: OrgUser, resource) -> bool:
    """Return True if orguser may delete resource.

    Allowed when:
    - orguser created the resource (the creator is its owner), OR
    - orguser holds the admin role (org-level override).

    Ownership is keyed off ``created_by`` — always populated at creation time — so
    there is no separate nullable owner column to backfill or keep in sync.
    """
    if resource.created_by_id is not None and resource.created_by_id == orguser.id:
        return True
    return orguser.new_role.slug == "admin"
