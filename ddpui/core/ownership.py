from ddpui.models.org_user import OrgUser


def can_delete_resource(orguser: OrgUser, resource) -> bool:
    """Return True if orguser may delete resource.

    Allowed when:
    - orguser is the resource's owner, OR
    - orguser holds the admin role (org-level override).
    """
    if resource.owner_id is not None and resource.owner_id == orguser.id:
        return True
    return orguser.new_role.slug == "admin"
