"""these need to be moved somewhere else"""

from ddpui.models.org_user import OrgUser
from ddpui.models.permissions import Permission, Verb, Target, UserPermissions


def has_permission(orguser: OrgUser, verb: Verb, target: Target) -> bool:
    """check if a user has a permission"""
    permission = Permission.objects.filter(verb=verb, target=target).first()
    if permission:
        userpermission = UserPermissions.objects.filter(
            orguser=orguser, permission=permission
        ).first()
        if userpermission:
            return userpermission.is_able
        return permission.default_is_able
    return False
