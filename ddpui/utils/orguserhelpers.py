"""Helper functions for OrgUser objects"""

from django.utils.text import slugify

from ddpui.auth import PLATFORM_ADMIN_PERMISSION
from ddpui.models.org_user import Invitation, InvitationSchema
from ddpui.models.org_user import OrgUser, OrgUserResponse
from ddpui.models.org import OrgWarehouse, OrgType
from ddpui.models.orgtnc import OrgTnC
from ddpui.models.role_based_access import Role


def permissions_for_role(role: Role) -> list[dict]:
    """the {slug, name} permission list for a Role, as OrgUserResponse carries it"""
    # `.all()` and not `.filter()`/`.select_related()`: callers that prefetched
    # rolepermissions (currentuserv2, get_organization_users) must hit the cache
    return [
        {"slug": rolep.permission.slug, "name": rolep.permission.name}
        for rolep in role.rolepermissions.all()
    ]


def holds_platform_admin(permissions: list[dict]) -> bool:
    """True when a permissions_for_role list grants the Admin Portal"""
    return any(perm["slug"] == PLATFORM_ADMIN_PERMISSION for perm in permissions)


def from_orguser(orguser: OrgUser) -> OrgUserResponse:
    """helper to turn an OrgUser into an OrgUserResponse"""
    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    orguser_new_role = orguser.new_role.slug

    if orguser_new_role is None:
        raise ValueError("OrgUser does not have a new_role set")

    permissions = permissions_for_role(orguser.new_role)

    response = OrgUserResponse(
        user_id=orguser.user.id,
        email=orguser.user.email,
        org=orguser.org,
        active=orguser.user.is_active,
        new_role_slug=orguser_new_role,
        permissions=permissions,
        wtype=warehouse.wtype if warehouse else None,
        is_demo=orguser.org.base_plan() == OrgType.DEMO if orguser.org else False,
        subscription_plan=orguser.org.base_plan() if orguser.org else None,
        work_domain=orguser.work_domain,
        has_seen_rbac_notice=orguser.has_seen_rbac_notice,
        is_platform_admin=holds_platform_admin(permissions),
    )
    if orguser.org:
        response.org.tnc_accepted = OrgTnC.objects.filter(org=orguser.org).exists()
    return response


def from_invitation(invitation: Invitation):
    """Docstring"""
    return InvitationSchema(
        invited_email=invitation.invited_email,
        invited_by=from_orguser(invitation.invited_by),
        invited_on=invitation.invited_on,
        invite_code=invitation.invite_code,
        invited_new_role_slug=invitation.invited_new_role.slug,
    )
