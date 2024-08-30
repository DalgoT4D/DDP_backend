"""Helper functions for OrgUser objects"""

from django.utils.text import slugify

from ddpui.models.org_user import Invitation, InvitationSchema
from ddpui.models.org_user import OrgUser, OrgUserResponse, OrgUserRole
from ddpui.models.org import OrgWarehouse, OrgType
from ddpui.models.orgtnc import OrgTnC
from ddpui.models.role_based_access import RolePermission


def from_orguser(orguser: OrgUser):
    """helper to turn an OrgUser into an OrgUserResponse"""
    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    orguser_new_role = orguser.new_role.slug if orguser.new_role else None
    permissions = []
    if orguser_new_role:
        role_permissions = list(
            RolePermission.objects.filter(role=orguser.new_role).all()
        )
        permissions = [
            {"slug": item.permission.slug, "name": item.permission.name}
            for item in role_permissions
        ]

    response = OrgUserResponse(
        email=orguser.user.email,
        org=orguser.org,
        active=orguser.user.is_active,
        role=orguser.role,
        role_slug=slugify(OrgUserRole(orguser.role).name),
        new_role_slug=orguser_new_role,
        permissions=permissions,
        wtype=warehouse.wtype if warehouse else None,
        is_demo=orguser.org.type == OrgType.DEMO if orguser.org else False,
    )
    if orguser.org:
        response.org.tnc_accepted = OrgTnC.objects.filter(org=orguser.org).exists()
    return response


def from_invitation(invitation: Invitation):
    """Docstring"""
    return InvitationSchema(
        invited_email=invitation.invited_email,
        invited_role=invitation.invited_role,
        invited_role_slug=slugify(OrgUserRole(invitation.invited_role).name),
        invited_by=from_orguser(invitation.invited_by),
        invited_on=invitation.invited_on,
        invite_code=invitation.invite_code,
        invited_new_role_slug=invitation.invited_new_role.slug,
    )
