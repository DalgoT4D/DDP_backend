"""Helper functions for OrgUser objects"""

from django.utils.text import slugify

from ddpui.models.org_user import Invitation, InvitationSchema
from ddpui.models.org_user import OrgUser, OrgUserResponse, OrgUserRole
from ddpui.models.org import OrgWarehouse
from ddpui.models.orgtnc import OrgTnC


def from_orguser(orguser: OrgUser):
    """helper to turn an OrgUser into an OrgUserResponse"""
    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    response = OrgUserResponse(
        email=orguser.user.email,
        org=orguser.org,
        active=orguser.user.is_active,
        role=orguser.role,
        role_slug=slugify(OrgUserRole(orguser.role).name),
        wtype=warehouse.wtype if warehouse else None,
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
    )
