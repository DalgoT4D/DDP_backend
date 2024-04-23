from django.core.management.base import BaseCommand

from ddpui.models.org_user import OrgUser, Invitation
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE


class Command(BaseCommand):
    """
    Scripts needed to move current clients to role based access
    """

    def handle(self, *args, **options):
        # Update all orgusers to have role of Super admin

        role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
        if role:
            OrgUser.objects.filter(new_role__isnull=True).update(new_role=role)
            Invitation.objects.filter(invited_new_role__isnull=True).update(
                invited_new_role=role
            )
