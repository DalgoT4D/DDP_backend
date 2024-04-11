from django.core.management.base import BaseCommand

from ddpui.models.org_user import OrgUser
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
            OrgUser.objects.update(new_role=role)
