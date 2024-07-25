from dotenv import load_dotenv
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.constants import SYSTEM_USER_EMAIL

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    Adds user for superset usage dashboard
    """

    help = "Adds auth user and orguser that system will use to lock scheduled pipelines"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        pass

    def handle(self, *args, **options):
        """create orguser with null org and auth user with no password"""
        user = User.objects.create(
            email=SYSTEM_USER_EMAIL,
            username=SYSTEM_USER_EMAIL,
            password="",
        )
        logger.info("create auth user")
        OrgUser.objects.create(user=user, org=None, role=OrgUserRole.ACCOUNT_MANAGER)
        logger.info(f"created system orguser")
