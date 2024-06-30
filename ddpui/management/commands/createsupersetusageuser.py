from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.secretsmanager import save_superset_usage_dashboard_credentials

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    Adds user for superset usage dashboard
    """

    help = "Adds user for superset usage dashboard"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument("--username", required=True)
        parser.add_argument("--first-name", required=True)
        parser.add_argument("--last-name", required=True)
        parser.add_argument("--password", required=True)

    def handle(self, *args, **options):
        """adds superset credentials to secrets manager"""
        secret_id = save_superset_usage_dashboard_credentials(
            {
                "username": options["username"],
                "first_name": options["first_name"],
                "last_name": options["last_name"],
                "password": options["password"],
            },
        )
        logger.info(f"credentials saved to secretId = {secret_id}")
