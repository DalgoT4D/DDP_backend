from django.core.management.base import BaseCommand
from ddpui.models.org import Org, OrgWarehouse
from ddpui.core.sqlgeneration_service import SqlGeneration
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    help = "Setup chat with data for an org"

    def add_arguments(self, parser):
        parser.add_argument("org_slug", type=str, help="Org slug")

    def handle(self, *args, **options):
        org_slug = options["org_slug"]
        try:
            org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            logger.error(f"Org with slug '{org_slug}' does not exist.")
            return

        try:
            warehouse = OrgWarehouse.objects.get(org=org)
        except OrgWarehouse.DoesNotExist:
            logger.error(f"Warehouse for org '{org_slug}' does not exist.")
            return

        # Assuming SqlGeneration class has a method to create embeddings
        sql_generation = SqlGeneration(warehouse)
        try:
            sql_generation.setup_training_plan_and_execute()
            logger.info(f"Embeddings created successfully for org '{org_slug}'.")
        except Exception as e:
            logger.error(f"Error creating embeddings for org '{org_slug}': {e}")
