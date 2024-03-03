from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDbt
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

class Command(BaseCommand):
    help = 'Updates OrgDbt objects with transformation_type = "github"'

    def handle(self, *args, **options):
        org_dbt_objects = OrgDbt.objects.all()

        if not org_dbt_objects.exists():
            logger.error('No OrgDbt objects found in the database')
            return

        for obj in org_dbt_objects:
            obj.transform_type = "github"
            obj.save()

        logger.info('Successfully updated OrgDbt objects')
