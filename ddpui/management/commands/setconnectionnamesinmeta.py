from django.core.management.base import BaseCommand
from ddpui.models.org import ConnectionMeta
from ddpui.models.tasks import OrgTask
from ddpui.ddpairbyte import airbyte_service


class Command(BaseCommand):
    """Update connection names in ConnectionMeta records by fetching from Airbyte"""

    help = "Update connection names in ConnectionMeta records by fetching from Airbyte"

    def add_arguments(self, parser):
        """Docstring"""

    def handle(self, *args, **options):
        for orgtask in OrgTask.objects.filter(connection_id__isnull=False):
            cm = ConnectionMeta.objects.filter(connection_id=orgtask.connection_id).first()
            if cm is None:
                cm = ConnectionMeta(connection_id=orgtask.connection_id)
            if cm.connection_name is None:
                airbyte_conn = airbyte_service.get_connection(
                    orgtask.org.airbyte_workspace_id, orgtask.connection_id
                )
                cm.connection_name = airbyte_conn["name"]
                cm.save()
