from django.core.management.base import BaseCommand

from ddpui.models.org import OrgDataFlow


class Command(BaseCommand):
    """Docstring"""

    help = "Updates the mapping with the correct value of dataflow type in orgdataflow table"

    def handle(self, *args, **options):
        """Docstring"""
        # for manual airbyte syncs on ingest page
        OrgDataFlow.objects.filter(deployment_name__startswith="manual-sync").update(
            dataflow_type="manual"
        )
        # for manual dbt run on transform page
        OrgDataFlow.objects.filter(deployment_name__startswith="manual-run").update(
            dataflow_type="manual"
        )
