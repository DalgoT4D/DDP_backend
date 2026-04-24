"""django command to remove a dbt source from an org"""

import uuid
from dotenv import load_dotenv
from django.core.management.base import BaseCommand
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.taskprogress import TaskProgress
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, DbtEdge
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse_v2

logger = CustomLogger("ddpui")
load_dotenv()


class Command(BaseCommand):
    """
    This script takes a table name
    It checks that no UI4T edges emanate from it
    If there are no edges, it deletes the OrgDbtModel and re-runs sync_sources
    """

    help = "Removes a dbt source from an org"

    def add_arguments(self, parser):
        parser.add_argument("org", help="org slug")
        parser.add_argument("schema", help="schema name")
        parser.add_argument("table", help="table name")

    def handle(self, *args, **options):
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            self.stdout.write(self.style.ERROR(f"Org {options['org']} not found"))
            return

        tablename = options["table"]
        schema = options["schema"]

        source_model = OrgDbtModel.objects.filter(
            orgdbt=org.dbt, schema=schema, type="source", name=tablename
        ).first()

        if source_model is None:
            self.stdout.write(
                self.style.ERROR(
                    f"Table {tablename} not found in schema {schema} for org {org.slug}"
                )
            )
            return

        print(f"Found {source_model}")
        assert not DbtEdge.objects.filter(to_node=source_model).exists()

        if DbtEdge.objects.filter(from_node=source_model).exists():
            for edge in DbtEdge.objects.filter(from_node=source_model):
                print("Cannot delete, source is connected to:")
                print(edge.to_node)
            return

        print(f"Deleting {source_model}")
        source_model.delete()

        org_warehouse = OrgWarehouse.objects.filter(org=org).first()

        task_id = str(uuid.uuid4())
        hashkey = f"{TaskProgressHashPrefix.SYNCSOURCES.value}-{org.slug}"

        taskprogress = TaskProgress(
            task_id=task_id,
            hashkey=hashkey,
            expire_in_seconds=10 * 60,  # max 10 minutes)
        )
        taskprogress.add(
            {
                "message": "Started syncing sources",
                "status": "runnning",
            }
        )

        print("Syncing sources")
        sync_sources_for_warehouse_v2(org.dbt.id, org_warehouse.id, task_id, hashkey)
