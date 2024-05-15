from django.core.management.base import BaseCommand
from ninja.errors import HttpError
from ddpui.ddpprefect.prefect_service import prefect_put
from ddpui.models.org import OrgDataFlowv1, Org
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE, DDP_WORK_QUEUE


class Command(BaseCommand):
    """Docstring"""

    help = "Sets the name of the worker pool for all manual deployments."

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument(
            "--orgslug",
            type=str,
            help="Org slug: use 'all' to run for all orgs at once",
            required=True,
        )
        parser.add_argument(
            "--workpool", type=str, help="Worker pool name", required=True
        )

    def handle(self, *args, **options):
        """Docstring"""
        orgs = Org.objects.all()
        if options["orgslug"] != "all":
            orgs = orgs.filter(slug=options["orgslug"])

        work_pool_name = options["workpool"]

        for org in orgs:
            print("=" * 40 + org.slug + "=" * 40)
            print(
                f"Moving all deployments of this org {org.slug} from agents to workers"
            )

            for dataflow in OrgDataFlowv1.objects.filter(org=org).all():
                work_queue_name = DDP_WORK_QUEUE

                if dataflow.name.find("airbyte-sync") != -1 or dataflow.name.find(
                    "airbyte-reset"
                ):
                    work_queue_name = DDP_WORK_QUEUE
                elif dataflow.name.find("dbt-run") != -1:
                    work_queue_name = MANUL_DBT_WORK_QUEUE

                print(
                    f"Setting the work_queue_name to {work_queue_name} and worker pool to {work_pool_name} for deployment {dataflow.deployment_name}"
                )
                try:
                    res = prefect_put(
                        f"v1/deployments/{dataflow.deployment_id}",
                        {
                            "work_pool_name": work_pool_name,
                            "work_queue_name": work_queue_name,
                        },
                    )
                    print(res)
                except HttpError as e:
                    print(f"Error updating deployment {dataflow.deployment_id}: {e}")
