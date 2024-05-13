from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1, OrgPrefectBlockv1
from ddpui.models.org import OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask
from ddpui.ddpairbyte.airbytehelpers import create_airbyte_deployment
from ddpui.utils.constants import TASK_AIRBYTERESET, TASK_AIRBYTESYNC
from ddpui.ddpprefect import AIRBYTESERVER


class Command(BaseCommand):
    """
    This script setups the existing orgs to run reset connection via deployments
    """

    help = "Setups the existing org to run reset connection via deployments"

    def add_arguments(self, parser):
        parser.add_argument("--orgslug", type=str, help="Org slug")

    def handle(self, *args, **options):
        """Setups the existing org to run reset connection via deployments"""
        orgs = Org.objects.all()
        if options["orgslug"] != "all":
            orgs = orgs.filter(slug=options["orgslug"])

        reset_task = None
        try:
            reset_task = Task.objects.filter(slug=TASK_AIRBYTERESET).first()
            assert reset_task is not None
        except AssertionError:
            print(f"Task {TASK_AIRBYTERESET} does not exist")
            return

        for org in orgs:
            print("=" * 40 + org.slug + "=" * 40)

            # fetch server block
            server_block = OrgPrefectBlockv1.objects.filter(
                org=org,
                block_type=AIRBYTESERVER,
            ).first()
            if server_block is None:
                print(f"{org.slug} has no {AIRBYTESERVER} block in OrgPrefectBlock")
                print("Skipping this org")
                return

            # create the orgtask for each manual-sync connection present in the org
            for org_task in OrgTask.objects.filter(
                org=org, task__slug=TASK_AIRBYTESYNC, connection_id__isnull=False
            ):
                print(
                    f"Creating reset connection task for orgtask with connection_id {org_task.connection_id}"
                )
                sync_dataflow_orgtask = DataflowOrgTask.objects.filter(
                    orgtask=org_task
                ).first()
                sync_dataflow = (
                    sync_dataflow_orgtask.dataflow if sync_dataflow_orgtask else None
                )

                if not sync_dataflow:
                    print(
                        f"Couldnt find dataflow orgtask of connection_id {org_task.connection_id}. Skipping this orgtask"
                    )
                    continue

                if OrgTask.objects.filter(
                    org=org,
                    task__slug=TASK_AIRBYTERESET,
                    connection_id=org_task.connection_id,
                ).exists():
                    print(
                        f"Reset connection orgtask/deployment/dataflow is already created for connection_id {org_task.connection_id}"
                    )
                    continue

                reset_orgtask = OrgTask.objects.create(
                    org=org,
                    connection_id=org_task.connection_id,
                    task=reset_task,
                    generated_by=org_task.generated_by,
                )
                reset_dataflow = create_airbyte_deployment(
                    org, reset_orgtask, server_block
                )

                # map sync dataflow to reset dataflow
                sync_dataflow.reset_conn_dataflow = reset_dataflow
                sync_dataflow.save()

                # validating
                print(
                    f"Validating things for newly created reset_dataflow for connection_id {org_task.connection_id}"
                )
                try:
                    assert reset_dataflow.deployment_id is not None
                    assert (
                        DataflowOrgTask.objects.filter(
                            dataflow=reset_dataflow, orgtask=reset_orgtask
                        ).count()
                        == 1
                    )
                    assert (
                        OrgTask.objects.filter(
                            org=org,
                            task__slug=TASK_AIRBYTERESET,
                            connection_id=org_task.connection_id,
                        ).count()
                        == 1
                    )
                except AssertionError as exc:
                    print(
                        f"Validation failed for orgtask of connection_id {org_task.connection_id}"
                    )
                    print(exc)
                    return

            print("=" * 80)
