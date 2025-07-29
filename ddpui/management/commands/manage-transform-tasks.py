from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.tasks import OrgTask, Task, OrgTaskGeneratedBy, DataflowOrgTask
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.utils.constants import TASK_DBTSEED, TASK_DOCSGENERATE
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    This script manages the transform tasks
    """

    help = "This script manages the transform tasks"

    def add_dbt_seed_as_transform_task(self, org: Org):
        """
        Add dbt seed as a transform task
        1. Check if the org has dbt-seed task added or not. If not, create it as a system task
        2. If the task is there and has `generated_by` = client, make it as system task i.e. `generated_by` = system
        """
        assert Task.objects.filter(slug=TASK_DBTSEED).exists(), "dbt-seed task not found"
        assert Task.objects.filter(
            slug=TASK_DBTSEED, is_system=True
        ).exists(), "dbt-seed task is not a system task. please make sure to run the seeder at seed/tasks.json"

        org_task = OrgTask.objects.filter(org=org, task__slug=TASK_DBTSEED).first()

        if not org_task:
            OrgTask.objects.create(
                org=org,
                task=Task.objects.filter(slug=TASK_DBTSEED).first(),
                generated_by=OrgTaskGeneratedBy.SYSTEM,
            )
            logger.info(f"Added system {TASK_DBTSEED} task as a transform task for org {org.slug}")
        else:
            if org_task.generated_by == OrgTaskGeneratedBy.CLIENT:
                org_task.generated_by = OrgTaskGeneratedBy.SYSTEM
                org_task.save()
                logger.info(f"Changed {TASK_DBTSEED} task to system task for org {org.slug}")

        return None

    def remove_dbt_docs_generate_from_current_pipelines(self, org: Org):
        """
        1. Update the deployment params in prefect to remove this task
        2. Remove the orgtask mapping in DataflowOrgTask
        """
        for dataflow in OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate").all():
            deployment_id = dataflow.deployment_id
            try:
                deployment = prefect_service.get_deployment(deployment_id)
            except Exception as err:
                logger.error(
                    f"Error while fetching deployment params for deployment_id {dataflow.deployment_id} {org.slug}: {err}"
                )
                continue

            params = deployment.get("parameters", {})
            config = params.get("config", {})
            tasks = config.get("tasks", [])

            if len([task for task in tasks if task["slug"] == TASK_DOCSGENERATE]) > 0:
                updated_task_list = [task for task in tasks if task["slug"] != TASK_DOCSGENERATE]
                tasks = updated_task_list

                try:
                    prefect_service.update_dataflow_v1(
                        dataflow.deployment_id,
                        PrefectDataFlowUpdateSchema3(
                            deployment_params={"config": config}, cron=dataflow.cron
                        ),
                    )
                except Exception as err:
                    logger.error(
                        f"Error while updating deployment params for deployment_id {dataflow.deployment_id} {org.slug}: {err}"
                    )
                    continue

            # remove the DataflowOrgTask mapping
            DataflowOrgTask.objects.filter(
                dataflow=dataflow, orgtask__task__slug=TASK_DOCSGENERATE
            ).delete()

            logger.info(
                f"Removed dbt-docs-generate task from orchestrate pipeline for deployment {dataflow.deployment_id} {org.slug}"
            )

        ## assert if any mapping it still there
        assert (
            DataflowOrgTask.objects.filter(
                orgtask__task__slug=TASK_DOCSGENERATE,
                dataflow__dataflow_type="orchestrate",
                dataflow__org=org,
            ).count()
            == 0
        ), "Some mappings still exist for dbt-docs-generate task"

        logger.info("Validated")

        return None

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        pass

    def handle(self, *args, **options):
        for org in Org.objects.all():
            self.add_dbt_seed_as_transform_task(org)
            self.remove_dbt_docs_generate_from_current_pipelines(org)
